//! Process HTTP connections on the server.

use std::pin::Pin;
use std::time::Duration;

use async_std::future::{pending, timeout, Future, TimeoutError};
use async_std::io::{self, BufReader, Read, Write};
use async_std::prelude::*;
use futures_util::io::AsyncBufReadExt;
use http_types::headers::{CONNECTION, UPGRADE};
use http_types::upgrade::Connection;
use http_types::{Request, Response, StatusCode};

use crate::chunked::ChunkedDecoder;

mod decode;
mod encode;

pub use decode::{decode, BodyInfo, BodyKind};
pub use encode::Encoder;

/// Configure the server.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// Timeout to handle headers. Defaults to 60s.
    headers_timeout: Option<Duration>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            headers_timeout: Some(Duration::from_secs(60)),
        }
    }
}

/// Accept a new incoming HTTP/1.1 connection.
///
/// Supports `KeepAlive` requests by default.
pub async fn accept<RW, F, Fut>(io: RW, endpoint: F) -> http_types::Result<()>
where
    RW: Read + Write + Clone + Send + Sync + Unpin + 'static,
    F: Fn(Request) -> Fut,
    Fut: Future<Output = http_types::Result<Response>>,
{
    accept_with_opts(io, endpoint, Default::default()).await
}

/// Accept a new incoming HTTP/1.1 connection.
///
/// Supports `KeepAlive` requests by default.
pub async fn accept_with_opts<RW, F, Fut>(
    mut io: RW,
    endpoint: F,
    opts: ServerOptions,
) -> http_types::Result<()>
where
    RW: Read + Write + Clone + Send + Sync + Unpin + 'static,
    F: Fn(Request) -> Fut,
    Fut: Future<Output = http_types::Result<Response>>,
{
    let mut reader = BufReader::new(io.clone());

    loop {
        // Decode a new request, timing out if this takes longer than the timeout duration.
        let fut = decode(&mut reader);

        let (req, body) = if let Some(timeout_duration) = opts.headers_timeout {
            match timeout(timeout_duration, fut).await {
                Ok(Ok(Some(r))) => r,
                Ok(Ok(None)) | Err(TimeoutError { .. }) => break, /* EOF or timeout */
                Ok(Err(e)) => return Err(e),
            }
        } else {
            match fut.await? {
                Some(r) => r,
                None => break, /* EOF */
            }
        };

        let has_upgrade_header = req.header(UPGRADE).is_some();
        let connection_header_is_upgrade = req
            .header(CONNECTION)
            .map(|connection| connection.as_str().eq_ignore_ascii_case("upgrade"))
            .unwrap_or(false);

        let upgrade_requested = has_upgrade_header && connection_header_is_upgrade;

        let method = req.method();

        // Pass the request to the endpoint and encode the response.
        log::trace!("join start!");

        let (done_sender, done_receiver) = async_std::sync::channel(1);

        let endpoint_fut = async {
            log::trace!("body_fut: running endpoint");
            let res = endpoint(req).await;
            log::trace!("body_fut: sending");
            let _ = done_sender.send(()).await;
            log::trace!("body_fut: sending done");
            res
        };

        let reader_fut = async {
            if let Some(body) = body {
                let mut body_reader: Pin<Box<dyn Read>> = match body.kind {
                    BodyKind::Chunked(trailer_sender) => {
                        Box::pin(ChunkedDecoder::new(&mut reader, trailer_sender))
                    }
                    BodyKind::Length(len) => Box::pin((&mut reader).take(len as u64)),
                };

                log::trace!("reader_fut: copying to pipe");
                let res = io::copy(body_reader.as_mut(), body.pipe).await;
                log::trace!("reader_fut: copy to pipe returned {:?}", res);
                log::trace!("reader_fut: copying to sink");
                let res = io::copy(body_reader.as_mut(), io::sink()).await;
                log::trace!("reader_fut: copy to sink returned {:?}", res);
                res?;
            } else {
                log::trace!("reader_fut: no body");
            }

            // Keep running endpoint concurrently with a "peek" of the connection.
            // The peek future completes if the client cancels the in-progress request
            let peek_fut = async {
                log::trace!("reader_fut/peek_fut: peeking");
                match reader.fill_buf().await {
                    // If peek returns EOF, return an error
                    Ok(buf) if buf.len() == 0 => {
                        log::trace!("reader_fut/peek_fut: peek returned EOF");
                        http_types::Result::<()>::Err(
                            io::Error::new(io::ErrorKind::BrokenPipe, "Client canceled request")
                                .into(),
                        )
                    }
                    // If peek returns data, it must be data from the next request's headers.
                    Ok(_) => {
                        log::trace!("reader_fut/peek_fut: peek returned data");
                        pending::<()>().await;
                        unreachable!()
                    }
                    // If peek fails with error, return it.
                    Err(e) => {
                        log::trace!("reader_fut/peek_fut: peek returned err {:?}", e);
                        Err(e.into())
                    }
                }
            };

            let recv_fut = async {
                // Return when the recv is done, doesn't matter if it errored.
                log::trace!("reader_fut/recv_fut: receiving");
                let _ = done_receiver.recv().await;
                log::trace!("reader_fut/recv_fut: receiving");

                Ok(())
            };

            let res = peek_fut.race(recv_fut).await;
            log::trace!("reader_fut/recv_fut: done with {:?}", res);
            res
        };
        let (mut res, _) = endpoint_fut.try_join(reader_fut).await?;

        log::trace!("join done!");

        let upgrade_provided = res.status() == StatusCode::SwitchingProtocols && res.has_upgrade();

        let upgrade_sender = if upgrade_requested && upgrade_provided {
            Some(res.send_upgrade())
        } else {
            None
        };

        let mut encoder = Encoder::new(res, method);

        // Stream the response to the writer.
        io::copy(&mut encoder, &mut io).await?;

        if let Some(upgrade_sender) = upgrade_sender {
            upgrade_sender.send(Connection::new(io.clone())).await;
            return Ok(());
        }
    }

    Ok(())
}
