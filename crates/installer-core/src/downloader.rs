use futures_util::StreamExt;
use std::path::Path;
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::error::{AppError, Result};
use crate::mirrors::PER_MIRROR_TIMEOUT;
use crate::progress::{DownloadProgress, ProgressCallback};

/// How many times we'll (re)connect to the SAME mirror before giving up and
/// letting the caller fall through to the next mirror in the chain. Attempt 1
/// is the initial download; the rest are resume attempts after a mid-stream
/// drop.
///
/// Same-mirror retry exists because the free GH proxies in the mirror list
/// (gh-proxy.com, ghfast.top, …) routinely reset the connection near the *tail*
/// of a large binary. Pre-v0.5 a single such drop deleted the partial file and
/// jumped to a different mirror, restarting from byte 0 — the user-visible
/// "download was almost done, then it switched lines and started over".
const MAX_ATTEMPTS: usize = 4;

/// If the body stream produces no new bytes for this long, treat the connection
/// as dead and trigger a resume attempt. This is the body-phase counterpart to
/// `PER_MIRROR_TIMEOUT`, which only guards the *header* phase: once headers
/// arrive the body stream is otherwise untimed, so a mirror that accepts the
/// connection then silently stops sending (an overloaded proxy that neither
/// closes nor feeds) would hang forever without this.
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Issue `GET url` and wait for response headers, bounded by
/// `PER_MIRROR_TIMEOUT`. The body stream is **not** under this timeout —
/// once headers arrive, a slow-but-alive download over flaky links is
/// expected, and we must not cut a partially-finished file off.
///
/// Pre-v0.5 callers used `client.get(url).send().await?` bare, which
/// meant a dead mirror would idle out on reqwest's 60s global timeout
/// instead of the 8s per-mirror budget. Every code path that walks a
/// mirror chain should go through this helper.
pub async fn send_with_timeout(
    client: &reqwest::Client,
    url: &str,
) -> Result<reqwest::Response> {
    let resp = tokio::time::timeout(PER_MIRROR_TIMEOUT, client.get(url).send())
        .await
        .map_err(|_| {
            AppError::Other(format!(
                "{}s timeout waiting for response headers",
                PER_MIRROR_TIMEOUT.as_secs()
            ))
        })??;
    Ok(resp.error_for_status()?)
}

/// Download `url` into `dest`, with three layers of resilience over a single
/// mirror before the caller is expected to fall through to the next one:
///
/// 1. **Resume** — on a mid-stream drop we reopen the file in append mode and
///    re-request with `Range: bytes=N-`. A cooperating server replies `206
///    Partial Content` and we continue from byte N; a server that ignores the
///    header replies `200` and we transparently restart from byte 0.
/// 2. **Same-mirror retry** — up to `MAX_ATTEMPTS`, but only while we're still
///    making progress (see the resume guard below). A mirror that delivers 0
///    bytes is treated as dead and handed straight back to the caller.
/// 3. **Body idle timeout** — each chunk read is bounded by `BODY_IDLE_TIMEOUT`
///    so a silently-stalled connection is converted into a resume attempt
///    instead of hanging.
///
/// Returns the total bytes written on success.
pub async fn download_to_file(
    client: &reqwest::Client,
    progress: &ProgressCallback,
    tool_id: &str,
    mirror_name: &str,
    url: &str,
    dest: &Path,
) -> Result<u64> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut downloaded: u64 = 0;
    let mut total: Option<u64> = None;
    let mut last_err: Option<AppError> = None;

    for attempt in 1..=MAX_ATTEMPTS {
        // On a resume attempt, ask the server to continue from where we stopped.
        let mut req = client.get(url);
        if downloaded > 0 {
            req = req.header(reqwest::header::RANGE, format!("bytes={}-", downloaded));
        }

        // Header phase is bounded by PER_MIRROR_TIMEOUT, same as send_with_timeout.
        let resp = match tokio::time::timeout(PER_MIRROR_TIMEOUT, req.send()).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                // Connect-level failure: this mirror is unreachable, don't burn
                // retries on it — hand back to the caller's mirror chain.
                return Err(e.into());
            }
            Err(_) => {
                return Err(AppError::Other(format!(
                    "{}s timeout waiting for response headers",
                    PER_MIRROR_TIMEOUT.as_secs()
                )));
            }
        };
        let resp = resp.error_for_status()?;

        // 206 means our Range was honored and we should append; anything else
        // (typically 200) means the server is resending from the start, so reset.
        let resuming = resp.status() == reqwest::StatusCode::PARTIAL_CONTENT;
        if downloaded > 0 && !resuming {
            downloaded = 0;
        }

        // Establish the full content size for progress. On a fresh 200 the
        // Content-Length IS the full size; on a 206 it's only the *remaining*
        // length, so the full size is already-downloaded + remaining. Keep the
        // first total we learn.
        if total.is_none() {
            total = match resp.content_length() {
                Some(len) if resuming => Some(downloaded + len),
                other => other,
            };
        }

        // Append when resuming, truncate on a fresh start.
        let mut file = if downloaded > 0 {
            OpenOptions::new().append(true).open(dest).await?
        } else {
            File::create(dest).await?
        };

        let mut stream = resp.bytes_stream();
        let mut last_emit = std::time::Instant::now();
        let mut stream_err: Option<AppError> = None;

        loop {
            match tokio::time::timeout(BODY_IDLE_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(bytes))) => {
                    file.write_all(&bytes).await?;
                    downloaded += bytes.len() as u64;

                    // Throttle progress events to ~10/sec
                    if last_emit.elapsed().as_millis() >= 100 {
                        progress(DownloadProgress {
                            tool_id: tool_id.to_string(),
                            downloaded,
                            total,
                            mirror: mirror_name.to_string(),
                        });
                        last_emit = std::time::Instant::now();
                    }
                }
                Ok(Some(Err(e))) => {
                    stream_err = Some(e.into());
                    break;
                }
                Ok(None) => break, // stream finished cleanly
                Err(_) => {
                    stream_err = Some(AppError::Other(format!(
                        "{}s idle timeout mid-download at {} bytes",
                        BODY_IDLE_TIMEOUT.as_secs(),
                        downloaded
                    )));
                    break;
                }
            }
        }
        file.flush().await?;

        match stream_err {
            None => {
                progress(DownloadProgress {
                    tool_id: tool_id.to_string(),
                    downloaded,
                    total,
                    mirror: mirror_name.to_string(),
                });
                return Ok(downloaded);
            }
            Some(e) => {
                tracing::warn!(
                    "download from {} interrupted at {} bytes (attempt {}/{}): {}",
                    mirror_name,
                    downloaded,
                    attempt,
                    MAX_ATTEMPTS,
                    e
                );
                last_err = Some(e);
                // Only resume if we actually made progress this round — a mirror
                // that dropped at 0 bytes won't get better by retrying, and we'd
                // rather spend that time on the next mirror in the chain.
                if attempt < MAX_ATTEMPTS && downloaded > 0 {
                    tokio::time::sleep(Duration::from_millis(300 * attempt as u64)).await;
                    continue;
                }
                break;
            }
        }
    }

    Err(last_err.unwrap_or(AppError::AllMirrorsFailed))
}
