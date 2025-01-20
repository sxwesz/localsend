use crate::model::file::FileDto;
use crate::webrtc::signaling::{ManagedSignalingConnection, WsServerSdpMessage};
use anyhow::Result;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::engine::GeneralPurpose;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

#[derive(Debug, Deserialize, Serialize)]
struct RTCInitialMessage {
    pub files: Vec<FileDto>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RTCInitialResponse {
    pub files: HashMap<String, String>,
}

pub struct RTCFile {
    pub file_id: String,
    pub binary_rx: mpsc::Receiver<Vec<u8>>,
}

struct RTCFileState {
    file_id: String,
    size: u64,
    binary_tx: mpsc::Sender<Vec<u8>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RTCSendFileHeaderMessage {
    pub id: String,
    pub token: String,
}

pub enum RTCStatus {
    /// Received remote SDP offer/answer. Ready to start P2P connection.
    SdpExchanged,

    /// Opened data channel. Ready to send/receive data.
    Connected,

    /// Data channel closed. Connection is closed.
    Finished,

    /// Error occurred. Connection is closed.
    Error(String),
}

pub struct RTCFileError {
    pub file_id: String,
    pub error: String,
}

const CHANNEL_LABEL: &str = "data";

pub async fn send_offer(
    signaling: &ManagedSignalingConnection,
    target_id: Uuid,
    files: Vec<FileDto>,
    status_tx: mpsc::Sender<RTCStatus>,
    selected_files_tx: mpsc::Sender<HashSet<String>>,
    error_tx: mpsc::Sender<RTCFileError>,
    mut sending_rx: mpsc::Receiver<RTCFile>,
) -> Result<()> {
    let (peer_connection, mut done_rx) = create_peer_connection().await?;

    let data_channel = peer_connection
        .create_data_channel(
            CHANNEL_LABEL,
            Some(RTCDataChannelInit {
                ordered: Some(true),
                max_packet_life_time: None,
                max_retransmits: None,
                protocol: None,
                negotiated: None,
            }),
        )
        .await?;

    let (file_tokens_tx, mut file_tokens_rx) = mpsc::channel::<HashMap<String, String>>(16);

    {
        let data_channel_clone = Arc::clone(&data_channel);
        let status_tx = status_tx.clone();
        let error_tx = error_tx.clone();
        data_channel.on_open(Box::new(move || {
            let data_channel = Arc::clone(&data_channel_clone);
            Box::pin(async move {
                'send: {
                    if let Err(e) = status_tx.send(RTCStatus::Connected).await {
                        let _ = status_tx.try_send(RTCStatus::Error(e.to_string()));
                        break 'send;
                    }

                    {
                        // send initial message
                        let initial_message =
                            serde_json::to_string(&RTCInitialMessage { files }).unwrap();

                        let result = process_string_in_chunks(
                            Arc::clone(&data_channel),
                            initial_message,
                            |data_channel, chunk| async move {
                                data_channel.send(&chunk).await?;
                                Ok(data_channel)
                            },
                        )
                        .await;

                        if let Err(e) = result {
                            let _ = status_tx.try_send(RTCStatus::Error(format!(
                                "Failed to send initial message: {e}"
                            )));
                            break 'send;
                        }
                    }

                    tracing::debug!("Sent initial message. Waiting for file tokens...");

                    // Receive file tokens
                    let file_tokens = match file_tokens_rx.recv().await {
                        Some(file_tokens) => file_tokens,
                        None => {
                            let _ = status_tx
                                .send(RTCStatus::Error(
                                    "Failed to receive file tokens".to_string(),
                                ))
                                .await;

                            break 'send;
                        }
                    };

                    // Publish selected files
                    if let Err(e) = selected_files_tx
                        .send(file_tokens.keys().cloned().collect())
                        .await
                    {
                        let _ = status_tx.send(RTCStatus::Error(e.to_string())).await;
                        break 'send;
                    }

                    tracing::debug!("Received file tokens. Sending files...");

                    while let Some(message) = sending_rx.recv().await {
                        let file_token = match file_tokens.get(&message.file_id) {
                            Some(file_token) => file_token,
                            None => {
                                let _ = error_tx
                                    .send(RTCFileError {
                                        file_id: message.file_id,
                                        error: "Failed to get file token".to_string(),
                                    })
                                    .await;

                                continue;
                            }
                        };

                        let header = RTCSendFileHeaderMessage {
                            id: message.file_id.clone(),
                            token: file_token.clone(),
                        };

                        if let Err(e) = data_channel
                            .send_text(
                                serde_json::to_string(&header).expect("Failed to serialize header"),
                            )
                            .await
                        {
                            let _ = error_tx
                                .send(RTCFileError {
                                    file_id: message.file_id,
                                    error: e.to_string(),
                                })
                                .await;
                            continue;
                        }

                        let result = process_in_chunks(
                            Arc::clone(&data_channel),
                            message.binary_rx,
                            |data_channel, chunk| async move {
                                data_channel.send(&chunk).await?;
                                Ok(data_channel)
                            },
                        )
                        .await;

                        if let Err(e) = result {
                            let _ = error_tx
                                .send(RTCFileError {
                                    file_id: message.file_id,
                                    error: e.to_string(),
                                })
                                .await;
                            continue;
                        }
                    }
                }

                tracing::debug!("Finishing...");

                let _ = status_tx.send(RTCStatus::Finished).await;
                if let Err(e) = data_channel.close().await {
                    tracing::error!("Failed to close data channel: {e}");
                }
            })
        }));
    }

    let mut initial_msg_buffer = BytesMut::new();
    initial_msg_buffer.clone();

    data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
        Box::pin({
            match msg.is_string {
                true => {
                    // read buffer
                    let initial_msg_buffer = initial_msg_buffer.freeze();
                    let initial_msg_str = String::from_utf8(initial_msg_buffer.to_vec()).unwrap();

                    // parse file tokens
                    if let Ok(file_tokens) = serde_json::from_str::<RTCInitialResponse>(&initial_msg_str) {
                        let _ = file_tokens_tx.try_send(file_tokens.files);
                    }
                }
                false => {
                    initial_msg_buffer.extend_from_slice(&msg.data);
                }
            }
            async move {}
        })
    }));

    let offer = peer_connection.create_offer(None).await?;
    let mut gather_complete = peer_connection.gathering_complete_promise().await;
    peer_connection.set_local_description(offer).await?;
    let _ = gather_complete.recv().await;

    let session_id = Uuid::new_v4().to_string();
    let local_description = peer_connection
        .local_description()
        .await
        .ok_or_else(|| anyhow::anyhow!("Could not generate local_description"))?;

    signaling
        .send_offer(
            session_id.clone(),
            target_id,
            encode_sdp(&local_description.sdp),
        )
        .await?;

    let (tx_answer, rx_answer) = tokio::sync::oneshot::channel();

    signaling
        .on_answer(session_id, |message| {
            tx_answer.send(message.sdp).unwrap();
        })
        .await;

    let remote_desc = rx_answer.await?;

    if let Err(e) = status_tx.send(RTCStatus::SdpExchanged).await {
        peer_connection.close().await?;
        return Err(e.into());
    }

    let answer = RTCSessionDescription::answer(decode_sdp(&remote_desc)?)?;

    peer_connection.set_remote_description(answer).await?;

    done_rx.recv().await;

    peer_connection.close().await?;

    Ok(())
}

pub async fn accept_offer(
    signaling: &ManagedSignalingConnection,
    offer: &WsServerSdpMessage,
    status_tx: mpsc::Sender<RTCStatus>,
    files_tx: oneshot::Sender<Vec<FileDto>>,
    selected_files_rx: oneshot::Receiver<HashSet<String>>,
    error_tx: mpsc::Sender<RTCFileError>,
    receiving_tx: mpsc::Sender<RTCFile>,
) -> Result<()> {
    let (peer_connection, mut done_rx) = create_peer_connection().await?;

    let (data_channel_tx, mut data_channel_rx) = mpsc::channel::<Arc<RTCDataChannel>>(1);

    peer_connection.on_data_channel(Box::new(move |d: Arc<RTCDataChannel>| {
        if d.label() != CHANNEL_LABEL {
            return Box::pin(async {});
        }

        let data_channel_tx = data_channel_tx.clone();
        Box::pin(async move {
            let d_clone = Arc::clone(&d);
            d.on_open(Box::new(move || {
                let _ = data_channel_tx.try_send(d_clone);

                Box::pin(async {})
            }));
        })
    }));

    let task = {
        let status_tx = status_tx.clone();
        let files_tx = Arc::new(Mutex::new(Some(files_tx)));
        tokio::spawn(async move {
            let Some(data_channel) = data_channel_rx.recv().await else {
                return;
            };

            let _ = status_tx.try_send(RTCStatus::Connected);

            let (break_tx, mut break_rx) = mpsc::channel::<()>(1);
            let initial_msg_buffer = Some(BytesMut::new());
            let file_size_map = Arc::new(Mutex::new(HashMap::<String, u64>::new()));

            {
                let file_size_map = file_size_map.clone();
                let break_tx = break_tx.clone();
                data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
                    let mut initial_msg_buffer = initial_msg_buffer.clone();
                    let file_size_map = file_size_map.clone();
                    let files_tx = files_tx.clone();
                    let break_tx = break_tx.clone();
                    Box::pin({
                        async move {
                            'receive: {
                                match msg.is_string {
                                    true => {
                                        if let Some(ref mut buffer) = initial_msg_buffer {
                                            let Ok(initial_msg_str) =
                                                String::from_utf8(msg.data.to_vec())
                                            else {
                                                let _ = break_tx.try_send(());
                                                break 'receive;
                                            };

                                            let Ok(initial_msg) =
                                                serde_json::from_str::<RTCInitialMessage>(
                                                    &initial_msg_str,
                                                )
                                            else {
                                                let _ = break_tx.try_send(());
                                                break 'receive;
                                            };

                                            let mut file_size_map = file_size_map.lock().await;
                                            for file in &initial_msg.files {
                                                file_size_map.insert(file.id.clone(), file.size);
                                            }
                                            drop(file_size_map);

                                            let mut files_tx = files_tx.lock().await;
                                            if let Some(files_tx) = files_tx.take() {
                                                let _ = files_tx.send(initial_msg.files);
                                                return;
                                            }

                                            // could not send files_tx
                                            let _ = break_tx.try_send(());
                                        }
                                    }
                                    false => {
                                        if let Some(ref mut buffer) = initial_msg_buffer {
                                            buffer.extend_from_slice(&msg.data);
                                            break 'receive;
                                        }
                                    }
                                }
                            }
                        }
                    })
                }));
            }

            let send_future = async move {
                let Ok(selected_files) = selected_files_rx.await else {
                    let _ = break_tx.try_send(());
                    return Ok::<(), anyhow::Error>(());
                };

                let file_tokens = selected_files
                    .into_iter()
                    .map(|file_id| {
                        let token = Uuid::new_v4().to_string();
                        (file_id, token)
                    })
                    .collect::<HashMap<String, String>>();

                let response_str =
                    serde_json::to_string(&RTCInitialResponse { files: file_tokens })?;
                if let Err(e) = process_string_in_chunks(
                    Arc::clone(&data_channel),
                    response_str,
                    |data_channel, chunk| async move {
                        data_channel.send(&chunk).await?;
                        Ok(data_channel)
                    },
                )
                .await
                {
                    let _ = break_tx.try_send(());
                    return Err(e);
                }

                // Mark the end of the initial message
                data_channel.send_text("".to_string()).await?;

                Ok(())
            };

            tokio::select! {
                _ = send_future => {
                    tracing::debug!("Sending done.");
                }
                _ = break_rx.recv() => {
                    let _ = status_tx.send(RTCStatus::Error("Unknown Error".to_string())).await;
                }
                _ = done_rx.recv() => {
                    let _ = status_tx.send(RTCStatus::Finished).await;
                }
            }
        })
    };

    let remote_desc_sdp = decode_sdp(&offer.sdp)?;
    let remote_desc = RTCSessionDescription::offer(remote_desc_sdp)?;
    peer_connection.set_remote_description(remote_desc).await?;

    let answer = peer_connection.create_answer(None).await?;

    let mut gather_complete = peer_connection.gathering_complete_promise().await;
    peer_connection.set_local_description(answer).await?;
    let _ = gather_complete.recv().await;

    let local_description = peer_connection
        .local_description()
        .await
        .ok_or_else(|| anyhow::anyhow!("generate local_description failed!"))?;

    signaling
        .send_answer(
            offer.session_id.clone(),
            offer.peer.id,
            encode_sdp(&local_description.sdp),
        )
        .await?;

    if let Err(e) = status_tx.send(RTCStatus::SdpExchanged).await {
        peer_connection.close().await?;
        return Err(e.into());
    }

    task.await?;

    peer_connection.close().await?;

    Ok(())
}

async fn create_peer_connection() -> Result<(Arc<RTCPeerConnection>, mpsc::Receiver<()>)> {
    let mut m = MediaEngine::default();
    m.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)?;

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();

    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let peer_connection = api.new_peer_connection(config).await?;

    let (done_tx, done_rx) = mpsc::channel::<()>(1);

    peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        if s == RTCPeerConnectionState::Failed {
            tracing::warn!("Peer Connection: State changed to Failed. Closing the connection...");
            let _ = done_tx.try_send(());
        }
        Box::pin(async {})
    }));

    Ok((Arc::new(peer_connection), done_rx))
}

const BASE_64_SDP: GeneralPurpose = URL_SAFE_NO_PAD;

fn encode_sdp(s: &str) -> String {
    let mut compressor = brotli::CompressorWriter::new(Vec::new(), 4096, 11, 24);
    compressor
        .write_all(s.as_bytes())
        .expect("Compression of SDP failed");
    BASE_64_SDP.encode(&compressor.into_inner())
}

fn decode_sdp(s: &str) -> Result<String> {
    let decoded_data = BASE_64_SDP.decode(s)?;
    let mut decompressor = brotli::Decompressor::new(&decoded_data[..], 4096);
    let mut decompressed = Vec::new();
    decompressor
        .read_to_end(&mut decompressed)
        .expect("Decompression failed");
    let result = String::from_utf8(decompressed)?;
    Ok(result)
}

const CHUNK_SIZE: usize = 16 * 1024; // 16 KiB

/// Process incoming data in chunks of CHUNK_SIZE
/// The callback returns the same data_channel to avoid re-creating or lifetime issues.
pub async fn process_in_chunks<T, F, Fut>(
    mut data_channel: T,
    mut rx: mpsc::Receiver<Vec<u8>>,
    mut callback: F,
) -> Result<()>
where
    F: FnMut(T, Bytes) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut buffer = BytesMut::with_capacity(CHUNK_SIZE);

    while let Some(data) = rx.recv().await {
        buffer.extend_from_slice(&data);

        while buffer.len() >= CHUNK_SIZE {
            let chunk = buffer.split_to(CHUNK_SIZE).freeze();

            // Process the chunk, reuse the data_channel
            data_channel = callback(data_channel, chunk).await?;
        }
    }

    // After the channel is closed, if there's leftover data, handle it as needed:
    if !buffer.is_empty() {
        callback(data_channel, buffer.freeze()).await?;
    }

    Ok(())
}

/// Convenience function for `process_in_chunks` that processes a string in chunks.
pub async fn process_string_in_chunks<T, F, Fut>(
    data_channel: T,
    string: String,
    callback: F,
) -> Result<()>
where
    F: FnMut(T, Bytes) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let (tx, rx) = mpsc::channel(1);

    tokio::spawn(async move {
        let _ = tx.send(string.into_bytes()).await;
    });

    process_in_chunks(data_channel, rx, callback).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_process_in_chunks() {
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let mut test_vec = vec![0; CHUNK_SIZE * 2 + 5];
            test_vec[CHUNK_SIZE..CHUNK_SIZE * 2]
                .iter_mut()
                .for_each(|x| *x = 1);
            test_vec[CHUNK_SIZE * 2..].iter_mut().for_each(|x| *x = 2);
            tx.send(test_vec).await.unwrap();
        });

        let mut chunks = Vec::new();

        let result = process_in_chunks(0, rx, |_, chunk| {
            chunks.push(chunk);
            async { Ok(0) }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), CHUNK_SIZE);
        assert_eq!(chunks[1].len(), CHUNK_SIZE);
        assert_eq!(chunks[2].len(), 5);

        assert_eq!(chunks[0].iter().all(|x| *x == 0), true);
        assert_eq!(chunks[1].iter().all(|x| *x == 1), true);
        assert_eq!(chunks[2].iter().all(|x| *x == 2), true);
    }
}
