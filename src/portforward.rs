//! Lazy local-port forwarders for out-of-cluster runs.
//!
//! A `Forwarder` binds `127.0.0.1:0`, accepts connections, and bridges
//! each one to `pod:remote_port` via `kube::Api::<Pod>::portforward`.
//! The accept loop is a detached tokio task; it exits when the
//! `_shutdown` oneshot drops, which happens on `Forwarder::drop`.

use std::net::{Ipv4Addr, SocketAddr};

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::Api;
use tokio::io::copy_bidirectional;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct Forwarder {
    pub local_port: u16,
    _shutdown: oneshot::Sender<()>,
}

impl Forwarder {
    pub async fn start(
        client: Client,
        namespace: String,
        pod: String,
        remote_port: u16,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let local_port = listener.local_addr()?.port();
        let (tx, mut rx) = oneshot::channel::<()>();

        tracing::debug!(
            local_port,
            namespace = %namespace,
            pod = %pod,
            remote_port,
            "portforward listening"
        );

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut rx => break,
                    accept = listener.accept() => {
                        let Ok((sock, _peer)) = accept else { break };
                        let api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
                        let pod_name = pod.clone();
                        tokio::spawn(async move {
                            if let Err(e) = bridge(sock, api, &pod_name, remote_port).await {
                                tracing::warn!(error = %e, pod = %pod_name, port = remote_port, "portforward bridge failed");
                            }
                        });
                    }
                }
            }
        });

        Ok(Forwarder {
            local_port,
            _shutdown: tx,
        })
    }
}

async fn bridge(
    mut sock: tokio::net::TcpStream,
    api: Api<Pod>,
    pod: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut pf = api.portforward(pod, &[port]).await?;
    let mut upstream = pf
        .take_stream(port)
        .ok_or("portforward did not return a stream for the requested port")?;
    let _ = copy_bidirectional(&mut sock, &mut upstream).await?;
    pf.join()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    Ok(())
}
