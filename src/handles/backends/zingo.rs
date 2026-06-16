//! Zingo wallet backend. gRPC client wiring still pending; methods
//! return a clear `Backend` error until the proto vendor lands.

use std::io;

use async_trait::async_trait;

use crate::handles::backends::WalletBackend;
use crate::handles::wallet::WalletKind;
use crate::{Endpoint, RpcError};

const COMPONENT: &str = "zingo";

/// Docker image URI for a given `zingolib` version tag. Used by
/// `manifest::pod_spec_for_wallet`.
pub(crate) fn image_uri(version: &str) -> String {
    format!("zingolabs/zingolib:{version}")
}

/// Zingo-flavoured wallet behaviour. ZST; stored as
/// `Arc<dyn WalletBackend>` inside `WalletHandle`.
#[derive(Debug)]
pub(crate) struct ZingoBackend;

#[async_trait]
impl WalletBackend for ZingoBackend {
    fn kind(&self) -> WalletKind {
        WalletKind::Zingo
    }

    fn label(&self) -> &'static str {
        COMPONENT
    }

    async fn synced_height(&self, _endpoint: &Endpoint) -> Result<u64, RpcError> {
        Err(RpcError::backend(
            COMPONENT,
            "synced_height",
            io::Error::new(io::ErrorKind::Unsupported, "Zingo gRPC client not yet wired"),
        ))
    }
}
