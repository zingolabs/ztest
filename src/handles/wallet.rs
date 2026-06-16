//! Wallet handle: typed RPC sugar over `WalletHandle`.
//!
//! Every method delegates to the [`crate::handles::backends::WalletBackend`]
//! trait object set at construction. No `match` on backend kind in this
//! file.

use crate::handles::WalletHandle;
use crate::RpcError;

/// Which wallet backend a `WalletHandle` wraps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletKind {
    Zingo,
}

impl WalletHandle {
    /// Which backend this handle wraps. Delegates to the trait object.
    pub fn kind(&self) -> WalletKind {
        self.backend.kind()
    }

    /// The wallet's last-synced chain height.
    pub async fn synced_height(&self) -> Result<u64, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.synced_height(&ep).await
    }
}
