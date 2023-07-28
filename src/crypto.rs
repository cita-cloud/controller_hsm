// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use cita_cloud_proto::{
    blockchain::{RawTransaction, RawTransactions},
    status_code::StatusCodeEnum,
};

pub fn hash_data(data: &[u8]) -> Vec<u8> {
    cfg_if::cfg_if! {
        if #[cfg(feature = "sm")] {
            crypto_sm::sm::hash_data(data)
        } else if #[cfg(feature = "eth")] {
            crypto_eth::eth::hash_data(data)
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "sm")] {
        fn recover_signature(msg: Vec<u8>, signature: Vec<u8>) -> Result<Vec<u8>, StatusCodeEnum> {
            let pub_key = crypto_sm::sm::recover_signature(&msg, &signature)?;
            Ok(crypto_sm::sm::pk2address(&pub_key))
        }
    } else if #[cfg(feature = "eth")] {
        fn recover_signature(msg: Vec<u8>, signature: Vec<u8>) -> Result<Vec<u8>, StatusCodeEnum> {
            let pub_key = crypto_eth::eth::recover_signature(&msg, &signature)?;
            Ok(crypto_eth::eth::pk2address(&pub_key))
        }
    }
}

pub async fn recover_signature_async(
    tx_hash: Vec<u8>,
    signature: Vec<u8>,
) -> Result<Vec<u8>, StatusCodeEnum> {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        cfg_if::cfg_if! {
            if #[cfg(feature = "sm")] {
                let _ = send.send(recover_signature(tx_hash, signature));
            } else if #[cfg(feature = "eth")] {
                let _ = send.send(recover_signature_eth(tx_hash, signature));
            }
        }
    });
    match recv.await {
        Ok(res) => res,
        Err(e) => {
            warn!("verify signature failed: {}", e);
            Err(StatusCodeEnum::SigCheckError)
        }
    }
}

pub async fn crypto_check_async(tx: Arc<RawTransaction>) -> Result<(), StatusCodeEnum> {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        cfg_if::cfg_if! {
            if #[cfg(feature = "sm")] {
                let _ = send.send(crypto_sm::sm::crypto_check(&tx));
            } else if #[cfg(feature = "eth")] {
                let _ = send.send(crypto_eth::eth::crypto_check(&tx));
            }
        }
    });
    match recv.await {
        Ok(res) => res,
        Err(e) => {
            warn!("crypto check failed: {}", e);
            Err(StatusCodeEnum::SigCheckError)
        }
    }
}

pub async fn crypto_check_batch_async(txs: Arc<RawTransactions>) -> Result<(), StatusCodeEnum> {
    let (send, recv) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        cfg_if::cfg_if! {
            if #[cfg(feature = "sm")] {
                let _ = send.send(crypto_sm::sm::crypto_check_batch(&txs));
            } else if #[cfg(feature = "eth")] {
                let _ = send.send(crypto_eth::eth::crypto_check_batch(&txs));
            }
        }
    });
    match recv.await {
        Ok(res) => {
            if res.is_success().is_ok() {
                Ok(())
            } else {
                Err(res)
            }
        }
        Err(e) => {
            warn!("crypto check failed: {}", e);
            Err(StatusCodeEnum::SigCheckError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crypto_test() {
        cfg_if::cfg_if! {
            if #[cfg(feature = "sm")] {
                let crypto = crypto_sm::crypto::Crypto::new("example/private_key");
            } else if #[cfg(feature = "eth")] {
                let crypto = crypto_eth::crypto::Crypto::new("example/private_key");
            }
        }

        // message must be a hash value
        let message = hash_data("rivtower".as_bytes());
        let signature = crypto.sign_message(&message).unwrap();

        cfg_if::cfg_if! {
            if #[cfg(feature = "sm")] {
                assert_eq!(signature.len(), crypto_sm::sm::SM2_SIGNATURE_BYTES_LEN);
            } else if #[cfg(feature = "eth")] {
                assert_eq!(
                    signature.len(),
                    crypto_eth::eth::SECP256K1_SIGNATURE_BYTES_LEN
                );
            }
        }

        assert_eq!(
            recover_signature(message, signature).unwrap(),
            hex::decode("14a0d0eb5538d1c5cc69d0f54c18bafec75ba95c").unwrap()
        );
    }
}
