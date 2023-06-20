use std::fmt::Debug;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, instrument};

use crate::{
    error::{JsonDeError, JsonSerError, NodeError, RpcError, WithReason},
    message::Message,
    node::Node,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KvRequest {
    Read {
        key: Value,
    },
    Write {
        key: Value,
        value: Value,
    },
    Cas {
        key: Value,
        from: Value,
        to: Value,
        create_if_not_exists: Option<bool>,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KvResponse {
    ReadOk { value: Value },
    WriteOk,
    CasOk,
    Error { code: u8, text: String },
}

impl Node {
    pub fn handle_kv(&self, msg: &Message<Value>) -> Result<(), NodeError> {
        match KvResponse::de(&msg.body.payload)? {
            KvResponse::ReadOk { value } => self.ack(msg, Ok(value)),
            KvResponse::WriteOk => self.ack(msg, Ok(json!(null))),
            KvResponse::CasOk => self.ack(msg, Ok(json!(null))),
            KvResponse::Error { code, text } => match code {
                20 => self.ack(msg, Err(RpcError::KeyNotFound)),
                22 => self.ack(msg, Err(RpcError::CasFailed(text))),
                _ => self.ack(msg, Err(RpcError::Unknown(code, text))),
            },
        }
    }

    #[instrument("KV read", skip(self))]
    pub async fn kv_read(
        &self,
        svc: &str,
        key: impl Into<Value> + Debug,
    ) -> Result<Option<Value>, NodeError> {
        match self
            .rpc(svc.into(), KvRequest::Read { key: key.into() })
            .await?
        {
            Ok(v) => Ok(Some(v)),
            Err(RpcError::KeyNotFound) => Ok(None),
            Err(e) => Err(NodeError::new_with("Unexpected response from seq-kv", e)),
        }
    }

    #[instrument("KV write", skip(self))]
    pub async fn kv_write(
        &self,
        svc: &str,
        key: impl Into<Value> + Debug,
        val: impl Into<Value> + Debug,
    ) -> Result<(), NodeError> {
        match self
            .rpc(
                svc.into(),
                KvRequest::Write {
                    key: key.into(),
                    value: val.into(),
                },
            )
            .await?
        {
            Ok(_) => Ok(()),
            Err(e) => Err(NodeError::new_with("Unexpected response from seq-kv", e)),
        }
    }

    #[instrument("KV cas", skip(self))]
    pub async fn kv_cas(
        &self,
        svc: &str,
        key: impl Into<Value> + Debug,
        from: impl Into<Value> + Debug,
        to: impl Into<Value> + Debug,
    ) -> Result<bool, NodeError> {
        match self
            .rpc(
                svc.into(),
                KvRequest::Cas {
                    key: key.into(),
                    from: from.into(),
                    to: to.into(),
                    create_if_not_exists: Some(true),
                },
            )
            .await?
        {
            Ok(_) => Ok(true),
            Err(RpcError::CasFailed(msg)) => {
                debug!(msg, "CAS failed");
                Ok(false)
            }
            Err(e) => Err(NodeError::new_with("Unexpected response from seq-kv", e)),
        }
    }

    #[instrument("KV fetch-and", skip(self, func))]
    pub async fn kv_fetch_and<T>(
        &self,
        svc: &str,
        key: impl Into<Value> + Debug + Clone,
        mut func: impl FnMut(&mut T),
    ) -> Result<T, NodeError>
    where
        T: Default + Debug + Clone + DeserializeOwned + Serialize,
    {
        let mut kv_val = self.kv_read(svc, key.clone()).await?;
        let mut val = kv_val.as_ref().map_or_else(|| Ok(T::default()), T::de)?;
        func(&mut val);

        while !self
            .kv_cas(svc, key.clone(), kv_val, val.ser_val()?)
            .await?
        {
            let new = self
                .kv_read(svc, key.clone())
                .await?
                .with_reason("Failed to read after CAS")?;
            val = T::de(&new)?;
            func(&mut val);

            kv_val = Some(new);
        }

        Ok(val)
    }
}
