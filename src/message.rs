use std::fmt::Debug;

use serde::{Deserialize, Serialize};

pub trait Payload:
    for<'a> Deserialize<'a> + Serialize + Debug + Clone + Send + Sync + 'static
{
}
impl<P> Payload for P where
    P: for<'a> Deserialize<'a> + Serialize + Debug + Clone + Send + Sync + 'static
{
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Body<P> {
    pub msg_id: Option<u32>,
    pub in_reply_to: Option<u32>,
    #[serde(flatten)]
    pub payload: P,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename = "init")]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename = "init_ok")]
pub struct InitOk {}
