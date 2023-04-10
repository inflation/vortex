use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Body<P> {
    pub msg_id: Option<u32>,
    pub in_reply_to: Option<u32>,
    #[serde(flatten)]
    pub payload: P,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename = "init")]
pub struct Init {
    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename = "init_ok")]
pub struct InitOk {}
