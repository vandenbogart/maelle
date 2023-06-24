use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read, Write},
    sync::{Arc, Mutex, RwLock},
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        in_reply_to: usize,
    },
    Echo {
        msg_id: usize,
        echo: String,
    },
    EchoOk {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
    Generate {
        msg_id: usize,
    },
    GenerateOk {
        msg_id: usize,
        in_reply_to: usize,
        id: String,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
        msg_id: usize,
    },
    TopologyOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Broadcast {
        msg_id: usize,
        message: usize,
    },
    BroadcastOk {
        msg_id: usize,
        in_reply_to: usize,
    },
    Read {
        msg_id: usize,
    },
    ReadOk {
        msg_id: usize,
        in_reply_to: usize,
        messages: Vec<usize>,
    },
    // Add {
    //     msg_id: usize,
    //     delta: usize,
    // },
    // AddOk {
    //     msg_id: usize,
    //     in_reply_to: usize,
    // },
    // Write {
    //     msg_id: usize,
    //     key: String,
    //     value: usize,
    // },
    // WriteOk {
    //     msg_id: usize,
    //     in_reply_to: usize,
    // },
}

#[derive(Serialize, Deserialize)]
struct Message {
    src: String,
    dest: String,
    body: Payload,
}

struct Node {
    id: String,
    node_ids: Vec<String>,
    last_msg_id: usize,
    topology: HashMap<String, Vec<String>>,
    messages: HashSet<usize>,
    callbacks: HashMap<usize, String>,
}
impl Node {
    fn new(id: String, node_ids: Vec<String>) -> Self {
        Self {
            id,
            node_ids,
            last_msg_id: 0,
            topology: HashMap::new(),
            messages: HashSet::new(),
            callbacks: HashMap::new(),
        }
    }
    fn next_msg_id(&mut self) -> usize {
        self.last_msg_id += 1;
        self.last_msg_id
    }
    fn gen_unique_id(&mut self) -> String {
        let mut copied_node_id = self.id.clone();
        copied_node_id.push_str(&self.last_msg_id.to_string());
        copied_node_id
    }
}

fn init_node(is: &mut impl Read) -> anyhow::Result<Node> {
    let mut os = std::io::stdout().lock();
    let mut line = String::new();
    BufReader::new(is)
        .read_line(&mut line)
        .expect("failed to read init message");
    let m: Message = serde_json::from_str(&line).expect("failed to deserialize init message");
    let node = match m.body {
        Payload::Init {
            msg_id,
            node_id,
            node_ids,
        } => {
            let resp = Message {
                src: node_id.clone(),
                dest: m.src,
                body: Payload::InitOk {
                    in_reply_to: msg_id,
                },
            };
            serde_json::to_writer(&mut os, &resp)?;
            os.write(b"\n")?;
            os.flush()?;
            Node::new(node_id, node_ids)
        }
        _ => anyhow::bail!("received non init message before init"),
    };

    Ok(node)
}

fn send_message(
    src: String,
    dest: String,
    body: Payload,
) -> anyhow::Result<()> {
    let mut os = std::io::stdout().lock();
    let resp = Message { src, dest, body };
    serde_json::to_writer(&mut os, &resp)?;
    os.write(b"\n")?;
    os.flush()?;
    Ok(())
}

// fn request_key(os: &mut impl Write, src: String, body: Payload) -> anyhow::Result<()> {
//     let body = Payload::Read { msg_id:  }

//     Ok(())
// }

fn main() -> anyhow::Result<()> {
    let mut stdin = std::io::stdin().lock();

    let node = init_node(&mut stdin)?;
    let node = Arc::new(Mutex::new(node));

    let mut reader = stdin.lines();

    while let Some(line) = reader.next() {
        let line = line.expect("failed to read line from input stream");
        let m: Message = serde_json::from_str(&line).expect("failed to deserialize message");
        let node = Arc::clone(&node);

        std::thread::spawn(move || -> anyhow::Result<()> {
            match m.body {
                Payload::Echo { msg_id, echo } => {
                    let mut node = node.lock().expect("failed to aquire read lock on node");
                    let body = Payload::EchoOk {
                        msg_id: node.next_msg_id(),
                        in_reply_to: msg_id,
                        echo,
                    };
                    send_message(node.id.clone(), m.src, body)?;
                }
                Payload::EchoOk {
                    msg_id,
                    in_reply_to,
                    echo,
                } => (),
                Payload::Generate { msg_id } => {
                    let mut node = node.lock().expect("failed to aquire read lock on node");
                    let body = Payload::GenerateOk {
                        msg_id: node.next_msg_id(),
                        in_reply_to: msg_id,
                        id: node.gen_unique_id(),
                    };
                    send_message(node.id.clone(), m.src, body)?;
                }
                Payload::GenerateOk {
                    msg_id,
                    in_reply_to,
                    id,
                } => (),
                Payload::Topology { topology, msg_id } => {
                    let mut node = node.lock().expect("failed to aquire read lock on node");
                    node.topology = topology;
                    let body = Payload::TopologyOk {
                        msg_id: node.next_msg_id(),
                        in_reply_to: msg_id,
                    };
                    send_message(node.id.clone(), m.src, body)?;
                }
                Payload::TopologyOk {
                    msg_id,
                    in_reply_to,
                } => (),
                Payload::Broadcast { msg_id, message } => {
                    let mut node = node.lock().expect("failed to aquire read lock on node");
                    let body = Payload::BroadcastOk {
                        msg_id: node.next_msg_id(),
                        in_reply_to: msg_id,
                    };
                    send_message(node.id.clone(), m.src.clone(), body)?;
                    if !node.messages.contains(&message) {
                        node.messages.insert(message);
                        let node_id = node.id.clone();
                        let neighbors = node
                            .topology
                            .get(&node_id)
                            .expect("failed get topology")
                            .clone();
                        for n in neighbors.into_iter() {
                            if n == m.src {
                                continue;
                            }
                            let msg_id = node.next_msg_id();
                            let body = Payload::Broadcast { msg_id, message };
                            node.callbacks.insert(msg_id, n.clone());
                            send_message(node.id.clone(), n, body)?;
                        }
                    };
                }
                Payload::BroadcastOk {
                    msg_id,
                    in_reply_to,
                } => {
                    let mut node = node.lock().expect("failed to lock node");
                    node.callbacks
                        .remove(&in_reply_to)
                        .expect("callback not found");
                }
                Payload::Read { msg_id } => {
                    let mut node = node.lock().expect("failed to aquire read lock on node");
                    let body = Payload::ReadOk {
                        msg_id: node.next_msg_id(),
                        in_reply_to: msg_id,
                        messages: node.messages.clone().into_iter().collect(),
                    };
                    send_message(node.id.clone(), m.src, body)?;
                }
                Payload::ReadOk {
                    msg_id,
                    in_reply_to,
                    messages,
                } => (),
                _ => anyhow::bail!("invalid message received"),
                // Payload::Add { msg_id, delta } => {
                //     let write_body = Payload::Write {
                //         msg_id,
                //         key: "g_counter".to_string(),
                //         value: delta,
                //     }

                // },
            };
            Ok(())
        });
    }

    Ok(())
}
