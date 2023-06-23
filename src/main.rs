use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Read, Write};

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
}
impl Node {
    fn new(id: String, node_ids: Vec<String>) -> Self {
        Self {
            id,
            node_ids,
            last_msg_id: 0,
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

fn init_node(is: &mut impl Read, os: &mut impl Write) -> anyhow::Result<Node> {
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
            serde_json::to_writer(&mut *os, &resp)?;
            os.write(b"\n")?;
            os.flush()?;
            Node::new(node_id, node_ids)
        }
        _ => anyhow::bail!("received non init message before init"),
    };

    Ok(node)
}

fn main() -> anyhow::Result<()> {
    let mut stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();
    let mut node = init_node(&mut stdin, &mut stdout)?;

    let mut reader = stdin.lines();

    while let Some(line) = reader.next() {
        let line = line.expect("failed to read line from input stream");
        let m: Message = serde_json::from_str(&line).expect("failed to deserialize message");

        match m.body {
            Payload::Echo { msg_id, echo } => {
                let resp = Message {
                    src: node.id.clone(),
                    dest: m.src,
                    body: Payload::EchoOk {
                        msg_id: node.next_msg_id(),
                        in_reply_to: msg_id,
                        echo,
                    },
                };
                serde_json::to_writer(&mut stdout, &resp)?;
                stdout.write(b"\n")?;
                stdout.flush()?;
            }
            Payload::EchoOk {
                msg_id,
                in_reply_to,
                echo,
            } => (),
            Payload::Generate { msg_id } => {
                let resp = Message {
                    src: node.id.clone(),
                    dest: m.src,
                    body: Payload::GenerateOk {
                        msg_id: node.next_msg_id(),
                        in_reply_to: msg_id,
                        id: node.gen_unique_id(),
                    },
                };
                serde_json::to_writer(&mut stdout, &resp)?;
                stdout.write(b"\n")?;
                stdout.flush()?;
            }
            Payload::GenerateOk {
                msg_id,
                in_reply_to,
                id,
            } => (),
            _ => anyhow::bail!("invalid message received"),
        }
    }
    Ok(())
}
