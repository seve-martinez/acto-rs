use async_trait::async_trait;
use rand::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Deserialize, Clone)]
struct Record {
    id: u32,
    first_name: String,
    last_name: String,
    email: String,
    addr: String,
    ip_address: String,
}

#[async_trait]
trait Actor {
    fn handle(&mut self, msg: ActorMessage);
    async fn get_msg(&mut self) -> Option<ActorMessage>;
}

struct MyActor<T: Send> {
    recv: mpsc::Receiver<ActorMessage>,
    resource: T,
}

enum ActorMessage {
    GetRandomUser { respond_to: oneshot::Sender<Record> },
}

struct State(HashMap<u32, Record>);

impl State {
    fn new() -> Self {
        let mut db = HashMap::new();
        let file = std::fs::File::open("MOCK_DATA.csv").unwrap();
        let mut rdr = csv::Reader::from_reader(file);

        for result in rdr.deserialize() {
            let record: Record = result.unwrap();
            db.entry(record.id).or_insert(record);
        }
        Self(db)
    }
}

impl Resource for State {
    fn get(&self, uid: u32) -> Option<Record> {
        self.0.get(&uid).cloned()
    }
}

impl MyActor<State> {
    fn new(recv: mpsc::Receiver<ActorMessage>) -> Self {
        let resource = State::new();
        MyActor { recv, resource }
    }
}

trait Resource {
    fn get(&self, uid: u32) -> Option<Record>;
}

#[async_trait]
impl<T: Send + Resource> Actor for MyActor<T> {
    fn handle(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::GetRandomUser { respond_to } => {
                let mut rng = rand::thread_rng();
                let uid = rng.gen_range(0..1000);
                let user = self.resource.get(uid);

                if let Some(user) = user {
                    let _ = respond_to.send(user.to_owned());
                };
            }
        }
    }

    async fn get_msg(&mut self) -> Option<ActorMessage> {
        self.recv.recv().await
    }
}

async fn run<T: Send + Resource>(mut actor: MyActor<T>) {
    while let Some(msg) = actor.get_msg().await {
        actor.handle(msg);
    }
}

#[derive(Clone)]
struct MyActorHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl MyActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = MyActor::new(receiver);
        tokio::spawn(run(actor));

        Self { sender }
    }

    pub async fn get_random_user(&self) -> Record {
        let (send, recv) = oneshot::channel();
        let msg = ActorMessage::GetRandomUser { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor has been killed")
    }
}

#[tokio::main]
async fn main() {
    let handle = MyActorHandle::new();
    for _ in 0..10 {
        let user = handle.get_random_user().await;
        println!("{:?}", user);
    }
}
