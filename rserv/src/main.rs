extern crate zmq;
extern crate rand;

use std::env;
use std::thread;
use std::time::Duration;
use rand::{Rng};

type GenError = Box<std::error::Error>;
type GenResult<T> = Result<T, GenError>;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        run_janus().expect("");
    } else {
        run_ret().expect("");
    };
}

fn incoming_thread() -> GenResult<bool> {
    let context = zmq::Context::new();
    let req = context.socket(zmq::DEALER)?;
    let identity = "Local";
    req.set_identity(identity.as_bytes())?;

    println!("Connect dealer");
    req.connect("tcp://localhost:5555")?;
    println!("Connected");

    let mut i = 1;
    let mut remote_client_id : Option<String> = None;

    loop {
        println!("sending World {}", i);

        //req.send("Local".as_bytes(), zmq::SNDMORE)?;
        req.send("".as_bytes(), zmq::SNDMORE)?;
        //req.send(remote_client_id.as_ref().unwrap().as_bytes(), zmq::SNDMORE)?;
        req.send(format!("World {}", i).as_bytes(), 0)?;
        println!("sent World {}", i);

        loop {
            let mut items = [
                req.as_poll_item(zmq::POLLIN)
            ];

            // Deplete replies
            zmq::poll(&mut items, 0)?;

            if items[0].is_readable() {
                let response = req.recv_string(0)?.unwrap();
                println!("thread received {}", response);

                if response.len() > 0 {
                    println!("thread set remote client to {}", response);
                    remote_client_id = Some(response);
                }
            } else {
                break;
            }
        }

        thread::sleep(Duration::from_millis(5000));

        i = i + 1;
    }

    Ok(true)
}

fn run_janus() -> GenResult<bool> {
    println!("janus");

    let context = zmq::Context::new();
    let local_router = context.socket(zmq::ROUTER)?;
    let remote_router = context.socket(zmq::ROUTER)?;

    let mut remote_client_id : Option<String> = None;

    local_router.bind("tcp://*:5555").expect("Binding fail");
    thread::spawn(move || { incoming_thread(); });

    remote_router.bind("tcp://*:5557").expect("Binding fail");

    loop {
        let mut items = [
            remote_router.as_poll_item(zmq::POLLIN),
            local_router.as_poll_item(zmq::POLLIN)
        ];

        zmq::poll(&mut items[0..if remote_client_id.is_none() { 1 } else { 2 }], 0)?;

        if items[0].is_readable() {
            println!("Can read remote");
            let remote_client_value = remote_router.recv_string(0).expect("Failed to read remote").unwrap();
            remote_router.recv_string(0).expect("Failed to read frame").unwrap();

            // Remote is still connected
            // Read reply and forward to local req
            println!("Read remote client {}", remote_client_value);
            remote_client_id = Some(remote_client_value);

            let reply = remote_router.recv_string(0).unwrap().unwrap();
            println!("Read reply {}", reply);
            local_router.send("Local".as_bytes(), zmq::SNDMORE)?;
            local_router.send("".as_bytes(), zmq::SNDMORE)?;
            local_router.send(reply.as_bytes(), 0)?;
        }

        if items[1].is_readable() {
            println!("Can read local");

            if remote_client_id.is_some() {
                println!("Has client");

                let local_client_id = local_router.recv_string(0)?;
                local_router.recv_string(0)?;
                let next_message = local_router.recv_string(0)?;

                remote_router.send(remote_client_id.expect("no client").as_bytes(), zmq::SNDMORE)?;
                remote_router.send("".as_bytes(), zmq::SNDMORE)?;
                remote_router.send(local_client_id.expect("err").as_bytes(), zmq::SNDMORE)?;
                remote_router.send("".as_bytes(), zmq::SNDMORE)?;
                remote_router.send(next_message.expect("err").as_bytes(), 0)?;

                remote_client_id = None;
            }
        }
    }

    Ok(true)
}

fn run_ret() -> GenResult<bool> {
    println!("ret");

    let ctx = zmq::Context::new();

    let socket = ctx.socket(zmq::REQ)?;
    socket.set_identity(format!("Ret {}", rand::thread_rng().gen_ascii_chars().take(5).collect::<String>()).as_bytes())?;
    socket.connect("tcp://127.0.0.1:5557")?;
    let mut first = true;

    loop {
        if !first {
            /*println!("send name");
            socket.send("Ret".as_bytes(), zmq::SNDMORE).expect("error sending name");
            println!("send empty");
            socket.send("".as_bytes(), zmq::SNDMORE).expect("error sending empty");*/
        }

        first = false;
        println!("send next");
        socket.send("next".as_bytes(), 0).expect("error sending");
        println!("sent");
        let ident = socket.recv_msg(0).expect("error receiving");
        socket.recv_msg(0).expect("error receiving empty");
        let message = socket.recv_msg(0).expect("error receiving");
        println!("Got {}", message.as_str().unwrap());
    }

    Ok(true)
}
