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
    let identity = "LocalDealer";
    req.set_identity(identity.as_bytes())?;

    req.connect("tcp://localhost:5555")?;

    let mut i = 1;

    loop {
        println!("sending {}", i);
        req.send("".as_bytes(), zmq::SNDMORE)?;
        req.send(format!("{}", i).as_bytes(), 0)?;

        loop {
            let mut items = [
                req.as_poll_item(zmq::POLLIN)
            ];

            // Deplete replies
            zmq::poll(&mut items, 0)?;

            if items[0].is_readable() {
                let response = req.recv_string(0)?.unwrap();
            } else {
                break;
            }
        }

        //thread::sleep(Duration::from_millis(1000));

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
    let mut can_send_next_message_to_remote = false;
    let mut should_resend_last_message = false;
    let mut last_message : Option<String> = None;

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
            let remote_client_value = remote_router.recv_string(0).expect("Failed to read remote").unwrap();
            remote_router.recv_string(0).expect("Failed to read frame").unwrap();

            // Remote is still connected
            // Read reply and forward to local req
            can_send_next_message_to_remote = true;

            should_resend_last_message = remote_client_id.is_some() && !remote_client_value.eq(remote_client_id.as_ref().unwrap());

            remote_client_id = Some(remote_client_value);

            let reply = remote_router.recv_string(0).unwrap().unwrap();
            local_router.send("LocalDealer".as_bytes(), zmq::SNDMORE)?;
            local_router.send("".as_bytes(), zmq::SNDMORE)?;
            local_router.send(reply.as_bytes(), 0)?;
        }

        if items[1].is_readable() {
            if can_send_next_message_to_remote {
                let local_client_id = local_router.recv_string(0)?;
                local_router.recv_string(0)?; // Empty frame
                let next_message = local_router.recv_string(0)?; // Data payload
                let include_last_message = should_resend_last_message && last_message.is_some();

                remote_router.send(remote_client_id.as_ref().expect("no client").as_bytes(), zmq::SNDMORE)?;
                remote_router.send("".as_bytes(), zmq::SNDMORE)?;
                remote_router.send(local_client_id.as_ref().expect("err").as_bytes(), zmq::SNDMORE)?;
                remote_router.send("".as_bytes(), zmq::SNDMORE)?;

                if include_last_message {
                    remote_router.send(last_message.as_ref().expect("err").as_bytes(), zmq::SNDMORE)?;
                    should_resend_last_message = false;
                }

                remote_router.send(next_message.as_ref().expect("err").as_bytes(), 0)?;

                last_message = Some(next_message.as_ref().unwrap().clone());

                can_send_next_message_to_remote = false;
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

    loop {
        socket.send("next".as_bytes(), 0).expect("error sending");
        socket.recv_msg(0).expect("error receiving"); // ident
        socket.recv_msg(0).expect("error receiving empty");
        let message = socket.recv_msg(0).expect("error receiving");
        println!("Got {}", message.as_str().unwrap());

        if socket.get_rcvmore().unwrap() {
            let extra_message = socket.recv_msg(0).expect("error receiving");
            println!("Got {}", extra_message.as_str().unwrap());
        }
    }

    Ok(true)
}
