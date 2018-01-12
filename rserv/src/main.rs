extern crate zmq;
extern crate rand;

use std::env;
use std::thread;
use std::time::Duration;
use rand::{Rng};
use std::slice;
use std::mem;

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

fn incoming_thread(context: &zmq::Context) -> GenResult<bool> {
    let dealer= context.socket(zmq::DEALER)?;

    dealer.set_identity("LocalDealer".as_bytes())?;
    dealer.set_sndhwm(1000000); // Allow large outgoing queue

    dealer.connect("inproc://dealer").expect("local connect fail");

    let mut items = [ dealer.as_poll_item(zmq::POLLIN) ];
    let empty_bytes = "".as_bytes();
    let mut i = 1;

    loop {
        thread::sleep(Duration::from_millis(1000));

        //if i % 1000 == 0 {
            println!("sending {}", i);
        //}

        dealer.send(empty_bytes, zmq::SNDMORE)?;
        dealer.send(format!("{}", i).as_bytes(), 0)?;

        loop {
            // Deplete replies
            zmq::poll(&mut items, 0)?;

            if items[0].is_readable() {
                dealer.recv_msg(0)?;
            } else {
                break;
            }
        }

        i = i + 1;
    }

    Ok(true)
}

fn run_janus() -> GenResult<bool> {
    println!("janus");

    let context = zmq::Context::new();
    let local_router = context.socket(zmq::ROUTER)?;
    let remote_router = context.socket(zmq::ROUTER)?;

    let mut can_send_next_message_to_remote = false;
    let mut should_resend_last_message = false;

    let mut remote_client_id : Option<zmq::Message> = None;
    let mut last_message : Option<zmq::Message> = None;

    let empty_bytes = "".as_bytes();
    let local_dealer_bytes = "LocalDealer".as_bytes();

    local_router.bind("inproc://dealer").expect("Binding fail");
    thread::spawn(move || { incoming_thread(&context.clone()); });

    remote_router.bind("tcp://*:5555").expect("Binding fail");

    let mut pollers = [
        remote_router.as_poll_item(zmq::POLLIN),
        local_router.as_poll_item(zmq::POLLIN)
    ];

    let mut i = 1;

    loop {
        zmq::poll(&mut pollers[0..if remote_client_id.is_none() { 1 } else { 2 }], 0)?;

        if pollers[0].is_readable() {
            let remote_client_value = remote_router.recv_msg(0)?;
            remote_router.recv_msg(0)?;

            // Remote is still connected
            // Read reply and forward to local dealer
            can_send_next_message_to_remote = true;

            // If the remote client id changed, we need to resend the last message
            should_resend_last_message = remote_client_id.is_some() && !remote_client_value.eq(&remote_client_id.as_ref().unwrap());

            if remote_client_id.is_none() || should_resend_last_message {
                remote_client_id = Some(zmq::Message::from_slice(&remote_client_value)?);
            }

            let reply = remote_router.recv_msg(0)?;
            local_router.send(local_dealer_bytes, zmq::SNDMORE)?;
            local_router.send(empty_bytes, zmq::SNDMORE)?;
            local_router.send_msg(reply, 0)?;
        }

        if pollers[1].is_readable() && can_send_next_message_to_remote {
            i = i + 1;

            if i % 1000 == 0 {
                println!("route {}" , i);
            }

            let local_client_id = &local_router.recv_msg(0)?;
            local_router.recv_msg(0)?; // Empty frame
            let next_message = local_router.recv_msg(0)?; // Data payload
            let include_last_message = should_resend_last_message && last_message.is_some();

            remote_router.send(remote_client_id.as_ref().unwrap(), zmq::SNDMORE)?;
            remote_router.send(empty_bytes, zmq::SNDMORE)?;

            // Message may have two payloads if we are re-playing the last message.
            // The separation between payloads is determined by the initial 64-bit length value
            // which is the length of the first payload in bytes.
            //
            // If more data exists after that length in the message, that is the second payload.
            // This has to be done because the chumak elixir zmq client does not support multipart
            // message receiving, so we need to layer on this additional protocol to identify the
            // payload boundary.

            if include_last_message {
                let last_message_msg = last_message.unwrap();
                let len : i64 = last_message_msg.len() as i64;
                let lens: &[u8] = unsafe { slice::from_raw_parts((&len as *const i64) as *const _, mem::size_of::<usize>()) };

                remote_router.send(lens, zmq::SNDMORE)?;
                remote_router.send_msg(last_message_msg, zmq::SNDMORE)?;
                should_resend_last_message = false;

                last_message = Some(zmq::Message::from_slice(&next_message)?);
            } else {
                let len : i64 = next_message.len() as i64;
                let lenp: *const i64 = &len;
                let lens: &[u8] = unsafe { slice::from_raw_parts((&len as *const i64) as *const _, mem::size_of::<usize>()) };

                remote_router.send(lens, zmq::SNDMORE)?;
                last_message = Some(zmq::Message::from_slice(&next_message)?);
            }

            remote_router.send_msg(next_message, 0)?;

            can_send_next_message_to_remote = false;
        }
    }

    Ok(true)
}

fn run_ret() -> GenResult<bool> {
    println!("ret");

    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::REQ)?;

    socket.set_identity(format!("Ret {}", rand::thread_rng().gen_ascii_chars().take(5).collect::<String>()).as_bytes())?;
    socket.connect("tcp://127.0.0.1:5555")?;
    let mut i = 0;
    let empty_bytes = "".as_bytes();

    loop {
        socket.send(empty_bytes, 0).expect("error sending");
        let message = socket.recv_msg(0).expect("error receiving");

        //if i % 1000 == 0 {
            println!("Received msg {}", message.as_str().unwrap());
        //}

        i = i + 1;

        if socket.get_rcvmore().unwrap() {
            let extra_message = socket.recv_msg(0).expect("error receiving");
            println!("Got {}", extra_message.as_str().unwrap());
        }
    }

    Ok(true)
}
