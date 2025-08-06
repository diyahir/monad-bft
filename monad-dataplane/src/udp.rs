// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::VecDeque,
    io::{Error, ErrorKind},
    net::SocketAddr,
    os::fd::{AsRawFd, FromRawFd},
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use monoio::{
    net::udp::UdpSocket,
    spawn,
    time::{self, timeout},
};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use super::{RecvUdpMsg, UdpMsg};
use crate::buffer_ext::SocketBufferExt;

// When running in docker with vpnkit, the maximum safe MTU is 1480, as per:
// https://github.com/moby/vpnkit/tree/v0.5.0/src/hostnet/slirp.ml#L17-L18
pub const DEFAULT_MTU: u16 = 1480;

const IPV4_HDR_SIZE: u16 = 20;
const UDP_HDR_SIZE: u16 = 8;
pub const fn segment_size_for_mtu(mtu: u16) -> u16 {
    mtu - IPV4_HDR_SIZE - UDP_HDR_SIZE
}

pub const DEFAULT_SEGMENT_SIZE: u16 = segment_size_for_mtu(DEFAULT_MTU);

const ETHERNET_MTU: u16 = 1500;
const ETHERNET_SEGMENT_SIZE: u16 = segment_size_for_mtu(ETHERNET_MTU);

pub fn spawn_tasks(
    local_addr: SocketAddr,
    udp_ingress_tx: mpsc::Sender<RecvUdpMsg>,
    udp_egress_rx: mpsc::Receiver<(SocketAddr, UdpMsg)>,
    up_bandwidth_mbps: u64,
    buffer_size: Option<usize>,
) {
    // Bind the UDP socket and clone it for use in different tasks.
    let udp_socket_rx = UdpSocket::bind(local_addr).unwrap();

    if let Some(requested_buffer_size) = buffer_size {
        if let Err(e) = udp_socket_rx.set_recv_buffer_size(requested_buffer_size) {
            panic!("set_recv_buffer_size to {requested_buffer_size} failed with: {e}");
        }
        let actual_buffer_size = udp_socket_rx
            .recv_buffer_size()
            .expect("get recv buffer size");
        if actual_buffer_size < requested_buffer_size {
            panic!("unable to set udp receive buffer size to {requested_buffer_size}. Got {actual_buffer_size} instead. Set net.core.rmem_max to at least {requested_buffer_size}");
        }
    }
    if let Some(requested_buffer_size) = buffer_size {
        if let Err(e) = udp_socket_rx.set_send_buffer_size(requested_buffer_size) {
            panic!("set_send_buffer_size to {requested_buffer_size} failed with: {e}");
        }
        let actual_buffer_size = udp_socket_rx
            .send_buffer_size()
            .expect("get send buffer size");
        if actual_buffer_size < requested_buffer_size {
            panic!("unable to set udp send buffer size to {requested_buffer_size}. got {actual_buffer_size} instead. set net.core.wmem_max to at least {requested_buffer_size}");
        }
    }

    let raw_fd = udp_socket_rx.as_raw_fd();
    let udp_socket_tx =
        UdpSocket::from_std(unsafe { std::net::UdpSocket::from_raw_fd(raw_fd) }).unwrap();

    {
        const MTU_DISCOVER: libc::c_int = libc::IP_PMTUDISC_OMIT;

        if unsafe {
            libc::setsockopt(
                raw_fd,
                libc::SOL_IP,
                libc::IP_MTU_DISCOVER,
                &MTU_DISCOVER as *const _ as _,
                std::mem::size_of_val(&MTU_DISCOVER) as _,
            )
        } != 0
        {
            panic!(
                "set IP_MTU_DISCOVER failed with: {}",
                Error::last_os_error()
            );
        }
    }

    {
        const SO_PRIORITY_VALUE: libc::c_int = 6;

        if unsafe {
            libc::setsockopt(
                raw_fd,
                libc::SOL_SOCKET,
                libc::SO_PRIORITY,
                &SO_PRIORITY_VALUE as *const _ as _,
                std::mem::size_of_val(&SO_PRIORITY_VALUE) as _,
            )
        } != 0
        {
            panic!("set SO_PRIORITY failed with: {}", Error::last_os_error());
        }
    }

    spawn(rx(udp_socket_rx, udp_ingress_tx));
    spawn(tx(udp_socket_tx, udp_egress_rx, up_bandwidth_mbps));
}

async fn rx(udp_socket_rx: UdpSocket, udp_ingress_tx: mpsc::Sender<RecvUdpMsg>) {
    loop {
        let buf = BytesMut::with_capacity(ETHERNET_SEGMENT_SIZE.into());

        match udp_socket_rx.recv_from(buf).await {
            (Ok((len, src_addr)), buf) => {
                let payload = buf.freeze();

                let msg = RecvUdpMsg {
                    src_addr,
                    payload,
                    stride: len.max(1).try_into().unwrap(),
                };

                if let Err(err) = udp_ingress_tx.send(msg).await {
                    warn!(?src_addr, ?err, "error queueing up received UDP message");
                    break;
                }
            }
            (Err(err), _buf) => {
                warn!("udp_socket_rx.recv_from() error {}", err);
            }
        }
    }
}

const PACING_SLEEP_OVERSHOOT_DETECTION_WINDOW: Duration = Duration::from_millis(100);
const QUEUED_MESSAGE_LIMIT: usize = 10_000;

async fn tx(
    socket_tx: UdpSocket,
    mut udp_egress_rx: mpsc::Receiver<(SocketAddr, UdpMsg)>,
    up_bandwidth_mbps: u64,
) {
    let mut udp_segment_size: u16 = DEFAULT_SEGMENT_SIZE;
    set_udp_segment_size(&socket_tx, udp_segment_size);
    let mut max_chunk: u16 = max_write_size_for_segment_size(udp_segment_size);

    let mut next_transmit = Instant::now();

    let mut message_queues: [VecDeque<(SocketAddr, Bytes, u16)>; 2] =
        [VecDeque::new(), VecDeque::new()];

    loop {
        if fill_message_queues(&mut udp_egress_rx, &mut message_queues)
            .await
            .is_err()
        {
            return;
        }

        let (queue_index, (addr, mut payload, stride)) =
            dequeue_highest_priority(&mut message_queues);

        if udp_segment_size != stride {
            udp_segment_size = stride;
            set_udp_segment_size(&socket_tx, udp_segment_size);
            max_chunk = max_write_size_for_segment_size(udp_segment_size);
        }

        // Transmit the first max_chunk bytes of this (addr, payload) pair.
        let chunk = payload.split_to(payload.len().min(max_chunk.into()));
        let chunk_len = chunk.len();

        let now = Instant::now();

        if next_transmit > now {
            pace_with_message_collection(
                next_transmit - now,
                &mut udp_egress_rx,
                &mut message_queues,
            )
            .await;
        } else {
            let late = now - next_transmit;

            if late > PACING_SLEEP_OVERSHOOT_DETECTION_WINDOW {
                next_transmit = now;
            }
        }

        let (ret, chunk) = socket_tx.send_to(chunk, addr).await;

        if let Err(err) = &ret {
            match err.kind() {
                // ENETUNREACH is returned when trying to send to an IPv4 address from a
                // socket bound to a local IPv6 address.
                ErrorKind::NetworkUnreachable => debug!(
                    local_addr =? socket_tx.local_addr().unwrap(),
                    ?addr,
                    "send address family mismatch. message is dropped"
                ),

                // TODO: An EINVAL return is likely due to MTU/GSO issues -- we should fall
                // back to disabling GSO for this chunk and transmitting the constituent
                // segments individually.
                ErrorKind::InvalidInput => warn!(
                    local_addr =? socket_tx.local_addr().unwrap(),
                    udp_segment_size,
                    max_chunk,
                    ?addr,
                    len = chunk.len(),
                    "got EINVAL on send. message is dropped"
                ),

                // EAFNOSUPPORT is returned when trying to send to an IPv6 address from a
                // socket bound to an IPv4 address.
                //
                // EAFNOSUPPORT is returned as ErrorKind::Uncategorized, which can't be
                // matched against, so it has to be tested for under the wildcard match.
                _ => {
                    if is_eafnosupport(err) {
                        debug!(
                            local_addr =? socket_tx.local_addr().unwrap(),
                            ?addr,
                            "send address family mismatch. message is dropped"
                        );
                    } else {
                        error!(
                            local_addr =? socket_tx.local_addr().unwrap(),
                            udp_segment_size,
                            max_chunk,
                            ?addr,
                            len = chunk.len(),
                            ?err,
                            "unexpected send error. message is dropped"
                        );
                    }
                }
            }
        }

        // the remainder of the message is re-queued only if the send is succesful
        if ret.is_ok() {
            next_transmit +=
                Duration::from_nanos((chunk_len as u64) * 8 * 1000 / up_bandwidth_mbps);

            // Re-queue (addr, payload) at the end of the list if there are bytes left to transmit.
            if !payload.is_empty() {
                message_queues[queue_index].push_back((addr, payload, stride));
            }
        }
    }
}

async fn fill_message_queues(
    udp_egress_rx: &mut mpsc::Receiver<(SocketAddr, UdpMsg)>,
    message_queues: &mut [VecDeque<(SocketAddr, Bytes, u16)>; 2],
) -> Result<(), ()> {
    while message_queues.iter().all(|q| q.is_empty()) || !udp_egress_rx.is_empty() {
        match udp_egress_rx.recv().await {
            Some((addr, udp_msg)) => {
                message_queues[udp_msg.priority as usize].push_back((
                    addr,
                    udp_msg.payload,
                    udp_msg.stride,
                ));
            }
            None => return Err(()),
        }
    }
    Ok(())
}

fn dequeue_highest_priority(
    message_queues: &mut [VecDeque<(SocketAddr, Bytes, u16)>; 2],
) -> (usize, (SocketAddr, Bytes, u16)) {
    for (priority, queue) in message_queues.iter_mut().enumerate() {
        if let Some(msg) = queue.pop_front() {
            return (priority, msg);
        }
    }
    unreachable!("fill_message_queues ensures at least one queue is non-empty")
}

async fn pace_with_message_collection(
    sleep_duration: Duration,
    udp_egress_rx: &mut mpsc::Receiver<(SocketAddr, UdpMsg)>,
    message_queues: &mut [VecDeque<(SocketAddr, Bytes, u16)>; 2],
) {
    let total_queued: usize = message_queues.iter().map(|q| q.len()).sum();

    if total_queued < QUEUED_MESSAGE_LIMIT {
        let deadline = Instant::now() + sleep_duration;

        while Instant::now() < deadline {
            let remaining = deadline - Instant::now();

            match timeout(remaining, udp_egress_rx.recv()).await {
                Ok(Some((addr, udp_msg))) => {
                    message_queues[udp_msg.priority as usize].push_back((
                        addr,
                        udp_msg.payload,
                        udp_msg.stride,
                    ));

                    let total: usize = message_queues.iter().map(|q| q.len()).sum();
                    if total >= QUEUED_MESSAGE_LIMIT {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => break, // Timeout
            }
        }
    } else {
        time::sleep(sleep_duration).await;
    }
}

fn set_udp_segment_size(socket: &UdpSocket, udp_segment_size: u16) {
    let udp_segment_size: libc::c_int = udp_segment_size as i32;

    if unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_UDP,
            libc::UDP_SEGMENT,
            &udp_segment_size as *const _ as _,
            std::mem::size_of_val(&udp_segment_size) as _,
        )
    } != 0
    {
        panic!("set UDP_SEGMENT failed with: {}", Error::last_os_error());
    }
}

const MAX_AGGREGATED_WRITE_SIZE: u16 = 65535 - IPV4_HDR_SIZE - UDP_HDR_SIZE;
const MAX_AGGREGATED_SEGMENTS: u16 = 128;

fn max_write_size_for_segment_size(segment_size: u16) -> u16 {
    (MAX_AGGREGATED_WRITE_SIZE / segment_size).min(MAX_AGGREGATED_SEGMENTS) * segment_size
}

// This is very very ugly, but there is no other way to figure this out.
fn is_eafnosupport(err: &Error) -> bool {
    const EAFNOSUPPORT: &str = "Address family not supported by protocol";

    let err = format!("{}", err);

    err.len() >= EAFNOSUPPORT.len() && &err[0..EAFNOSUPPORT.len()] == EAFNOSUPPORT
}
