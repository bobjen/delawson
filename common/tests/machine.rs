//! Public test-only software keys; these fixtures are not credentials.
use common::{
    config::{Compress, Messaging, SendMode},
    machine::{Authentication, Member, Stream},
    shamir,
};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::Duration,
};
const A: &[u8] = include_bytes!("fixtures/machine-a.der");
const B: &[u8] = include_bytes!("fixtures/machine-b.der");
fn member(private: &[u8], services: &[&str]) -> Member {
    let key = rustls::crypto::ring::sign::any_supported_type(&PrivateKeyDer::Pkcs8(
        PrivatePkcs8KeyDer::from(private.to_vec()),
    ))
    .unwrap();
    Member {
        public_key: shamir::hex(key.public_key().unwrap().as_ref()),
        services: services.iter().map(|s| s.to_string()).collect(),
    }
}
fn config(private: Option<&[u8]>, members: Vec<Member>) -> (tempfile::TempDir, Messaging) {
    let dir = tempfile::tempdir().unwrap();
    let path = private.map(|p| {
        let path = dir.path().join("key.der");
        std::fs::write(&path, p).unwrap();
        path
    });
    let config = Messaging {
        authentication: Some(Authentication {
            members,
            private_key: path,
            tpm_unique: None,
            public_key: None,
            tpm_device: "/dev/tpmrm0".into(),
            owner_auth: None,
        }),
        expected_peer: Some("KS0".into()),
        ring_identities: Default::default(),
        local_ring: None,
        ignore_ring_identity: false,
        send: SendMode::Direct,
        encrypt: true,
        compress: Compress::None,
        compress_min: 64,
        compress_sample: 0,
        peer_conns: 1,
        key: "00".repeat(16),
    };
    (dir, config)
}
fn socket(address: std::net::SocketAddr) -> TcpStream {
    let socket = TcpStream::connect(address).unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    socket.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    socket
}
#[test]
fn authenticated_and_anonymous_clients_and_split_io() {
    for private in [Some(B), None] {
        let members = vec![member(A, &["KS0"]), member(B, &["EN0"])];
        let (_sdir, server) = config(Some(A), members.clone());
        let (_cdir, client) = config(private, members);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let expect = private.is_some();
        let server = std::thread::spawn(move || {
            let mut stream = Stream::new(listener.accept().unwrap().0, &server, true).unwrap();
            assert_eq!(stream.peer_key.is_some(), expect);
            let mut bytes = vec![0; 256 * 1024];
            stream.read_exact(&mut bytes).unwrap();
            assert!(bytes.iter().all(|b| *b == 42));
            stream.write_all(&bytes).unwrap();
        });
        let mut writer = Stream::new(socket(address), &client, false).unwrap();
        let mut reader = writer.try_clone().unwrap();
        let (tx, rx) = mpsc::channel();
        let reading = std::thread::spawn(move || {
            tx.send(()).unwrap();
            let mut bytes = vec![0; 256 * 1024];
            reader.read_exact(&mut bytes).unwrap();
            assert!(bytes.iter().all(|b| *b == 42));
        });
        rx.recv().unwrap();
        writer.write_all(&vec![42; 256 * 1024]).unwrap();
        reading.join().unwrap();
        server.join().unwrap();
    }
}
#[test]
fn unknown_client_is_rejected() {
    let (_sdir, server) = config(Some(A), vec![member(A, &["KS0"])]);
    let (_cdir, client) = config(Some(B), vec![member(A, &["KS0"]), member(B, &["EN0"])]);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        Stream::new(listener.accept().unwrap().0, &server, true).is_err()
    });
    let _ = Stream::new(socket(address), &client, false);
    assert!(worker.join().unwrap());
}
#[test]
fn server_must_be_enrolled_for_requested_service() {
    for unknown in [false, true] {
        let (_sdir, server) = config(Some(A), vec![member(A, &["KS0"])]);
        let members = if unknown { vec![member(B, &["KS0"])] } else { vec![member(A, &["EN0"])] };
        let (_cdir, client) = config(None, members);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let _ = Stream::new(listener.accept().unwrap().0, &server, true);
        });
        assert!(Stream::new(socket(address), &client, false).is_err());
        worker.join().unwrap();
    }
}
#[test]
fn enrollment_is_bound_to_public_key_not_a_claimed_name() {
    let a = member(A, &["KS0"]);
    let b = member(B, &["EN0"]);
    let (_dir, config) = config(None, vec![a.clone(), b.clone()]);
    let auth = config.authentication.unwrap();
    let key = |m: &Member| {
        (0..m.public_key.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&m.public_key[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>()
    };
    assert!(auth.permits(Some(&key(&a)), "KS0"));
    assert!(!auth.permits(Some(&key(&b)), "KS0"));
    assert!(!auth.permits(None, "KS0"));
}

#[test]
fn simultaneous_large_writes_do_not_block_readers() {
    let members = vec![member(A, &["KS0"]), member(B, &["EN0"])];
    let (_sd, server) = config(Some(A), members.clone());
    let (_cd, client) = config(Some(B), members);
    fn exchange(mut stream: Stream) {
        let mut writer = stream.try_clone().unwrap();
        let send =
            std::thread::spawn(move || writer.write_all(&vec![17; 4 * 1024 * 1024]).unwrap());
        let mut received = vec![0; 4 * 1024 * 1024];
        stream.read_exact(&mut received).unwrap();
        assert!(received.iter().all(|b| *b == 17));
        send.join().unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let socket = listener.accept().unwrap().0;
        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        socket.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        exchange(Stream::new(socket, &server, true).unwrap());
    });
    exchange(Stream::new(socket(address), &client, false).unwrap());
    server.join().unwrap();
}
