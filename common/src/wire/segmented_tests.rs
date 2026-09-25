use super::*;
use crate::config::Compress;
use std::net::TcpListener;
use std::time::Duration;

fn batch() -> MessageBuffer {
    MessageBuffer::new(buffer::shared_pool(Default::default()).unwrap())
}

#[test]
fn segmented_frames_match_contiguous_encoding_at_every_boundary() {
    let requests = [
        Req::new(Op::Ping).id(3),
        Req::new(Op::Status)
            .id(7)
            .text("abc")
            .number(0x0123456789abcdef)
            .flag(true)
            .bytes(vec![99; 8000]),
        Req::new(Op::AppendExtent).structure({
            let mut s = StructWriter::new();
            s.bytes(&vec![23; 9000]);
            s
        }),
    ];
    for start in pool::segmented::CHUNK_BYTES as u64 - 16..pool::segmented::CHUNK_BYTES as u64 + 1 {
        for req in &requests {
            let mut b = batch();
            b.bytes = pool::SegmentedBuffer::with_start(b.bytes.pool(), start);
            put_request(&mut b, req).unwrap();
            let mut expected = Vec::new();
            put_request(&mut expected, req).unwrap();
            let mut actual = Vec::new();
            b.copy_to(start..b.bytes.end(), &mut actual).unwrap();
            assert_eq!(actual, expected);
            let reader = b.bytes.reader_range(start + 4..b.bytes.end()).unwrap();
            assert_eq!(Req::decode_from(reader, expected.len() - 4).unwrap(), *req);
            let resp = Resp { id: req.id, status: Status::Ok, args: req.args.clone() };
            b.clear();
            put_response(&mut b, &resp).unwrap();
            let reader = b.bytes.reader_range(4..b.bytes.end()).unwrap();
            assert_eq!(Resp::decode_from(reader, b.len() - 4).unwrap(), resp);
        }
    }
}

#[test]
fn framing_error_restores_the_previous_batch() {
    struct Failed;
    impl Message for Failed {
        fn encode_into(&self, out: &mut impl Write) -> std::io::Result<()> {
            out.write_all(&[23; 9000])?;
            Err(std::io::Error::other("injected encoding failure"))
        }
    }
    let mut b = batch();
    put_request(&mut b, &Req::new(Op::Ping)).unwrap();
    let mut before = Vec::new();
    b.copy_to(0..b.bytes.end(), &mut before).unwrap();
    assert!(b.append_message(&Failed).is_err());
    assert_eq!(b.len(), before.len());
    let mut after = Vec::new();
    b.copy_to(0..b.bytes.end(), &mut after).unwrap();
    assert_eq!(after, before);
    assert_eq!(b.bytes.depth(), Some(0));
}

#[test]
fn batching_splits_only_at_frame_boundaries_and_rejects_bad_lengths() {
    let mut b = batch();
    let req = Req::new(Op::Ping);
    put_request(&mut b, &req).unwrap();
    let one = b.len() as u64;
    put_request(&mut b, &req).unwrap();
    put_request(&mut b, &req).unwrap();
    assert_eq!(segment_end_with_limit(&b, 0, one * 2).unwrap(), one * 2);
    assert_eq!(segment_end_with_limit(&b, one * 2, one * 2).unwrap(), one * 3);
    assert_eq!(segment_end_with_limit(&b, 0, 1).unwrap(), one);
    b.append(&[0; 3]).unwrap();
    assert!(segment_end(&b, 0).is_err());
    b.clear();
    b.append(&((MAX_FRAME_BYTES + 1) as u32).to_le_bytes()).unwrap();
    assert!(segment_end(&b, 0).is_err());
    b.clear();
    b.append(&100u32.to_le_bytes()).unwrap();
    assert!(segment_end(&b, 0).is_err());
}

#[test]
fn socket_round_trips_single_and_batched_messages_in_all_sealing_modes() {
    for encrypt in [false, true] {
        for compress in [Compress::None, Compress::Lz4] {
            let mut config = crate::testing::messaging();
            config.encrypt = encrypt;
            config.compress = compress;
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server_config = config.clone();
            let server = std::thread::spawn(move || {
                let stream = listener.accept().unwrap().0;
                stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                stream.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
                let (mut reader, mut writer) = accept(stream, &server_config).unwrap();
                for count in [1, 203] {
                    let mut out = writer.new_buffer();
                    let mut arrived = None;
                    for i in 0..count {
                        let req = reader.request().unwrap();
                        assert_eq!(req.id, i);
                        assert_eq!(reader.more(), i + 1 < count);
                        if i + 1 == count {
                            assert_eq!(reader.at, 0);
                            assert_eq!(reader.open.bytes.end(), 0);
                        }
                        if let Some(time) = arrived {
                            assert_eq!(time, reader.arrived());
                        }
                        arrived = Some(reader.arrived());
                        put_response(
                            &mut out,
                            &Resp { id: req.id, status: Status::Ok, args: req.args },
                        )
                        .unwrap();
                    }
                    writer.send(&mut out).unwrap();
                    assert!(out.is_empty());
                    assert!(out.bytes.chunk(0).is_none(), "retained capacity is not readable");
                }
            });
            let stream = TcpStream::connect(address).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            stream.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut link = connect(stream, &config).unwrap();
            for count in [1, 203] {
                let requests: Vec<_> = (0..count)
                    .map(|i| {
                        // First frame ends three bytes before a block boundary, splitting the next frame's length.
                        match i {
                            0 => Req::new(Op::Ping).id(i).bytes(vec![
                                7;
                                pool::segmented::CHUNK_BYTES
                                    - 21
                            ]),
                            1 => Req::new(Op::Ping).id(i),
                            2 => Req::new(Op::Status).id(i).bytes(vec![19; 300_000]),
                            _ => Req::new(Op::Status).id(i).number(i as u64).text("hello"),
                        }
                    })
                    .collect();
                for req in &requests {
                    put_request(&mut link.plain, req).unwrap();
                }
                link.wire.send(&mut link.plain).unwrap();
                for req in requests {
                    let resp = link.reader.response().unwrap();
                    assert_eq!(resp.id, req.id);
                    assert_eq!(resp.args, req.args);
                }
                assert_eq!(link.reader.at, 0);
                assert_eq!(link.reader.open.bytes.end(), 0);
            }
            server.join().unwrap();
        }
    }
}

#[test]
fn received_invalid_frames_and_segment_lengths_fail_before_dispatch() {
    for plain in [
        vec![1, 2, 3],
        100u32.to_le_bytes().to_vec(),
        ((MAX_FRAME_BYTES + 1) as u32).to_le_bytes().to_vec(),
    ] {
        let (mut wire, mut reader) = crate::testing::link();
        let mut sealed = Vec::new();
        wire.seal.seal(&mut plain.clone(), &mut sealed).unwrap();
        wire.stream.write_all(&sealed).unwrap();
        assert!(reader.request().is_err());
    }
    let (mut wire, mut reader) = crate::testing::link();
    let mut header = [0; seal::HEADER];
    header[..4].copy_from_slice(&((MAX_SEGMENT_BYTES + seal::OVERHEAD + 1) as u32).to_le_bytes());
    wire.stream.write_all(&header).unwrap();
    assert!(reader.request().unwrap_err().to_string().contains("peer announced"));
}

#[test]
fn message_buffers_share_the_configured_process_pool() {
    let a = batch();
    let b = batch();
    assert!(std::ptr::eq(a.bytes.pool(), b.bytes.pool()));
    assert!(a.bytes.depth().is_none());
    let mut different = crate::config::BufferPoolConfig::default();
    different.max_blocks -= 1;
    assert!(buffer::shared_pool(different).is_err());
}
