use common::config::BufferPoolConfig;
use common::wire::{Arg, MessageBytes, Op, Req, StructWriter, configure_message_pool};
use std::io::Write;

#[test]
fn process_configuration_controls_payload_and_frame_promotion() {
    configure_message_pool(BufferPoolConfig {
        max_blocks: 128,
        initial_blocks: 0,
        small_message_bytes: 1024,
    })
    .unwrap();
    let mut writer = StructWriter::new();
    for _ in 0..1024 {
        writer.number_u8(1);
    }
    let Arg::Struct(_, bytes, _) = writer.finish(Op::Ping) else { panic!() };
    assert!(!bytes.is_segmented());
    let mut bytes = bytes;
    bytes.write_all(&[2]).unwrap();
    assert!(bytes.is_segmented());
    let mut framed = MessageBytes::new();
    common::wire::put_request(&mut framed, &Req::new(Op::Ping).bytes(bytes.clone())).unwrap();
    assert!(framed.is_segmented());
    assert_eq!(bytes.len(), 1025);
    assert!(
        configure_message_pool(BufferPoolConfig::default()).is_err(),
        "configuration cannot change after initialization"
    );
}
