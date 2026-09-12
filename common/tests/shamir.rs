mod tests {
    use common::shamir::*;
    #[test]
    fn known_polynomial() {
        // f(x)=42 + 7x + 19x² in GF(256), independently evaluated at 1,2,3.
        let shares: Vec<_> = [(1, 62), (2, 104), (3, 124)]
            .map(|(x, v)| Share { set: [0; 32], threshold: 3, x, value: [v; 32] })
            .into();
        assert_eq!(recover(&shares).unwrap(), [42; 32]);
    }
    #[test]
    fn every_seven_of_thirteen() {
        let key = random_key().unwrap();
        let shares = split(&key, 7, 13).unwrap();
        for mask in 0u16..(1 << 13) {
            if mask.count_ones() != 7 {
                continue;
            }
            let subset: Vec<_> = shares
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, s)| Share::decode(&s.encode()).unwrap())
                .collect();
            assert_eq!(recover(&subset).unwrap(), key);
        }
        assert!(recover(&shares[..6]).is_err());
        let mut bad = shares.clone();
        bad[6] = bad[0].clone();
        assert!(recover(&bad).is_err());
        bad = shares.clone();
        bad[6].set[0] ^= 1;
        assert!(recover(&bad).is_err());
        bad = shares.clone();
        bad[12].value[0] ^= 1;
        assert!(recover(&bad).is_err());
        assert!(matches(&key, &verifier(&key)));
        assert!(!matches(&[0; 32], &verifier(&key)));
    }
}
