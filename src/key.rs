//! Key 编码与 Value 编解码模块
//!
//! 所有多字节整数统一使用**大端字节序**（`.to_be_bytes()`），
//! 保证 RocksDB 字典序与数值序一致，范围扫描正确。
//!
//! cf_dedup Key 结构：
//!   [namespace_hash: u64 (8B)] + [key_hash: u128 (16B)]
//!   总长度：24 字节
//!
//! cf_expire Key 结构：
//!   [expire_at_ms: u64 (8B)] + [namespace_hash: u64 (8B)] + [key_hash: u128 (16B)]
//!   总长度：32 字节
//!
//! cf_dedup Value 结构：
//!   [expire_at_ms: u64 (8B)] + [created_at_ms: u64 (8B)] + [original_key_len: u32 (4B)]
//!   + [original_key_bytes] + [metadata_bytes]

use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

use crate::error::{DedupError, Result};

/// 命名空间 ID（u64 哈希）
pub type NamespaceId = u64;

/// cf_dedup Key 长度
pub const DEDUP_KEY_LEN: usize = 8 + 16; // namespace_hash + key_hash

/// cf_expire Key 长度
pub const EXPIRE_KEY_LEN: usize = 8 + 8 + 16; // expire_at + namespace_hash + key_hash

/// Value 头部长度（不包含 original_key 和 metadata）
pub const VALUE_HEADER_LEN: usize = 8 + 8 + 4; // expire_at + created_at + key_len

/// 计算命名空间的 64-bit 哈希
#[inline]
pub fn hash_namespace(ns: &[u8]) -> NamespaceId {
    xxh3_64(ns)
}

/// 计算 Key 的 128-bit 哈希
#[inline]
pub fn hash_key(key: &[u8]) -> u128 {
    xxh3_128(key)
}

// ---------------------------------------------------------------------------
// cf_dedup Key 构造
// ---------------------------------------------------------------------------

/// 构造 cf_dedup 的 Key：namespace_hash (BE) + key_hash (BE)
#[inline]
pub fn build_dedup_key(ns_hash: NamespaceId, key_hash: u128) -> [u8; DEDUP_KEY_LEN] {
    let mut buf = [0u8; DEDUP_KEY_LEN];
    buf[0..8].copy_from_slice(&ns_hash.to_be_bytes());
    buf[8..24].copy_from_slice(&key_hash.to_be_bytes());
    buf
}

/// 从 cf_dedup Key 解析出 ns_hash 和 key_hash
#[inline]
pub fn parse_dedup_key(bytes: &[u8]) -> Result<(NamespaceId, u128)> {
    if bytes.len() != DEDUP_KEY_LEN {
        return Err(DedupError::Codec(format!(
            "dedup key 长度错误: 期望 {}, 实际 {}",
            DEDUP_KEY_LEN,
            bytes.len()
        )));
    }
    let mut ns_buf = [0u8; 8];
    ns_buf.copy_from_slice(&bytes[0..8]);
    let ns_hash = u64::from_be_bytes(ns_buf);

    let mut key_buf = [0u8; 16];
    key_buf.copy_from_slice(&bytes[8..24]);
    let key_hash = u128::from_be_bytes(key_buf);

    Ok((ns_hash, key_hash))
}

// ---------------------------------------------------------------------------
// cf_expire Key 构造
// ---------------------------------------------------------------------------

/// 构造 cf_expire 的 Key：expire_at_ms (BE) + namespace_hash (BE) + key_hash (BE)
#[inline]
pub fn build_expire_key(expire_at_ms: u64, ns_hash: NamespaceId, key_hash: u128) -> [u8; EXPIRE_KEY_LEN] {
    let mut buf = [0u8; EXPIRE_KEY_LEN];
    buf[0..8].copy_from_slice(&expire_at_ms.to_be_bytes());
    buf[8..16].copy_from_slice(&ns_hash.to_be_bytes());
    buf[16..32].copy_from_slice(&key_hash.to_be_bytes());
    buf
}

/// 从 cf_expire Key 解析出 expire_at_ms, ns_hash, key_hash
#[inline]
pub fn parse_expire_key(bytes: &[u8]) -> Result<(u64, NamespaceId, u128)> {
    if bytes.len() != EXPIRE_KEY_LEN {
        return Err(DedupError::Codec(format!(
            "expire key 长度错误: 期望 {}, 实际 {}",
            EXPIRE_KEY_LEN,
            bytes.len()
        )));
    }
    let mut ts_buf = [0u8; 8];
    ts_buf.copy_from_slice(&bytes[0..8]);
    let expire_at_ms = u64::from_be_bytes(ts_buf);

    let mut ns_buf = [0u8; 8];
    ns_buf.copy_from_slice(&bytes[8..16]);
    let ns_hash = u64::from_be_bytes(ns_buf);

    let mut key_buf = [0u8; 16];
    key_buf.copy_from_slice(&bytes[16..32]);
    let key_hash = u128::from_be_bytes(key_buf);

    Ok((expire_at_ms, ns_hash, key_hash))
}

/// 构造 cf_expire 范围扫描的起始 Key（仅包含时间戳前缀）
#[inline]
pub fn expire_key_prefix(ts_ms: u64) -> [u8; 8] {
    ts_ms.to_be_bytes()
}

// ---------------------------------------------------------------------------
// cf_dedup Value 编解码
// ---------------------------------------------------------------------------

/// 去重记录值
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupValue {
    /// 过期时间戳（毫秒）
    pub expire_at_ms: u64,
    /// 创建时间戳（毫秒）
    pub created_at_ms: u64,
    /// 原始 Key（用于 Hash 冲突校验）
    pub original_key: Vec<u8>,
    /// 业务元数据（可选）
    pub metadata: Vec<u8>,
}

impl DedupValue {
    /// 序列化为字节数组
    pub fn encode(&self) -> Result<Vec<u8>> {
        let key_len = self.original_key.len();
        if key_len > u32::MAX as usize {
            return Err(DedupError::Codec("original_key 过长".into()));
        }
        let total = VALUE_HEADER_LEN + key_len + self.metadata.len();
        let mut buf = Vec::with_capacity(total);

        buf.extend_from_slice(&self.expire_at_ms.to_be_bytes());
        buf.extend_from_slice(&self.created_at_ms.to_be_bytes());
        buf.extend_from_slice(&(key_len as u32).to_be_bytes());
        buf.extend_from_slice(&self.original_key);
        buf.extend_from_slice(&self.metadata);

        Ok(buf)
    }

    /// 从字节数组反序列化
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < VALUE_HEADER_LEN {
            return Err(DedupError::Codec(format!(
                "value 长度不足: 至少 {}, 实际 {}",
                VALUE_HEADER_LEN,
                bytes.len()
            )));
        }

        let mut ts_buf = [0u8; 8];
        ts_buf.copy_from_slice(&bytes[0..8]);
        let expire_at_ms = u64::from_be_bytes(ts_buf);

        ts_buf.copy_from_slice(&bytes[8..16]);
        let created_at_ms = u64::from_be_bytes(ts_buf);

        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&bytes[16..20]);
        let key_len = u32::from_be_bytes(len_buf) as usize;

        let key_end = VALUE_HEADER_LEN + key_len;
        if bytes.len() < key_end {
            return Err(DedupError::Codec(format!(
                "value 截断: 期望至少 {} 字节, 实际 {}",
                key_end,
                bytes.len()
            )));
        }

        let original_key = bytes[VALUE_HEADER_LEN..key_end].to_vec();
        let metadata = bytes[key_end..].to_vec();

        Ok(Self {
            expire_at_ms,
            created_at_ms,
            original_key,
            metadata,
        })
    }

    /// 是否已过期
    #[inline]
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_at_ms <= now_ms
    }
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dedup_key_roundtrip() {
        let ns = hash_namespace(b"test_ns");
        let kh = hash_key(b"hello world");
        let key = build_dedup_key(ns, kh);
        let (ns2, kh2) = parse_dedup_key(&key).unwrap();
        assert_eq!(ns, ns2);
        assert_eq!(kh, kh2);
    }

    #[test]
    fn test_expire_key_roundtrip() {
        let ts = 1_700_000_000_000u64;
        let ns = hash_namespace(b"ns1");
        let kh = hash_key(b"key1");
        let key = build_expire_key(ts, ns, kh);
        let (ts2, ns2, kh2) = parse_expire_key(&key).unwrap();
        assert_eq!(ts, ts2);
        assert_eq!(ns, ns2);
        assert_eq!(kh, kh2);
    }

    #[test]
    fn test_expire_key_ordering() {
        // 验证大端字节序下字典序与数值序一致
        let k1 = build_expire_key(1000, 0, 0);
        let k2 = build_expire_key(2000, 0, 0);
        assert!(k1 < k2);

        // 同时间戳下，按 namespace_hash 排序
        let k3 = build_expire_key(1000, 1, 0);
        assert!(k1 < k3);
    }

    #[test]
    fn test_value_roundtrip() {
        let v = DedupValue {
            expire_at_ms: 1_700_000_000_000,
            created_at_ms: 1_699_000_000_000,
            original_key: b"https://example.com/page/123".to_vec(),
            metadata: b"{\"depth\":3}".to_vec(),
        };
        let encoded = v.encode().unwrap();
        let decoded = DedupValue::decode(&encoded).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn test_value_no_metadata() {
        let v = DedupValue {
            expire_at_ms: 100,
            created_at_ms: 50,
            original_key: b"k".to_vec(),
            metadata: vec![],
        };
        let encoded = v.encode().unwrap();
        let decoded = DedupValue::decode(&encoded).unwrap();
        assert_eq!(v, decoded);
        assert!(decoded.metadata.is_empty());
    }

    #[test]
    fn test_is_expired() {
        let v = DedupValue {
            expire_at_ms: 1000,
            created_at_ms: 0,
            original_key: vec![],
            metadata: vec![],
        };
        assert!(!v.is_expired(999));
        assert!(v.is_expired(1000));
        assert!(v.is_expired(1001));
    }
}
