// NBD newstyle protocol constants and data structures.
// Reference: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
#![allow(dead_code)]

// ─── Magic numbers ───────────────────────────────────────────────────────────

/// Server initial magic ("NBDMAGIC")
pub const NBD_MAGIC: u64 = 0x4e42444d41474943;

/// Newstyle magic / option magic ("IHAVEOPT")
pub const NBD_IHAVEOPT: u64 = 0x49484156454f5054;

/// Option reply magic
pub const NBD_REP_MAGIC: u64 = 0x0003e889045565a9;

/// Request magic in transmission phase
pub const NBD_REQUEST_MAGIC: u32 = 0x25609513;

/// Simple reply magic in transmission phase
pub const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

// ─── Handshake flags (server → client) ───────────────────────────────────────

pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// ─── Client flags ────────────────────────────────────────────────────────────

pub const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1 << 0;
pub const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

// ─── Transmission flags (in export info) ─────────────────────────────────────

pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const NBD_FLAG_READ_ONLY: u16 = 1 << 1;
pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
pub const NBD_FLAG_SEND_FUA: u16 = 1 << 3;
pub const NBD_FLAG_ROTATIONAL: u16 = 1 << 4;
pub const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
pub const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
pub const NBD_FLAG_SEND_DF: u16 = 1 << 7;
pub const NBD_FLAG_CAN_MULTI_CONN: u16 = 1 << 8;
pub const NBD_FLAG_SEND_RESIZE: u16 = 1 << 9;
pub const NBD_FLAG_SEND_CACHE: u16 = 1 << 10;
pub const NBD_FLAG_SEND_FAST_ZERO: u16 = 1 << 11;

// ─── Option types (client → server) ──────────────────────────────────────────

pub const NBD_OPT_EXPORT_NAME: u32 = 1;
pub const NBD_OPT_ABORT: u32 = 2;
pub const NBD_OPT_LIST: u32 = 3;
pub const NBD_OPT_PEEK_EXPORT: u32 = 4;
pub const NBD_OPT_STARTTLS: u32 = 5;
pub const NBD_OPT_INFO: u32 = 6;
pub const NBD_OPT_GO: u32 = 7;

// ─── Option reply types (server → client) ────────────────────────────────────

pub const NBD_REP_ACK: u32 = 1;
pub const NBD_REP_SERVER: u32 = 2;
pub const NBD_REP_INFO: u32 = 3;
pub const NBD_REP_ERR_UNSUP: u32 = 0x8000_0001;
pub const NBD_REP_ERR_POLICY: u32 = 0x8000_0002;
pub const NBD_REP_ERR_INVALID: u32 = 0x8000_0003;
pub const NBD_REP_ERR_PLATFORM: u32 = 0x8000_0004;
pub const NBD_REP_ERR_TLS_REQD: u32 = 0x8000_0005;
pub const NBD_REP_ERR_UNKNOWN: u32 = 0x8000_0006;
pub const NBD_REP_ERR_SHUTDOWN: u32 = 0x8000_0007;
pub const NBD_REP_ERR_BLOCK_SIZE_REQD: u32 = 0x8000_0008;
pub const NBD_REP_ERR_TOO_BIG: u32 = 0x8000_0009;

// ─── Info types (used with NBD_OPT_INFO / NBD_OPT_GO) ───────────────────────

pub const NBD_INFO_EXPORT: u16 = 0;
pub const NBD_INFO_NAME: u16 = 1;
pub const NBD_INFO_DESCRIPTION: u16 = 2;
pub const NBD_INFO_BLOCK_SIZE: u16 = 3;

// ─── Command types (transmission phase) ──────────────────────────────────────

pub const NBD_CMD_READ: u16 = 0;
pub const NBD_CMD_WRITE: u16 = 1;
pub const NBD_CMD_DISC: u16 = 2;
pub const NBD_CMD_FLUSH: u16 = 3;
pub const NBD_CMD_TRIM: u16 = 4;
pub const NBD_CMD_CACHE: u16 = 5;
pub const NBD_CMD_WRITE_ZEROES: u16 = 6;
pub const NBD_CMD_BLOCK_STATUS: u16 = 7;
pub const NBD_CMD_RESIZE: u16 = 8;

// ─── Command flags ────────────────────────────────────────────────────────────

pub const NBD_CMD_FLAG_FUA: u16 = 1 << 0;
pub const NBD_CMD_FLAG_NO_HOLE: u16 = 1 << 1;
pub const NBD_CMD_FLAG_DF: u16 = 1 << 2;
pub const NBD_CMD_FLAG_REQ_ONE: u16 = 1 << 3;
pub const NBD_CMD_FLAG_FAST_ZERO: u16 = 1 << 4;

// ─── Error codes ─────────────────────────────────────────────────────────────

pub const NBD_E_OK: u32 = 0;
pub const NBD_E_PERM: u32 = 1;
pub const NBD_E_IO: u32 = 5;
pub const NBD_E_NOMEM: u32 = 12;
pub const NBD_E_INVAL: u32 = 22;
pub const NBD_E_NOSPC: u32 = 28;
pub const NBD_E_OVERFLOW: u32 = 75;
pub const NBD_E_NOTSUP: u32 = 95;
pub const NBD_E_SHUTDOWN: u32 = 108;

// ─── Structures ───────────────────────────────────────────────────────────────

/// Parsed NBD request from the client during transmission phase.
#[derive(Debug)]
pub struct NbdRequest {
    pub flags: u16,
    pub cmd: u16,
    pub handle: u64,
    pub offset: u64,
    pub length: u32,
}

/// Reply to send to the client.
#[derive(Debug)]
pub struct NbdReply {
    pub error: u32,
    pub handle: u64,
}
