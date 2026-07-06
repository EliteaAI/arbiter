// arbiter.js
//
// Minimal JavaScript port of arbiter's EventNode / ServiceNode / RpcNode,
// scoped to CALLING services from JS -> Python (pylon). It does NOT register
// or host services on the JS side.
//
// Transport assumption: an already-connected Socket.IO client is available
// (e.g. `window.socket`) that relays these events to a real arbiter EventNode
// on the backend:
//     client emits "eventnode_join"  {password, room}   (once, on start)
//     client emits "eventnode_event" <base64 string>     (to publish)
//     client   on  "eventnode_event" <base64 string>     (to receive)
//
// Wire format of every event frame (mirrors arbiter/eventnode/base.py):
//     frame = gzip( pickle.dumps({name, payload}, HIGHEST_PROTOCOL) )
//     if hmacKey: frame = frame + hmac_<digest>(hmacKey, frame)   // raw bytes
//     wire  = base64(frame)                                       // data_base64=True
//
// Copyright 2026 EPAM Systems. Apache-2.0.

/* eslint-disable no-bitwise */

// ---------------------------------------------------------------------------
// Byte / base64 helpers
// ---------------------------------------------------------------------------

function base64Encode(u8) {
  let s = "";
  for (let i = 0; i < u8.length; i += 1) s += String.fromCharCode(u8[i]);
  return btoa(s);
}

function base64Decode(str) {
  const s = atob(str);
  const u8 = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i += 1) u8[i] = s.charCodeAt(i);
  return u8;
}

function concatBytes(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

const UTF8_ENCODER = new TextEncoder();
const UTF8_DECODER = new TextDecoder("utf-8");

// ---------------------------------------------------------------------------
// gzip (native CompressionStream / DecompressionStream)
// ---------------------------------------------------------------------------

async function gzip(u8) {
  const stream = new Blob([u8]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function gunzip(u8) {
  const stream = new Blob([u8]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

// ---------------------------------------------------------------------------
// HMAC (SubtleCrypto). Digest is appended raw and compared constant-time.
// ---------------------------------------------------------------------------

const HMAC_ALGOS = {
  sha1: { name: "SHA-1", size: 20 },
  sha256: { name: "SHA-256", size: 32 },
  sha384: { name: "SHA-384", size: 48 },
  sha512: { name: "SHA-512", size: 64 },
};

async function hmacDigest(keyBytes, dataBytes, digest) {
  const algo = HMAC_ALGOS[digest];
  if (!algo) throw new Error(`Unsupported hmac digest: ${digest}`);
  const key = await crypto.subtle.importKey(
    "raw", keyBytes, { name: "HMAC", hash: algo.name }, false, ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, dataBytes);
  return new Uint8Array(sig);
}

function constantTimeEqual(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i += 1) diff |= a[i] ^ b[i];
  return diff === 0;
}

// ---------------------------------------------------------------------------
// Pickle ENCODER (subset) — emits protocol-4 opcodes for JSON-shaped values.
//
// Supported JS -> Python:
//   null -> None, boolean -> bool, string -> str, Uint8Array -> bytes,
//   Array -> list, plain object / Map -> dict,
//   number: integer -> int, otherwise -> float,
//   BigInt -> int, PyFloat(x) -> float (forces float even for integers).
// We never emit PUT/GET, so no memo bookkeeping is required.
// ---------------------------------------------------------------------------

export class PyFloat {
  constructor(value) { this.value = Number(value); }
}

const OP = {
  PROTO: 0x80, FRAME: 0x95, STOP: 0x2e,
  NONE: 0x4e, NEWTRUE: 0x88, NEWFALSE: 0x89,
  BININT: 0x4a, BININT1: 0x4b, BININT2: 0x4d,
  LONG1: 0x8a, LONG4: 0x8b,
  BINFLOAT: 0x47,
  SHORT_BINUNICODE: 0x8c, BINUNICODE: 0x58, BINUNICODE8: 0x8d,
  SHORT_BINBYTES: 0x43, BINBYTES: 0x42, BINBYTES8: 0x8e,
  EMPTY_LIST: 0x5d, APPENDS: 0x65,
  EMPTY_DICT: 0x7d, SETITEMS: 0x75,
  EMPTY_TUPLE: 0x29, TUPLE1: 0x85, TUPLE2: 0x86, TUPLE3: 0x87, TUPLE: 0x74,
  MARK: 0x28,
  // decoder-only opcodes below
  LONG_BINPUT: 0x72, BINPUT: 0x71, MEMOIZE: 0x94,
  BINGET: 0x68, LONG_BINGET: 0x6a, GET: 0x67, PUT: 0x70,
  BINUNICODE_V: 0x56, STRING: 0x53, BINSTRING: 0x54, SHORT_BINSTRING: 0x55,
  APPEND: 0x61, LIST: 0x6c, SETITEM: 0x73, DICT: 0x64,
  BYTEARRAY8: 0x96,
  GLOBAL: 0x63, STACK_GLOBAL: 0x93, REDUCE: 0x52, BUILD: 0x62,
  NEWOBJ: 0x81, NEWOBJ_EX: 0x92, OBJ: 0x6f, INST: 0x69,
  POP: 0x30, POP_MARK: 0x31, DUP: 0x32,
  EMPTY_SET: 0x8f, FROZENSET: 0x91, ADDITEMS: 0x90,
  INT: 0x49, LONG: 0x4c, FLOAT: 0x46, UNICODE: 0x56,
  EXT1: 0x82, EXT2: 0x83, EXT4: 0x84,
  PERSID: 0x50, BINPERSID: 0x51,
};

class ByteWriter {
  constructor() { this.chunks = []; this.length = 0; }
  byte(b) { this.chunks.push(Uint8Array.of(b)); this.length += 1; }
  bytes(u8) { this.chunks.push(u8); this.length += u8.length; }
  u32le(n) { const b = new Uint8Array(4); new DataView(b.buffer).setUint32(0, n >>> 0, true); this.bytes(b); }
  i32le(n) { const b = new Uint8Array(4); new DataView(b.buffer).setInt32(0, n | 0, true); this.bytes(b); }
  u64le(n) { const b = new Uint8Array(8); new DataView(b.buffer).setBigUint64(0, BigInt(n), true); this.bytes(b); }
  f64be(n) { const b = new Uint8Array(8); new DataView(b.buffer).setFloat64(0, n, false); this.bytes(b); }
  concat() {
    const out = new Uint8Array(this.length);
    let off = 0;
    for (const c of this.chunks) { out.set(c, off); off += c.length; }
    return out;
  }
}

function encodeLongBytes(n) {
  // little-endian two's-complement, minimal (Python pickle encode_long)
  if (n === 0n) return new Uint8Array(0);
  const out = [];
  let v = n;
  for (;;) {
    const b = Number(v & 0xffn);
    out.push(b);
    v >>= 8n; // arithmetic shift for BigInt
    if (v === 0n && !(b & 0x80)) break;
    if (v === -1n && (b & 0x80)) break;
  }
  return Uint8Array.from(out);
}

class Pickler {
  constructor() { this.w = new ByteWriter(); }

  dump(obj) {
    this.w.byte(OP.PROTO);
    this.w.byte(4);
    this.save(obj);
    this.w.byte(OP.STOP);
    return this.w.concat();
  }

  save(obj) {
    if (obj === null || obj === undefined) { this.w.byte(OP.NONE); return; }
    if (obj === true) { this.w.byte(OP.NEWTRUE); return; }
    if (obj === false) { this.w.byte(OP.NEWFALSE); return; }
    const t = typeof obj;
    if (t === "number") { Number.isInteger(obj) ? this.saveInt(BigInt(obj)) : this.saveFloat(obj); return; }
    if (t === "bigint") { this.saveInt(obj); return; }
    if (t === "string") { this.saveStr(obj); return; }
    if (obj instanceof PyFloat) { this.saveFloat(obj.value); return; }
    if (obj instanceof Uint8Array) { this.saveBytes(obj); return; }
    if (ArrayBuffer.isView(obj) || obj instanceof ArrayBuffer) { this.saveBytes(new Uint8Array(obj.buffer || obj)); return; }
    if (Array.isArray(obj)) { this.saveList(obj); return; }
    if (obj instanceof Map) { this.saveDict([...obj.entries()]); return; }
    if (t === "object") { this.saveDict(Object.entries(obj)); return; }
    throw new Error(`Cannot pickle value of type ${t}`);
  }

  saveInt(n) {
    if (n >= 0n && n <= 0xffn) { this.w.byte(OP.BININT1); this.w.byte(Number(n)); return; }
    if (n >= 0n && n <= 0xffffn) { this.w.byte(OP.BININT2); this.w.bytes(Uint8Array.of(Number(n & 0xffn), Number((n >> 8n) & 0xffn))); return; }
    if (n >= -0x80000000n && n <= 0x7fffffffn) { this.w.byte(OP.BININT); this.w.i32le(Number(n)); return; }
    const enc = encodeLongBytes(n);
    if (enc.length < 256) { this.w.byte(OP.LONG1); this.w.byte(enc.length); this.w.bytes(enc); }
    else { this.w.byte(OP.LONG4); this.w.u32le(enc.length); this.w.bytes(enc); }
  }

  saveFloat(x) { this.w.byte(OP.BINFLOAT); this.w.f64be(x); }

  saveStr(s) {
    const b = UTF8_ENCODER.encode(s);
    if (b.length < 256) { this.w.byte(OP.SHORT_BINUNICODE); this.w.byte(b.length); this.w.bytes(b); }
    else if (b.length <= 0xffffffff) { this.w.byte(OP.BINUNICODE); this.w.u32le(b.length); this.w.bytes(b); }
    else { this.w.byte(OP.BINUNICODE8); this.w.u64le(b.length); this.w.bytes(b); }
  }

  saveBytes(b) {
    if (b.length < 256) { this.w.byte(OP.SHORT_BINBYTES); this.w.byte(b.length); this.w.bytes(b); }
    else if (b.length <= 0xffffffff) { this.w.byte(OP.BINBYTES); this.w.u32le(b.length); this.w.bytes(b); }
    else { this.w.byte(OP.BINBYTES8); this.w.u64le(b.length); this.w.bytes(b); }
  }

  saveList(arr) {
    this.w.byte(OP.EMPTY_LIST);
    if (arr.length === 0) return;
    this.w.byte(OP.MARK);
    for (const item of arr) this.save(item);
    this.w.byte(OP.APPENDS);
  }

  saveDict(entries) {
    this.w.byte(OP.EMPTY_DICT);
    if (entries.length === 0) return;
    this.w.byte(OP.MARK);
    for (const [k, v] of entries) { this.save(k); this.save(v); }
    this.w.byte(OP.SETITEMS);
  }
}

export function pickle(obj) { return new Pickler().dump(obj); }

// ---------------------------------------------------------------------------
// Pickle DECODER (subset) — handles protocol 0-5 opcodes we expect from
// CPython for provider/response payloads. Python objects reduced via
// GLOBAL/REDUCE/BUILD (e.g. exceptions) become PyObject placeholders.
// ---------------------------------------------------------------------------

export class PyGlobal {
  constructor(name) { this.name = name; }
}

export class PyObject {
  constructor(cls, args) { this.cls = cls; this.args = args || []; this.state = null; }
}

const MARK_OBJECT = Symbol("pickle-mark");

class Unpickler {
  constructor(u8) {
    this.buf = u8;
    this.view = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
    this.pos = 0;
    this.stack = [];
    this.memo = new Map();
  }

  readByte() { const b = this.buf[this.pos]; this.pos += 1; return b; }
  readN(n) { const s = this.buf.subarray(this.pos, this.pos + n); this.pos += n; return s; }
  readLine() {
    let end = this.pos;
    while (this.buf[end] !== 0x0a) end += 1;
    const s = this.buf.subarray(this.pos, end);
    this.pos = end + 1;
    return UTF8_DECODER.decode(s);
  }
  u16() { const v = this.view.getUint16(this.pos, true); this.pos += 2; return v; }
  i32() { const v = this.view.getInt32(this.pos, true); this.pos += 4; return v; }
  u32() { const v = this.view.getUint32(this.pos, true); this.pos += 4; return v; }
  u64() { const v = this.view.getBigUint64(this.pos, true); this.pos += 8; return Number(v); }
  f64be() { const v = this.view.getFloat64(this.pos, false); this.pos += 8; return v; }

  popMark() {
    // find last MARK
    let i = this.stack.length - 1;
    while (i >= 0 && this.stack[i] !== MARK_OBJECT) i -= 1;
    const items = this.stack.slice(i + 1);
    this.stack.length = i; // drop items and the mark
    return items;
  }

  static toNumber(bytes) {
    if (bytes.length === 0) return 0;
    let n = 0n;
    for (let i = bytes.length - 1; i >= 0; i -= 1) n = (n << 8n) | BigInt(bytes[i]);
    if (bytes[bytes.length - 1] & 0x80) n -= (1n << BigInt(8 * bytes.length));
    return Number.isSafeInteger(Number(n)) ? Number(n) : n;
  }

  load() {
    for (;;) {
      const op = this.readByte();
      switch (op) {
        case OP.PROTO: this.readByte(); break;
        case OP.FRAME: this.pos += 8; break;
        case OP.STOP: return this.stack.pop();

        case OP.NONE: this.stack.push(null); break;
        case OP.NEWTRUE: this.stack.push(true); break;
        case OP.NEWFALSE: this.stack.push(false); break;

        case OP.BININT: this.stack.push(this.i32()); break;
        case OP.BININT1: this.stack.push(this.readByte()); break;
        case OP.BININT2: this.stack.push(this.u16()); break;
        case OP.LONG1: this.stack.push(Unpickler.toNumber(this.readN(this.readByte()))); break;
        case OP.LONG4: this.stack.push(Unpickler.toNumber(this.readN(this.u32()))); break;
        case OP.INT: { const s = this.readLine(); this.stack.push(s === "00" ? false : s === "01" ? true : parseInt(s, 10)); break; }
        case OP.LONG: { const s = this.readLine().replace(/L$/, ""); this.stack.push(Number(BigInt(s))); break; }

        case OP.BINFLOAT: this.stack.push(this.f64be()); break;
        case OP.FLOAT: this.stack.push(parseFloat(this.readLine())); break;

        case OP.SHORT_BINUNICODE: this.stack.push(UTF8_DECODER.decode(this.readN(this.readByte()))); break;
        case OP.BINUNICODE: this.stack.push(UTF8_DECODER.decode(this.readN(this.u32()))); break;
        case OP.BINUNICODE8: this.stack.push(UTF8_DECODER.decode(this.readN(this.u64()))); break;
        case OP.UNICODE: this.stack.push(this.readLine()); break;

        case OP.SHORT_BINBYTES: this.stack.push(this.readN(this.readByte()).slice()); break;
        case OP.BINBYTES: this.stack.push(this.readN(this.u32()).slice()); break;
        case OP.BINBYTES8: this.stack.push(this.readN(this.u64()).slice()); break;
        case OP.BYTEARRAY8: this.stack.push(this.readN(this.u64()).slice()); break;

        case OP.SHORT_BINSTRING: this.stack.push(UTF8_DECODER.decode(this.readN(this.readByte()))); break;
        case OP.BINSTRING: this.stack.push(UTF8_DECODER.decode(this.readN(this.u32()))); break;
        case OP.STRING: { let s = this.readLine(); s = s.slice(1, -1); this.stack.push(s); break; }

        case OP.EMPTY_LIST: this.stack.push([]); break;
        case OP.LIST: this.stack.push(this.popMark()); break;
        case OP.APPEND: { const v = this.stack.pop(); this.stack[this.stack.length - 1].push(v); break; }
        case OP.APPENDS: { const items = this.popMark(); this.stack[this.stack.length - 1].push(...items); break; }

        case OP.EMPTY_TUPLE: this.stack.push([]); break;
        case OP.TUPLE: this.stack.push(this.popMark()); break;
        case OP.TUPLE1: { const a = this.stack.pop(); this.stack.push([a]); break; }
        case OP.TUPLE2: { const b = this.stack.pop(); const a = this.stack.pop(); this.stack.push([a, b]); break; }
        case OP.TUPLE3: { const c = this.stack.pop(); const b = this.stack.pop(); const a = this.stack.pop(); this.stack.push([a, b, c]); break; }

        case OP.EMPTY_DICT: this.stack.push({}); break;
        case OP.DICT: { const items = this.popMark(); const d = {}; for (let i = 0; i < items.length; i += 2) d[items[i]] = items[i + 1]; this.stack.push(d); break; }
        case OP.SETITEM: { const v = this.stack.pop(); const k = this.stack.pop(); this.stack[this.stack.length - 1][k] = v; break; }
        case OP.SETITEMS: { const items = this.popMark(); const d = this.stack[this.stack.length - 1]; for (let i = 0; i < items.length; i += 2) d[items[i]] = items[i + 1]; break; }

        case OP.EMPTY_SET: this.stack.push(new Set()); break;
        case OP.FROZENSET: this.stack.push(new Set(this.popMark())); break;
        case OP.ADDITEMS: { const items = this.popMark(); const s = this.stack[this.stack.length - 1]; for (const it of items) s.add(it); break; }

        case OP.MARK: this.stack.push(MARK_OBJECT); break;
        case OP.POP: this.stack.pop(); break;
        case OP.POP_MARK: this.popMark(); break;
        case OP.DUP: this.stack.push(this.stack[this.stack.length - 1]); break;

        case OP.MEMOIZE: this.memo.set(this.memo.size, this.stack[this.stack.length - 1]); break;
        case OP.PUT: this.memo.set(parseInt(this.readLine(), 10), this.stack[this.stack.length - 1]); break;
        case OP.BINPUT: this.memo.set(this.readByte(), this.stack[this.stack.length - 1]); break;
        case OP.LONG_BINPUT: this.memo.set(this.u32(), this.stack[this.stack.length - 1]); break;
        case OP.GET: this.stack.push(this.memo.get(parseInt(this.readLine(), 10))); break;
        case OP.BINGET: this.stack.push(this.memo.get(this.readByte())); break;
        case OP.LONG_BINGET: this.stack.push(this.memo.get(this.u32())); break;

        case OP.GLOBAL: { const module = this.readLine(); const name = this.readLine(); this.stack.push(new PyGlobal(`${module}.${name}`)); break; }
        case OP.STACK_GLOBAL: { const name = this.stack.pop(); const module = this.stack.pop(); this.stack.push(new PyGlobal(`${module}.${name}`)); break; }
        case OP.REDUCE: { const args = this.stack.pop(); const callable = this.stack.pop(); this.stack.push(new PyObject(callable, args)); break; }
        case OP.NEWOBJ: { const args = this.stack.pop(); const cls = this.stack.pop(); this.stack.push(new PyObject(cls, args)); break; }
        case OP.NEWOBJ_EX: { this.stack.pop(); const args = this.stack.pop(); const cls = this.stack.pop(); this.stack.push(new PyObject(cls, args)); break; }
        case OP.BUILD: { const state = this.stack.pop(); const obj = this.stack[this.stack.length - 1]; if (obj instanceof PyObject) obj.state = state; break; }

        default:
          throw new Error(`Unsupported pickle opcode 0x${op.toString(16)} at pos ${this.pos - 1}`);
      }
    }
  }
}

export function unpickle(u8) { return new Unpickler(u8).load(); }

// Best-effort JS Error from a decoded Python exception (PyObject/PyGlobal).
export function pyToError(value) {
  if (value instanceof PyObject) {
    const cls = value.cls instanceof PyGlobal ? value.cls.name : String(value.cls);
    const msg = value.args && value.args.length ? value.args.map(String).join(", ") : "";
    const err = new Error(msg ? `${cls}: ${msg}` : cls);
    err.pyClass = cls;
    err.pyArgs = value.args;
    err.pyState = value.state;
    return err;
  }
  return new Error(typeof value === "string" ? value : JSON.stringify(value));
}

// ---------------------------------------------------------------------------
// AsyncQueue — promise-based analogue of Python's queue.SimpleQueue.
// get(timeoutMs) resolves with the next item or rejects with QueueEmpty.
// ---------------------------------------------------------------------------

export class QueueEmpty extends Error {
  constructor() { super("QueueEmpty"); this.name = "QueueEmpty"; }
}

class AsyncQueue {
  constructor() { this.items = []; this.waiters = []; }

  put(item) {
    const waiter = this.waiters.shift();
    if (waiter) { clearTimeout(waiter.timer); waiter.resolve(item); }
    else this.items.push(item);
  }

  get(timeoutMs) {
    if (this.items.length) return Promise.resolve(this.items.shift());
    return new Promise((resolve, reject) => {
      const waiter = { resolve, timer: null };
      if (timeoutMs != null) {
        waiter.timer = setTimeout(() => {
          const i = this.waiters.indexOf(waiter);
          if (i >= 0) this.waiters.splice(i, 1);
          reject(new QueueEmpty());
        }, timeoutMs);
      }
      this.waiters.push(waiter);
    });
  }
}

// ---------------------------------------------------------------------------
// EventNode — subscribe / emit over a Socket.IO client.
// ---------------------------------------------------------------------------

export const ANY = Symbol("catch-all");

export class EventNode {
  constructor(socket, {
    room = "events",
    password = "",
    hmacKey = null,
    hmacDigest = "sha512",
    dataBase64 = true,
    logErrors = true,
  } = {}) {
    this.socket = socket;
    this.room = room;
    this.password = password;
    this.hmacKey = typeof hmacKey === "string" ? UTF8_ENCODER.encode(hmacKey) : hmacKey;
    this.hmacDigestName = hmacDigest;
    this.dataBase64 = dataBase64;
    this.logErrors = logErrors;

    this.eventCallbacks = new Map(); // name -> Set<cb>
    this.catchAll = new Set();
    this.started = false;

    this._onEvent = (body) => { this._recv(body).catch((e) => this._logErr("recv", e)); };
  }

  start() {
    if (this.started) return;
    this.socket.emit("eventnode_join", { password: this.password, room: this.room });
    this.socket.on("eventnode_event", this._onEvent);
    this.started = true;
  }

  stop() {
    if (!this.started) return;
    if (this.socket.off) this.socket.off("eventnode_event", this._onEvent);
    this.started = false;
  }

  subscribe(name, cb) {
    if (name === ANY) { this.catchAll.add(cb); return; }
    if (!this.eventCallbacks.has(name)) this.eventCallbacks.set(name, new Set());
    this.eventCallbacks.get(name).add(cb);
  }

  unsubscribe(name, cb) {
    if (name === ANY) { this.catchAll.delete(cb); return; }
    const set = this.eventCallbacks.get(name);
    if (set) set.delete(cb);
  }

  async emit(name, payload = null) {
    let frame = await gzip(pickle({ name, payload }));
    if (this.hmacKey) frame = concatBytes(frame, await hmacDigest(this.hmacKey, frame, this.hmacDigestName));
    this.socket.emit("eventnode_event", this.dataBase64 ? base64Encode(frame) : frame);
  }

  async _recv(body) {
    let frame = this.dataBase64 ? base64Decode(body) : new Uint8Array(body);
    if (this.hmacKey) {
      const size = HMAC_ALGOS[this.hmacDigestName].size;
      const bodyDigest = frame.subarray(frame.length - size);
      frame = frame.subarray(0, frame.length - size);
      const expected = await hmacDigest(this.hmacKey, frame, this.hmacDigestName);
      if (!constantTimeEqual(bodyDigest, expected)) { this._logErr("recv", new Error("Invalid event digest, skipping")); return; }
    }
    const event = unpickle(await gunzip(frame));
    const name = event && event.name;
    const payload = event && event.payload;

    const callbacks = [...this.catchAll];
    const named = this.eventCallbacks.get(name);
    if (named) callbacks.push(...named);
    for (const cb of callbacks) {
      try { cb(name, payload); } catch (e) { this._logErr("callback", e); }
    }
  }

  _logErr(where, err) {
    if (this.logErrors) console.error(`[arbiter.js] ${where}:`, err);
  }
}

// ---------------------------------------------------------------------------
// ServiceNode — CALLER side only (discovery -> request -> response).
// ---------------------------------------------------------------------------

function uuid4() {
  if (crypto.randomUUID) return crypto.randomUUID();
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
  const h = [...b].map((x) => x.toString(16).padStart(2, "0"));
  return `${h.slice(0, 4).join("")}-${h.slice(4, 6).join("")}-${h.slice(6, 8).join("")}-${h.slice(8, 10).join("")}-${h.slice(10).join("")}`;
}

export class ServiceNode {
  constructor(eventNode, {
    idPrefix = "",
    defaultTimeout = null,          // ms; null = wait forever
    defaultDiscoveryAttempts = 1,
  } = {}) {
    this.eventNode = eventNode;
    this.idPrefix = idPrefix;
    this.defaultTimeout = defaultTimeout;
    this.defaultDiscoveryAttempts = defaultDiscoveryAttempts;
    this.queues = new Map(); // queue-name -> AsyncQueue
    this.started = false;

    this._onProvider = (_n, p) => { const q = this.queues.get(p && p.target); if (q) q.put(p); };
    this._onResponse = (_n, p) => { const q = this.queues.get(p && p.target); if (q) q.put(p); };
  }

  start() {
    if (this.started) return;
    if (!this.eventNode.started) this.eventNode.start();
    this.eventNode.subscribe("service_provider", this._onProvider);
    this.eventNode.subscribe("service_response", this._onResponse);
    this.started = true;
  }

  stop() {
    this.eventNode.unsubscribe("service_response", this._onResponse);
    this.eventNode.unsubscribe("service_provider", this._onProvider);
    this.started = false;
  }

  async request(service, {
    args = [],
    kwargs = {},
    timeout = undefined,
    discoveryAttempts = undefined,
  } = {}) {
    if (!this.started) throw new Error("ServiceNode is not started");
    const to = timeout === undefined ? this.defaultTimeout : timeout;
    const attempts = discoveryAttempts === undefined ? this.defaultDiscoveryAttempts : discoveryAttempts;

    let lastErr = null;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        return await this._request(service, args, kwargs, to); // eslint-disable-line no-await-in-loop
      } catch (e) {
        if (e instanceof QueueEmpty) { lastErr = e; continue; }
        throw e;
      }
    }
    throw lastErr || new QueueEmpty();
  }

  async _request(service, args, kwargs, timeout) {
    const requestId = `${this.idPrefix}${uuid4()}`;
    const discoveryQueue = `${requestId}:discovery`;
    const requestQueue = `${requestId}:request`;
    this.queues.set(discoveryQueue, new AsyncQueue());
    this.queues.set(requestQueue, new AsyncQueue());

    try {
      await this.eventNode.emit("service_discovery", { service, reply_to: discoveryQueue });
      for (;;) {
        const provider = await this.queues.get(discoveryQueue).get(timeout); // throws QueueEmpty on discovery timeout
        await this.eventNode.emit("service_request", {
          target: provider.ident,
          service,
          args,
          kwargs,
          reply_to: requestQueue,
        });
        let response;
        try {
          response = await this.queues.get(requestQueue).get(timeout);
        } catch (e) {
          if (e instanceof QueueEmpty) continue; // response timeout -> try next provider
          throw e;
        }
        if (response && Object.prototype.hasOwnProperty.call(response, "raise")) {
          throw pyToError(response.raise);
        }
        return response ? response.return : null;
      }
    } finally {
      this.queues.delete(requestQueue);
      this.queues.delete(discoveryQueue);
    }
  }
}

// ---------------------------------------------------------------------------
// RpcNode — thin sugar over ServiceNode with a Proxy-based call interface:
//     await rpc.call.some_service(1, 2, {kw: 3})   // last plain-object => kwargs
//     await rpc.callService("some_service", [1, 2], {kw: 3})
// ---------------------------------------------------------------------------

export class RpcNode {
  constructor(eventNode, { idPrefix = "", proxyTimeout = null } = {}) {
    this.eventNode = eventNode;
    this.serviceNode = new ServiceNode(eventNode, { idPrefix, defaultTimeout: proxyTimeout });
    this.eventNodeWasStarted = false;
    this.started = false;

    this.call = new Proxy({}, {
      get: (_t, name) => (...callArgs) => {
        let kwargs = {};
        let args = callArgs;
        const last = callArgs[callArgs.length - 1];
        if (last && typeof last === "object" && !Array.isArray(last)
            && !(last instanceof Uint8Array) && !(last instanceof PyFloat) && !(last instanceof Map)) {
          kwargs = last;
          args = callArgs.slice(0, -1);
        }
        return this.serviceNode.request(name, { args, kwargs });
      },
    });
  }

  start() {
    if (this.started) return;
    if (!this.eventNode.started) { this.eventNode.start(); this.eventNodeWasStarted = true; }
    this.serviceNode.start();
    this.started = true;
  }

  stop() {
    this.serviceNode.stop();
    if (this.eventNodeWasStarted) this.eventNode.stop();
    this.started = false;
  }

  callService(service, args = [], kwargs = {}, opts = {}) {
    if (!this.started) throw new Error("RpcNode is not started");
    return this.serviceNode.request(service, { args, kwargs, ...opts });
  }

  callWithTimeout(service, timeout, args = [], kwargs = {}) {
    return this.callService(service, args, kwargs, { timeout });
  }
}

// ---------------------------------------------------------------------------
// Usage (browser, socket.io already connected as window.socket):
//
//   import { EventNode, RpcNode } from "./arbiter.js";
//
//   const node = new EventNode(window.socket, { room: "events", password: "" });
//   const rpc  = new RpcNode(node, { proxyTimeout: 30000 });
//   rpc.start();
//
//   const result = await rpc.call.simple_add(1, 2);            // positional
//   const r2     = await rpc.call.some_service(1, { flag: true }); // + kwargs
//   const r3     = await rpc.callWithTimeout("slow_service", 60000, ["x"]);
// ---------------------------------------------------------------------------
