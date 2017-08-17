-- Constants
return {
  -- common
  GREETING_SIZE          = 128,
  GREETING_SALT_OFFSET   = 64,
  GREETING_SALT_SIZE     = 44,
  HEAD_BODY_LEN_SIZE     = 5,
  REQUEST_PER_CONNECTION = 100000,
  MAX_LIMIT              = 0xFFFFFFFF,

  -- default options
  HOST           = '127.0.0.1',
  PORT           = 3301,
  USER           = false,
  PASSWORD       = '',
  SOCKET_TIMEOUT = 5000,
  CONNECT_NOW    = true,

  -- packet codes
  OK         = 0,
  SELECT     = 1,
  INSERT     = 2,
  REPLACE    = 3,
  UPDATE     = 4,
  DELETE     = 5,
  CALL       = 6,
  AUTH       = 7,
  EVAL       = 8,
  UPSERT     = 9,
  PING       = 64,
  ERROR_TYPE = 65536,

  -- packet keys
  TYPE          = 0x00,
  SYNC          = 0x01,
  SPACE_ID      = 0x10,
  INDEX_ID      = 0x11,
  LIMIT         = 0x12,
  OFFSET        = 0x13,
  ITERATOR      = 0x14,
  KEY           = 0x20,
  TUPLE         = 0x21,
  FUNCTION_NAME = 0x22,
  USER_NAME     = 0x23,
  OPS           = 0x28,
  DATA          = 0x30,
  ERROR         = 0x31,

  -- default spaces
  SPACE_SCHEMA  = 272,
  SPACE_SPACE   = 280,
  SPACE_INDEX   = 288,
  SPACE_FUNC    = 296,
  SPACE_USER    = 304,
  SPACE_PRIV    = 312,
  SPACE_CLUSTER = 320,

  VIEW_SPACE    = 281,
  VIEW_INDEX    = 289,

  -- index info
  INDEX_SPACE_PRIMARY = 0,
  INDEX_SPACE_NAME    = 2,
  INDEX_INDEX_PRIMARY = 0,
  INDEX_INDEX_NAME    = 2,
}