local mp = require("MessagePack")
local C = require("const")
local string = string
local table = table
local ngx = ngx
local type = type
local ipairs = ipairs
local error = error
string = string
local socket = nil
local decode_base64 = nil
local sha1_bin = nil
local crypto = nil
local openssl_sha1_hash
openssl_sha1_hash = function(msg)
  return crypto.digest('sha1', msg, true)
end
if not ngx then
  socket = require("socket")
  socket.unix = require("socket.unix")
  local mime = require("mime")
  decode_base64 = mime.unb64
  crypto = require("crypto")
  if crypto.sha1 then
    print("This version of SHA1 is text only and is not supported")
  else
    sha1_bin = openssl_sha1_hash
  end
else
  socket = ngx.socket
  decode_base64 = ngx.decode_base64
  sha1_bin = ngx.sha1_bin
end
mp.set_integer('unsigned')
local _prepare_request
_prepare_request = function(h, b)
  local header = mp.pack(h)
  local body = mp.pack(b)
  local len = mp.pack(string.len(header) + string.len(body))
  return len .. header .. body
end
local _xor
_xor = function(str_a, str_b)
  local _bxor
  _bxor = function(a, b)
    local r = 0
    for i = 0, 31 do
      local x = a / 2 + b / 2
      if x ~= math.floor(x) then
        r = r + 2 ^ i
      end
      a = math.floor(a / 2)
      b = math.floor(b / 2)
    end
    return r
  end
  local result = ''
  if string.len(str_a) ~= string.len(str_b) then
    return 
  end
  for i = 1, string.len(str_a) do
    result = result .. string.char(_bxor(string.byte(str_a, i), string.byte(str_b, i)))
  end
  return result
end
local _prepare_key
_prepare_key = function(value)
  if type(value) == 'table' then
    return value
  elseif value == nil then
    return { }
  else
    return {
      value
    }
  end
end
local Tarantool
do
  local _class_0
  local _base_0 = {
    enable_lookups = function(self)
      self._lookup_spaces = true
      self._lookup_indexes = true
    end,
    disable_lookups = function(self)
      self._lookup_spaces = false
      self._lookup_indexes = false
      self._spaces = { }
      self._indexes = { }
    end,
    _wraperr = function(self, err)
      if err then
        return err .. ', server: ' .. self.host .. ':' .. self.port
      else
        return "Internal error"
      end
    end,
    connect = function(self, host, port)
      if not self.sock then
        return nil, "No socket created"
      end
      self.host = host or self.host
      self.port = tonumber(port or self.port)
      local ok = nil
      local err = nil
      if string.find(self.host, 'unix:/') then
        if ngx then
          ok, err = self.sock:connect(self.host)
        else
          ok, err = self.unix:connect(string.match(self.host, 'unix:(.+)'))
          if ok then
            self.sock = self.unix
          end
        end
      else
        ok, err = self.sock:connect(self.host, self.port)
      end
      if not ok then
        return ok, self:_wraperr(err)
      end
      return self:_handshake()
    end,
    disconnect = function(self)
      if not self.sock then
        return nil, "no socket created"
      end
      return self.sock:close()
    end,
    set_keepalive = function(self)
      if not self.sock then
        return nil, "no socket created"
      end
      local ok, err = self.sock:setkeepalive()
      if not ok then
        self:disconnect()
        return nil, err
      end
      return ok
    end,
    select = function(self, space, index, key, opts)
      if opts == nil then
        opts = { }
      end
      local spaceno, err = self:_resolve_space(space)
      if not spaceno then
        return nil, err
      end
      local indexno
      indexno, err = self:_resolve_index(spaceno, index)
      if not indexno then
        return nil, err
      end
      local body = {
        [C.SPACE_ID] = spaceno,
        [C.INDEX_ID] = indexno,
        [C.KEY] = _prepare_key(key)
      }
      if opts.limit ~= nil then
        body[C.LIMIT] = tonumber(opts.limit)
      else
        body[C.LIMIT] = C.MAX_LIMIT
      end
      if opts.offset ~= nil then
        body[C.OFFSET] = tonumber(opts.offset)
      else
        body[C.OFFSET] = 0
      end
      if type(opts.iterator) == 'number' then
        body[C.ITERATOR] = opts.iterator
      end
      local response
      response, err = self:_request({
        [C.TYPE] = C.SELECT
      }, body)
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return response.data
      end
    end,
    insert = function(self, space, tuple)
      local spaceno, err = self:_resolve_space(space)
      if not spaceno then
        return nil, err
      end
      local response
      response, err = self:_request({
        [C.TYPE] = C.INSERT
      }, {
        [C.SPACE_ID] = spaceno,
        [C.TUPLE] = tuple
      })
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return response.data
      end
    end,
    replace = function(self, space, tuple)
      local spaceno, err = self:_resolve_space(space)
      if not spaceno then
        return nil, err
      end
      local response
      response, err = self:_request({
        [C.TYPE] = C.REPLACE
      }, {
        [C.SPACE_ID] = spaceno,
        [C.TUPLE] = tuple
      })
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return response.data
      end
    end,
    delete = function(self, space, key)
      local spaceno, err = self:_resolve_space(space)
      if not spaceno then
        return nil, err
      end
      local response
      response, err = self:_request({
        [C.TYPE] = C.DELETE
      }, {
        [C.SPACE_ID] = spaceno,
        [C.KEY] = _prepare_key(key)
      })
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return response.data
      end
    end,
    update = function(self, index, key, oplist)
      local spaceno, err = self:_resolve_space(space)
      if not spaceno then
        return nil, err
      end
      local indexno
      indexno, err = self:_resolve_index(spaceno, index)
      if not indexno then
        return nil, err
      end
      local response
      response, err = self:_request({
        [C.TYPE] = C.UPDATE
      }, {
        [C.SPACE_ID] = spaceno,
        [C.INDEX_ID] = indexno,
        [C.KEY] = _prepare_key(key),
        [C.TUPLE] = oplist
      })
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return response.data
      end
    end,
    upsert = function(self, space, tuple, oplist)
      local spaceno, err = self:_resolve_space(space)
      if not spaceno then
        return nil, err
      end
      local response
      response, err = self:_request({
        [C.TYPE] = C.UPSERT
      }, {
        [C.SPACE_ID] = spaceno,
        [C.TUPLE] = tuple,
        [C.OPS] = oplist
      })
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return response.data
      end
    end,
    ping = function(self)
      local response, err = self:_request({
        [C.TYPE] = C.PING
      }, { })
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return "PONG"
      end
    end,
    call = function(self, proc, args)
      local response, err = self:_request({
        [C.TYPE] = C.CALL
      }, {
        [C.FUNCTION_NAME] = proc,
        [C.TUPLE] = args
      })
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return response.data[1]
      end
    end,
    _resolve_space = function(self, space)
      if type(space) == 'number' then
        return space
      elseif type(space) == 'string' then
        if self._lookup_spaces and self._spaces[space] then
          return self._spaces[space]
        end
      else
        return nil, 'Invalid space identificator: ' .. space
      end
      local data, err = self:select(C.SPACE_SPACE, C.INDEX_SPACE_NAME, space)
      if not data or not data[1] or not data[1][1] or err then
        return nil, (err or 'Can\'t find space with identifier: ' .. space)
      end
      local newspace = data[1][1]
      if self._lookup_spaces then
        self._spaces[space] = newspace
      end
      return newspace
    end,
    _resolve_index = function(self, space, index)
      if type(index) == 'number' then
        return index
      elseif type(index) == 'string' then
        if self.lookup_indexes and self._indexes[index] then
          return self._indexes[index]
        end
      else
        return nil, 'Invalid index identifier: ' .. index
      end
      local spaceno, err = self:_resolve_space(space)
      if not spaceno then
        return nil, err
      end
      local data
      data, err = self:select(C.SPACE_INDEX, C.INDEX_INDEX_NAME, {
        spaceno,
        index
      })
      if not data or not data[1] or not data[1][2] or err then
        return nil, (err or 'Can\'t find index with identifier: ' .. index)
      end
      local newindex = data[1][2]
      if self._lookup_indexes then
        self._indexes[index] = newindex
      end
      return newindex
    end,
    _handshake = function(self)
      local greeting = nil
      local greeting_err = nil
      if not self._salt then
        greeting, greeting_err = self.sock:receive(C.GREETING_SIZE)
        if not greeting or greeting_err then
          self.sock:close()
          return nil, self:_wraperr(greeting_err)
        end
        self._salt = string.sub(greeting, C.GREETING_SALT_OFFSET + 1)
        self._salt = string.sub(decode_base64(self._salt), 1, 20)
        local err
        self.authenticated, err = self:_authenticate()
        return self.authenticated, err
      end
      return true
    end,
    _authenticate = function(self)
      if not self.user then
        return true
      end
      local rbody = {
        [C.USER_NAME] = self.user,
        [C.TUPLE] = { }
      }
      local password = self.password or ''
      if password ~= '' then
        local step_1 = sha1_bin(self.password)
        local step_2 = sha1_bin(step_1)
        local step_3 = sha1_bin(self._salt .. step_2)
        local scramble = _xor(step_1, step_3)
        rbody[C.TUPLE] = {
          "chap-sha1",
          scramble
        }
      end
      local response, err = self:_request({
        [C.TYPE] = C.AUTH
      }, rbody)
      if err then
        return nil, err
      elseif response and response.code ~= C.OK then
        return nil, self:_wraperr(response.error)
      else
        return true
      end
    end,
    _request = function(self, header, body)
      local sock = self.sock
      if type(header) ~= 'table' then
        return nil, 'invlid request header'
      end
      self.sync_num = ((self.sync_num or 0) + 1) % C.REQUEST_PER_CONNECTION
      if not header[C.SYNC] then
        header[C.SYNC] = self.sync_num
      else
        self.sync_num = header[C.SYNC]
      end
      local request = _prepare_request(header, body)
      local bytes, err = sock:send(request)
      if bytes == nil then
        sock:close()
        return nil, self:_wraperr("Failed to send request: " .. err)
      end
      local size
      size, err = sock:receive(C.HEAD_BODY_LEN_SIZE)
      if not size then
        sock:close()
        return nil, self:_wraperr("Failed to get response size: " .. err)
      end
      size = mp.unpack(size)
      if not size then
        sock:close()
        return nil, self:_wraperr("Client get response invalid size")
      end
      local header_and_body
      header_and_body, err = sock:receive(size)
      if not header_and_body then
        sock:close()
        return nil, self:_wraperr("Failed to get response header and body: " .. err)
      end
      local iterator = mp.unpacker(header_and_body)
      local value, res_header = iterator()
      if type(res_header) ~= 'table' then
        return nil, self:_wraperr("Invalid header: " .. type(res_header) .. " (table expected)")
      end
      if res_header[C.SYNC] ~= self.sync_num then
        return nil, self:_wraperr("Invalid header SYNC: request: " .. self.sync_num .. " response: " .. res_header[C.SYNC])
      end
      local res_body
      value, res_body = iterator()
      if type(res_body) ~= 'table' then
        res_body = { }
      end
      return {
        code = res_header[C.TYPE],
        data = res_body[C.DATA],
        error = res_body[C.ERROR]
      }
    end
  }
  _base_0.__index = _base_0
  _class_0 = setmetatable({
    __init = function(self, params)
      self.meta = {
        host = C.HOST,
        port = C.PORT,
        user = C.USER,
        password = C.PASSWORD,
        socket_timeout = C.SOCKET_TIMEOUT,
        connect_now = C.CONNECT_NOW,
        _lookup_spaces = true,
        _lookup_indexes = true,
        _spaces = { },
        _indexes = { }
      }
      if params and type(params) == 'table' then
        for key, value in pairs(self.meta) do
          if params[key] ~= nil then
            self.meta[key] = params[key]
          end
          self[key] = self.meta[key]
        end
      end
      local sock, err = socket.tcp()
      if not sock then
        self.err = err
        return 
      end
      if self.socket_timeout then
        sock:settimeout(self.socket_timeout)
      end
      self.sock = sock
      if not ngx then
        self.unix = socket.unix()
      end
      if self.connect_now then
        local ok
        ok, err = self:connect()
        if not ok then
          print(err)
          self.err = err
        end
      end
    end,
    __base = _base_0,
    __name = "Tarantool"
  }, {
    __index = _base_0,
    __call = function(cls, ...)
      local _self_0 = setmetatable({}, _base_0)
      cls.__init(_self_0, ...)
      return _self_0
    end
  })
  _base_0.__class = _class_0
  Tarantool = _class_0
  return _class_0
end
