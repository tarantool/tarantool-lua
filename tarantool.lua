module("tarantool", package.seeall)

local mp     = require("MessagePack")
local C      = require("const")
local string = string
local table  = table
local ngx    = ngx
local type   = type
local ipairs = ipairs
local error  = error
local string = string
local socket
local decode_base64
local sha1_bin

-- Use non NGINX modules
-- requires: luasock (implicit), lua-resty-socket, sha1

if not ngx then
  socket = require("socket")
  socket.unix = require("socket.unix")
  local mime   = require("mime")
  decode_base64 = mime.unb64
  local sha1 = require("sha1")
  sha1_bin = sha1.binary
else
  socket = ngx.socket
  decode_base64 = ngx.decode_base64
  sha1_bin = ngx.sha1_bin
end

mp.set_integer('unsigned')

function new(self, params)
    local obj = {
        host           = C.HOST,
        port           = C.PORT,
        user           = C.USER,
        password       = C.PASSWORD,
        socket_timeout = C.SOCKET_TIMEOUT,
        connect_now    = C.CONNECT_NOW,
    }

    if params and type(params) == 'table' then
        for key, value in pairs(obj) do
            if params[key] ~= nil then
                obj[key] = params[key]
            end
        end
    end

    local sock, err = socket.tcp()
    if not sock then
        return nil, err
    end

    if obj.socket_timeout then
        sock:settimeout(obj.socket_timeout)
    end

    obj.sock     = sock
    obj._spaces  = {}
    obj._indexes = {}
    obj = setmetatable(obj, { __index = self })

    if not ngx then
      obj.unix = socket.unix()
    end

    if obj.connect_now then
        local ok, err = obj:connect()
        if not ok then
            return nil, err
        end
    end

    return obj
end

function wraperr(self, err)
    if err then
        err = err .. ', server: ' .. self.host .. ':' .. self.port
    end
    return err
end

function connect(self, host, port)
    if not self.sock then
        return nil, "no socket created"
    end

    self.host = host or self.host
    self.port = tonumber(port or self.port)

    local ok, err
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
        return ok, self:wraperr(err)
    end
    return self:_handshake()
end

function disconnect(self)
    if not self.sock then
        return nil, "no socket created"
    end
    return self.sock:close()
end

function set_keepalive(self)
    if not self.sock then
        return nil, "no socket created"
    end
    local ok, err = self.sock:setkeepalive()
    if not ok then
        self:disconnect()
        return nil, err
    end
    return ok
end

function select(self, space, index, key, opts)
    if opts == nil then
        opts = {}
    end

    local spaceno, err = self:_resolve_space(space)
    if not spaceno then
        return nil, err
    end

    local indexno, err = self:_resolve_index(spaceno, index)
    if not indexno then
        return nil, err
    end

    local body = {
        [C.SPACE_ID] = spaceno,
        [C.INDEX_ID] = indexno,
        [C.KEY]      = _prepare_key(key)
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

    local response, err = self:_request({ [ C.TYPE ] = C.SELECT }, body )
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return response.data
    end
end

function insert(self, space, tuple)
    local spaceno, err = self:_resolve_space(space)
    if not spaceno then
        return nil, err
    end

    local response, err = self:_request({ [C.TYPE] = C.INSERT }, { [C.SPACE_ID] = spaceno, [C.TUPLE] = tuple })
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return response.data
    end
end

function replace(self, space, tuple)
    local spaceno, err = self:_resolve_space(space)
    if not spaceno then
        return nil, err
    end

    local response, err = self:_request({ [C.TYPE] = C.REPLACE }, { [C.SPACE_ID] = spaceno, [C.TUPLE] = tuple })
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return response.data
    end
end

function delete(self, space, key)
    local spaceno, err = self:_resolve_space(space)
    if not spaceno then
        return nil, err
    end

    local response, err = self:_request({ [C.TYPE] = C.DELETE }, { [C.SPACE_ID] = spaceno, [C.KEY] = _prepare_key(key) })
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return response.data
    end
end

function update(self, space, index, key, oplist)
    local spaceno, err = self:_resolve_space(space)
    if not spaceno then
        return nil, err
    end

    local indexno, err = self:_resolve_index(spaceno, index)
    if not indexno then
        return nil, err
    end

    local response, err = self:_request({ [C.TYPE] = C.UPDATE }, {
            [C.SPACE_ID] = spaceno,
            [C.INDEX_ID] = indexno,
            [C.KEY]      = _prepare_key(key),
            [C.TUPLE]    = oplist,
        })
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return response.data
    end
end

function upsert(self, space, tuple, oplist)
    local spaceno, err = self:_resolve_space(space)
    if not spaceno then
        return nil, err
    end

    local response, err = self:_request({ [C.TYPE] = C.UPSERT }, {
            [C.SPACE_ID] = spaceno,
            [C.TUPLE]    = tuple,
            [C.OPS]      = oplist,
        })
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return response.data
    end
end

function ping(self)
    local response, err = self:_request({ [ C.TYPE ] = C.PING }, {} )
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return "PONG"
    end
end

function call(self, proc, args)
    local response, err = self:_request({ [ C.TYPE ] = C.CALL }, { [C.FUNCTION_NAME] = proc, [C.TUPLE] = args } )
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return response.data
    end
end

function _resolve_space(self, space)
    if type(space) == 'number' then
        return space
    elseif type(space) == 'string' then
        if self._spaces[space] then
            return self._spaces[space]
        end
    else
        return nil, 'Invalid space identificator: ' .. space
    end

    local data, err = self:select(C.SPACE_SPACE, C.INDEX_SPACE_NAME, space)
    if not data or not data[1] or not data[1][1] or err then
        return nil, (err or 'Can\'t find space with identificator: ' .. space)
    end

    self._spaces[space] = data[1][1]
    return self._spaces[space]
end

function _resolve_index(self, space, index)
    if type(index) == 'number' then
        return index
    elseif type(index) == 'string' then
        if self._indexes[index] then
            return self._indexes[index]
        end
    else
        return nil, 'Invalid index identificator: ' .. index
    end

    local spaceno, err = self:_resolve_space(space)
    if not spaceno then
        return nil, err
    end

    local data, err = self:select(C.SPACE_INDEX, C.INDEX_INDEX_NAME, { spaceno, index })
    if not data or not data[1] or not data[1][2] or err then
        return nil, (err or 'Can\'t find index with identificator: ' .. index)
    end

    self._indexes[index] = data[1][2]
    return self._indexes[index]
end

function _handshake(self)
    local greeting, greeting_err
    if not self._salt then
        greeting, greeting_err = self.sock:receive(C.GREETING_SIZE)
        if not greeting or greeting_err then
            self.sock:close()
            return nil, self:wraperr(greeting_err)
        end
        self._salt = string.sub(greeting, C.GREETING_SALT_OFFSET + 1)
        self._salt = string.sub(decode_base64(self._salt), 1, 20)
        self.authenticated = self:_authenticate()
        return self.authenticated
    end
    return true
end

function _authenticate(self)
    if not self.user then
        return true
    end

    local rbody = { [C.USER_NAME] = self.user, [C.TUPLE] = { } }

    local password = self.password or ''
    if password ~= '' then
        local step_1   = sha1_bin(self.password)
        local step_2   = sha1_bin(step_1)
        local step_3   = sha1_bin(self._salt .. step_2)
        local scramble = _xor(step_1, step_3)
        rbody[C.TUPLE] = { "chap-sha1",  scramble }
    end

    local response, err = self:_request({ [C.TYPE] = C.AUTH }, rbody)
    if err then
        return nil, err
    elseif response and response.code ~= C.OK then
        return nil, self:wraperr(response.error or "Internal error")
    else
        return true
    end
end

function _request(self, header, body)
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
    local request    = _prepare_request(header, body)
    local bytes, err = sock:send(request)
    if bytes == nil then
        sock:close()
        return nil, self:wraperr("Failed to send request: " .. err)
    end

    local size, err = sock:receive(C.HEAD_BODY_LEN_SIZE)
    if not size then
        sock:close()
        return nil, self:wraperr("Failed to get response size: " .. err)
    end

    size = mp.unpack(size)
    if not size then
        sock:close()
        return nil, self:wraperr("Client get response invalid size")
    end

    local header_and_body, err = sock:receive(size)
    if not header_and_body then
        sock:close()
        return nil, self:wraperr("Failed to get response header and body: " .. err)
    end

    local iterator = mp.unpacker(header_and_body)
    local value, res_header = iterator()
    if type(res_header) ~= 'table' then
        return nil, self:wraperr("Invalid header: " .. type(res_header) .. " (table expected)")
    end
    if res_header[C.SYNC] ~= self.sync_num then
        return nil, self:wraperr("Invalid header SYNC: request: " .. self.sync_num .. " response: " .. res_header[C.SYNC])
    end

    local value, res_body = iterator()
    if type(res_body) ~= 'table' then
        res_body = {}
    end

    return { code = res_header[C.TYPE], data = res_body[C.DATA], error = res_body[C.ERROR] }
end

function _prepare_request(h, b)
    local header = mp.pack(h)
    local body   = mp.pack(b)
    local len    = mp.pack(string.len(header) + string.len(body))
    return len .. header .. body
end

function _xor(str_a, str_b)
    function _bxor (a, b)
        local r = 0
        for i = 0, 31 do
            local x = a / 2 + b / 2
            if x ~= math.floor(x) then
                r = r + 2^i
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

function _prepare_key(value)
    if type(value) == 'table' then
        return value
    elseif value == nil then
        return { }
    else
        return { value }
    end
end
