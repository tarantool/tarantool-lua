function HexDump(str, spacer)
    return (
        string.gsub(str,"(.)",
            function (c)
                return string.format("%02X%s",string.byte(c), spacer or "")
            end
        )
    )
end


local tarantool = {
    _VERSION     = "tarantool-lua 0.0.1-dev",
    _DESCRIPTION = "Lua library for the tarantool k-v storage system",
}

local tnt  = require("tnt")
local yaml = require("yaml")

local default = {
    host    = "127.0.0.1",
    port    = 33013,
    timeout = 15
}

function tarantool.error(msg, level)
    error(msg, (level or 1) + 1)
end



local function create_connection(host, port, timeout)
    if host == nil then host = defaults.host end
    if port == nil then port = default.port end
    if timeout == nil then timeout = default.timeout end
    socket = require("socket").tcp()
    socket:settimeout(timeout)
    st, err = socket:connect(host, port)
    if st == nil then
        tarantool.error("LuaSocket: "..err, 5)
    end
    socket:setoption('tcp-nodelay', true)
    return socket
end

local function send_message(socket, rb)
    stat, pack = rb:getvalue()
    if stat == false then
        tarantool.error("RequestBuilder: "..pack, 4)
    end
    st, err = socket:send(rb:getvalue())
    if st == nil then
        tarantool.error("LuaSocket: "..err, 5)
    end
end

local function recv_message(socket, rp)
    local a, err = socket:receive('12')
    if a == nil then
        tarantool.error("LuaSocket: "..err, 5)
    end
    local b, err = socket:receive(tostring(tnt.get_body_len(a)))
    if b == nil then
        tarantool.error("LuaSocket: "..err, 5)
    end
    local stat, pack = rp:parse(a..b)
    if stat == false then
        tarantool.error("ResponseParser: "..pack, 4)
    end
    if pack.reply_code == 2 then
        tarantool.error(
            string.format(
                "TarantoolError: %d - %s",
                pack.error.errcode,
                pack.errcode.errstring
            ),
            6
        )
    end
    if pack.reply_code == 1 then
        return false, string.format(
                        "TarantoolError, retry: %d - %s",
                        pack.error.errcode,
                        pack.errcode.errstring
                    )
    end
    return true, yaml.dump(pack.tuples)
end

local function recv_message_2(socket, rp)
    ans, toread, header = '', 12, true
    while toread ~= 0 do
        st, err = socket:receive(tostring(toread))
        if st == nil then
            tarantool.error("LuaSocket: "..err, 5)
        end
        toread = toread - #st
        ans = ans..st
        if toread == 0 and header == true then
            header = false
            toread = tnt.get_body_len(st)
        end
    end
    stat, pack = rp:parse(a..b)
    if stat == false then
        tarantool.error("ResponseParser: "..pack, 4)
    end
    if pack.reply_code == 2 then
        tarantool.error(
            string.format(
                "TarantoolError: %d - %s",
                pack.error.errcode,
                pack.errcode.errstring
            ),
            6
        )
    end
    if pack.reply_code == 1 then
        return false, string.format(
                        "TarantoolError, retry: %d - %s",
                        pack.error.errcode,
                        pack.errcode.errstring
                    )
    end
    return true, pack.tuple
end

local function checkte(var, types, nvar, nfunc)
    if type(types) == 'string' then
       types = {types}
    end
    for i, j in ipairs(types) do
        if type(var) == j then
            return true
        end
    end
    tarantool.error(
        string.format("%s type error: %s must be one of {%s}, but not %s",
            nfunc,
            nvar,
            table.concat(types, ", "),
            type(var)
        ),
        3
    )
end

local function checkt(var, types)
    if type(types) == 'string' then
       types = {types}
    end
    for i, j in ipairs(types) do
        if type(var) == j then
            return true
        end
    end
    return false
end

local function map(func, array)
  local new_array = {}
  for i,v in ipairs(array) do
    new_array[i] = func(v)
  end
  return new_array
end

local function tbl_level(element)
    if checkt(element, 'table') then
        return math.max(unpack(map(tbl_level, element))) + 1
    end
    return 0
end

----------------- MTBL ---------------------------------

local Connection = {}
Connection.__index = Connection

setmetatable(Connection, {
    __call = function (cls, ...)
        return cls.connect(...)
    end,
})
Connection.op_codes = tnt.ops

function Connection.connect(...)
    local args = {...}
    local host, port, timeout = nil, nil, nil
    if #args > 0 then
        if #args > 1 then
            if #args > 2 then
                if #args > 3 then
                    error("Too many arguments for tarantool.connect()")
                end
                timeout = tonumber(args[3])
            else
                timeout = default.timeout
            end
            port = tonumber(args[2])
        else
            port = default.port
            timeout = default.timeout
        end
        host = tostring(args[1])
    else
        host = default.host
        port = default.port
        timeout = default.timeout
    end
    local self = setmetatable({}, Connection)
    self.sock = create_connection(host, port, timeout)
    self.reqid = 1
    self.rb = tnt.request_builder_new()
    self.rp = tnt.response_parser_new()
    return self
end

function Connection._insert(self, space, flags, ...)
    local varargs = table.pack(...)
    checkte(space, 'number', 'space', 'Connection.insert')
    checkte(flags, 'number', 'flags', 'Connection.insert')

    tuple = nil
    flags = flags + tnt.flags.RETURN_TUPLE
    if varargs.n == 1 and checkt(varargs[1], 'table') then --TODO: convert numbers to binstring
        for pos = 1, select("#", ...) do
            checkte(varargs[1][pos], {'string', 'number'}, 'tuple elements' ,'Connection.delete')
        end
        tuple = varargs[1]
    else
        for pos = 1, varargs.n do
            checkte(varargs[pos], {'string', 'number'}, 'tuple elements' ,'Connection.insert')
        end
        tuple = varargs
    end
    stat, err = self.rb:insert(self.reqid, space, flags, tuple)
    if not stat then
        tarantool.error(
            string.format(
                "Insert error: %s",
                err
            ),
            4
        )
    end
    send_message(self.sock, self.rb)
    status, ans = recv_message(self.sock, self.rp)
    self.reqid = self.reqid + 1
    self.rb:flush()
    return status, ans
end

function Connection.insert(self, space, ...)
    return self:_insert(space, tnt.flags.BOX_ADD, ...)
end

function Connection.replace(self, space, ...)
    return self:_insert(space, tnt.flags.BOX_REPLACE, ...)
end

function Connection.store(self, space, ...)
    return self:_insert(space, 0x00, ...)
end

function Connection.delete(self, space, ...)
    local varargs = table.pack(...)
    checkte(space, 'number', 'space', 'Connection.delete')

    flags = tnt.flags.RETURN_TUPLE
    if varargs.n == 1 and checkt(varargs[1], 'table') then --TODO: convert numbers to binstring
        for pos = 1, select("#", ...) do
            checkte(varargs[1][pos], {'string', 'number'}, 'tuple elements' ,'Connection.delete')
        end
        pkey = varargs[1]
    else
        for pos = 1, varargs.n do
            checkte(varargs[pos], {'string', 'number'}, 'tuple elements' ,'Connection.delete')
        end
        pkey = varargs
    end
    send_message(self.sock, self.rb:delete(self.reqid, space, flags, tuple))
    status, ans = recv_message(self.sock, self.rp)
    self.reqid = self.reqid + 1
    self.rb:flush()
    return status, ans
end

function Connection.update(self)
    return nil
end

function Connection.ping(self)
    send_message(self.sock, self.rb:ping(self.reqid))
    status, ans = recv_message(self.sock, self.rp)
    self.reqid = self.reqid + 1
    self.rb:flush()
    return status
end

function Connection.select(self, space, index, keys, offset, limit) --TODO: convert numbers to binstring
    checkte(space, 'number', 'space', 'Connection.select')
    checkte(index, 'number', 'index', 'Connection.select')
    checkte(keys, {'string', 'number', 'table'}, 'KEYS', 'Connection.select')
    keys_level = tbl_level(keys)
    if keys_level > 2 then
        tarantool.error(
            string.format(
                "keys must have nesting 2 or less, but not %s",
                keys_level
            ),
            3
        )
    end
    if keys_level ~= 0 then
        if keys_level == 1 then
            keys = {keys}
        end
        for i = 1, #keys do
            checkte(keys[i], {'string', 'number', 'table'}, 'KEYS', 'Connection.select')
            if checkt(keys[i], 'table') then
                for j = 1, #keys[i] do
                    checkte(keys[i][j], {'string', 'number'}, 'KEYS', 'Connection.select')
                end
            else
                keys[i] = {keys[i]}
            end
        end
    else
        keys = {{keys}}
    end
    checkte(offset, {'number', 'nil'}, 'offset', 'Connection.select')
    if offset == nil then offset = 0 end
    checkte(limit , {'number', 'nil'}, 'limit' , 'Connection.select')
    if limit == nil then limit = 0xFFFFFFFF end
    stat, err = self.rb:select(self.reqid, space, index, offset, limit, keys) 
    if not stat then
        tarantool.error(
            string.format("Select error: %s", err),
            4
        )
    end
    send_message(self.sock, self.rb)
    status, ans = recv_message(self.sock, self.rp)
    self.reqid = self.reqid + 1
    self.rb:flush()
    return status, ans
end

function Connection.call(self, name, args)
    checkte(name, 'string', 'name', 'Connection.call')
    checkte(name, {'table', 'nil'}, 'args', 'Connection.call')
    if args == nil then args = {} end --TODO: convert numbers to binstring
    send_message(self.sock, self.rb:call(self.reqid, name, args))
    status, ans = recv_message(self.sock, self.rp)
    self.reqid = self.reqid + 1
    self.rb:flush()
    return status, ans
end

----------------- API ----------------------------------
a = Connection.connect('127.0.0.1', 33013)
print (a:store(1, 'hello', '1', '23'))
print (a:select(1, 0, {'hello'}))

--
-- [host = 'localhost'[, port = 33013[, timeout=5]]]
--

