local tarantool = {
    _VERSION     = "tarantool-lua 0.0.1-dev",
    _DESCRIPTION = "Lua library for the tarantool k-v storage system",
}

-- DEBUG
local yaml = require("yaml")
--------

local tnt  = require("tnt")
local pack = require("pack")

local Schema = require("schema")

local default = {
    host    = "127.0.0.1",
    port    = 33013,
    timeout = 15,
    schema  = {},
}

function tarantool.error(msg, level)
    error(msg, (level or 1) + 1)
end

local function create_connection(host, port, timeout)
    if host == nil then host = defaults.host end
    if port == nil then port = default.port end
    if timeout == nil then timeout = default.timeout end
    local socket = require("socket").tcp()
    socket:settimeout(timeout)
    local stat, err = socket:connect(host, port)
    if stat == nil then
        tarantool.error("LuaSocket: "..err, 5)
    end
    socket:setoption('tcp-nodelay', true)
    return socket
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
            nfunc, nvar, table.concat(types, ", "), type(var)), 3)
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

local function apply(func, array)
    for _, v in ipairs(array) do
        func(v)
    end
end

local function map(func, array)
    local new_array = {}
    for k, v in ipairs(array) do
        new_array[k] = func(v)
    end
    return new_array
end

local function tbl_level(element)
    if checkt(element, 'table') then
        return math.max(unpack(map(tbl_level, element))) + 1
    end
    return 0
end

local function tbl_of_strnum_keys(func)
    local fun = func
    return function (tbl)
        checkte(tbl, 'table', 'keys', fun)
        apply(function(x) checkte(x, {'string', 'number'}, 'keys', fun) end, tbl)
    end
end

local function repack_tuple(varargs)
    if varargs.n == 1 and checkt(varargs[1], 'table') then
        varargs = vararags[1]
        varargs.n = #varargs
    end
    for pos = 1, varargs.n do
        checkte(varargs[pos], {'string', 'number'}, 'tuple elements' ,'Connection.delete')
    end
    return varargs
end

----------------- MTBL ---------------------------------

local Connection = {
    _reqid = function(self)
        self._req_id = self._req_id + 1
        return self._req_id
    end,

    update_ops = {
        ['=']     = {tnt.ops.OP_SET,      2},
        ['set']   = {tnt.ops.OP_SET,      2},
        ['+']     = {tnt.ops.OP_ADD,      2},
        ['add']   = {tnt.ops.OP_ADD,      2},
        ['&']     = {tnt.ops.OP_AND,      2},
        ['and']   = {tnt.ops.OP_AND,      2},
        ['^']     = {tnt.ops.OP_XOR,      2},
        ['xor']   = {tnt.ops.OP_XOR,      2},
        ['|']     = {tnt.ops.OP_OR,       2},
        ['or']    = {tnt.ops.OP_OR,       2},
        [':']     = {tnt.ops.OP_SPLICE,   4},
        ['splice']= {tnt.ops.OP_SPLICE,   4},
        ['#']     = {tnt.ops.OP_DELETE,   1},
        ['del']   = {tnt.ops.OP_DELETE,   1},
        ['delete']= {tnt.ops.OP_DELETE,   1},
        ['!']     = {tnt.ops.OP_INSERT,   2},
        ['ins']   = {tnt.ops.OP_INSERT,   2},
        ['insert']= {tnt.ops.OP_INSERT,   2},
    },

    _send_recv = function (self)
        local stat, tuples = false, nil
        local num = 10
        while not stat and num > 0 do
            self:_send_message()
            stat, tuples = self:_recv_message()
            num = num - 1
        end
        self._rb:flush()
        return stat, tuples
    end,

    _send_message = function (self)
        local stat, err = self._sock:send(self._rb:getvalue())
        if stat == nil then
            tarantool.error("LuaSocket: "..err, 5)
        end
    end,

    _recv_message = function (self)
        local a, err = self._sock:receive('12')
        if a == nil then
            tarantool.error("LuaSocket: "..err, 5)
        end
        local b, err = self._sock:receive(tostring(tnt.get_body_len(a)))
        if b == nil then
            tarantool.error("LuaSocket: "..err, 5)
        end
        local stat, package = self._rp:parse(a..b)
        if stat == false then
            tarantool.error("ResponseParser: "..package, 4)
        end
        if package.reply_code == 2 then
            tarantool.error(
                string.format(
                    "TarantoolError: %d - %s",
                    package.error.errcode,
                    package.errcode.errstring
                ),
                6
            )
        end
        if package.reply_code == 1 then
            return false, string.format(
                            "TarantoolError, retry: %d - %s",
                            package.error.errcode,
                            package.errcode.errstring
                        )
        end
        return true, package.tuples
    end,

    _insert = function (self, space, flags, ...)
        checkte(space, 'number', 'space', 'Connection.insert')
        checkte(flags, 'number', 'flags', 'Connection.insert')
        flags = flags + tnt.flags.RETURN_TUPLE

        local tuple = self._schema:pack_space(space, repack_tuple(table.pack(...)))
        local stat, err = self._rb:insert(self:_reqid(), space, flags, tuple)
        if not stat then tarantool.error(string.format("Insert error: %s", err), 4) end

        stat, pack = self:_send_recv()
        if stat then pack = map(self._shema:unpack_space_closure(arg), pack) end
        return stat, pack
    end,

    insert = function (self, space, ...)
        return self:_insert(space, tnt.flags.BOX_ADD, ...)
    end,

    replace = function (self, space, ...)
        return self:_insert(space, tnt.flags.BOX_REPLACE, ...)
    end,

    store = function (self, space, ...)
        return self:_insert(space, 0x00, ...)
    end,

    delete = function (self, space, ...)
        checkte(space, 'number', 'space', 'Connection.delete')
        flags = tnt.flags.RETURN_TUPLE

        local tuple = self._schema:pack_key(space, index, repack_tuple(table.pack(...)))
        local stat, err = self._rb:delete(self:_reqid(), space, flags, varargs)
        if not stat then tarantool.error(string.format("Delete error: %s", err), 4) end

        stat, pack = self:_send_recv()
        if stat then pack = map(self._shema:unpack_space_closure(space), pack) end
        return stat, pack
    end,

--[[--
    update = function (self, space, key, ...) --TODO: FUCKING IDIOTISM. NO ONE USE IT ANYWAY.
        checkte(space, 'number', 'space', 'Connection.select')
        local key = function ()
            local keys_level = tbl_level(key)
            if keys_level == 0 then
                checkte(keys, {'string', 'number'}, 'keys', 'Connection.select')
                keys = {{keys}}
            elseif keys_level == 1 then
                checkte(keys, 'table', 'keys', 'Connection.select')
                apply(function(x) checkte(x, {'string', 'number'}, 'keys', 'Connection.select') end, keys)
                keys = {keys}
            end
        end
        local key = key()
        local ops = function ()
            local vararg = table.pack(...)
            local vararg_lvl = tbl_level(vararg)
            if vararg.n == 1 and vararg_lvl == 3 then
                vararg = table.pack(table.unpack(vararg[1]))
                vararg_lvl = tbl_level(vararg)
            end
            if vararg_lvl ~= 2 then
                tarantool.error("connection.update Error: bad ops", 3)
            end
            for i = 1, vararg.n do
                checkte(vararg[i], {'table'}, 'operation', 'Connection.update')
                checkte(vararg[i][1], {'string'}, 'operation name', 'Connection.update')
                checkte(vararg[i][2], {'number'}, 'operation field', 'Connection.update')
                if self.update_ops[vararg[i][1] ] == nil then
                    tarantool.error(
                        string.format("Connection.update :"..
                            " Wrong op_code \'%s\'",
                            vararg[i][1]
                        ),
                        3
                    )
                end
                if self.update_ops[vararg[i][1] ][2] ~= #vararg[i] then
                    tarantool.error(
                        string.format("Connection.update :"..
                            " Bad number of arguments for OP on place"..
                            " %d - must be %d, but %d given",
                            i,
                            self.update_ops[vararg[i][1] ][2],
                            #vararg[i]
                        ),
                        3
                    )
                end
                vararg[i][1] = self.update_ops[vararg[i][1] ][1]
                if vararg[i][1] == tnt.ops.OP_DELETE then
                    vararg[i][2] = 1
                else
                    local pos = 3
                    if vararg[i][1] == tnt.ops.OP_SPLICE then
                        pos = 5
                    end
                    if checkt(vararg[i][pos], 'number') then
                        vararg[i][pos] = pack.pack_L(vararg[i][pos])
                    end
                end
            end
        end()
        
        stat, pack = self:_send_recv()
        if stat then pack = map(self._shema:unpack_space_closure(space), pack) end
        return stat, pack
    end,
--]]--

    ping = function (self)
        local stat, err = self._rb:ping(self:_reqid())
        if not stat then tarantool.error(string.format("Ping error: %s", err), 4) end

        time = socket.gettime()
        stat, pack = self:_send_recv()
        return stat, socket.gettime() - time
    end,

    select = function (self, space, index, keys, offset, limit) --TODO: convert numbers to binstring
        checkte(space, 'number', 'space', 'Connection.select')
        checkte(index, 'number', 'index', 'Connection.select')  
        checkte(offset, {'number', 'nil'}, 'offset', 'Connection.select')
        if offset == nil then offset = 0 end
        checkte(limit , {'number', 'nil'}, 'limit' , 'Connection.select')
        if limit == nil then limit = 0xFFFFFFFF end
        
        keys_level = tbl_level(keys)
        if keys_level > 2 or keys_level < 0 then
            tarantool.error(string.format("keys must have nesting 2 or less, but not %s", keys_level), 3)
        end
        if keys_level == 0 then
            checkte(keys, {'string', 'number'}, 'keys', 'Connection.select')
            keys = {{keys}}
        elseif keys_level == 1 then
            checkte(keys, 'table', 'keys', 'Connection.select')
            apply(function(x) checkte(x, {'string', 'number'}, 'keys', 'Connection.select') end, keys)
            keys = {keys}
        else
            apply(tbl_of_strnum_keys('Connection.select'), keys)
        end
        
        keys = map(self._schema:pack_key_closure(space, index), keys)
        local stat, err = self._rb:select(self:_reqid(), space, index,
                                          offset, limit, keys)
        if not stat then tarantool.error(string.format("Select error: %s", err), 4) end

        local stat, pack = self:send_recv()
        if stat then pack = map(self._shema:unpack_space_closure(space), pack) end
        return stat, pack
    end,

    call = function (self, name, ...)
        checkte(name, 'string', 'name', 'Connection.call')
        checkte(name, {'table', 'nil'}, 'args', 'Connection.call')

        local args = self._schema:pack_func(name, repack_tuple(table.pack(...)))
        local stat, err = self._rb:call(self:_reqid(), name, args)
        if not stat then tarantool.error(string.format("Call error: %s", err), 4) end

        local stat, pack = self:send_recv()
        if stat then pack = map(self._shema:unpack_func_closure(name), pack) end
        return stat, pack
    end,
}

Connection.__index = Connection
Connection.connect = function (t)
    print (t)
    setmetatable(t, {__index = default})
    local self = {}
    setmetatable(self, Connection)
    self._req_id = -1
    self._sock = create_connection(t.host, t.port, t.timeout)
    self._schema = Schema(t.schema)
    default.schema = {}
    self._rb = tnt.request_builder_new()
    self._rp = tnt.response_parser_new()
    return self
end
setmetatable(Connection, {
    __call = function (cls, ...)
        return cls.connect(...)
    end,
})
----------------- API ----------------------------------
--
--
def_schema = {
    default = 'string',
    spaces = {
        [0] = {
            fields  = {'string', 'number32'},
            indexes = {
                [0] = {0},
                [1] = {1},
            },
        },
    }
}


conn = Connection{host='127.0.0.1', port=33013, schema=def_schema}
ans = {conn:store(0, {'1', 2, '3'})}
print(yaml.dump(ans))
ans = {conn:store(0, {'2', 2, '3'})}
print(yaml.dump(ans))
ans = {conn:store(0, {'3', 1, '3'})}
print(yaml.dump(ans))
ans = {conn:select(0, 1, {2})}
print(yaml.dump(ans))
conn = Connection('127.0.0.1')
ans = {conn:select(0, 0, {'2'})}
print(yaml.dump(ans))
conn = Connection()
ans = {conn:select(0, 0, {'2'})}
print(yaml.dump(ans))
