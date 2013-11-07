local tarantool = {
    _VERSION     = "tarantool-lua 0.0.1-dev",
    _DESCRIPTION = "Lua library for the tarantool k-v storage system",
}

-- DEBUG
local yaml   = require("yaml")
--------

local tnt    = require("tnt")
local Schema = require("schema")
local h      = require("tnthelpers")

local map                = h.map
local apply              = h.apply
local checkt             = h.checkt
local checkte            = h.checkte
local tbl_level          = h.tbl_level
local repack_tuple       = h.repack_tuple
local tbl_of_strnum_keys = h.tbl_of_strnum_keys

function table.pack(...)
    return { n = select("#", ...), ... }
end


----------------- MTBL ---------------------------------

local Connection = {
    _reqid = function(self)
        self._req_id = self._req_id + 1
        return self._req_id
    end,
    error = function(msg, level)
        error(msg, (level or 1) + 1)
    end,
    close = function(self)
        self._socket:close()
    end,
    _body_len = tnt.get_body_len,

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
            self.error("LuaSocket: "..err, 5)
        end
    end,

    _recv_message = function (self)
        local a, err = self._sock:receive('12')
        if a == nil then
            self.error("LuaSocket: "..err, 5)
        end
        local b, err = self._sock:receive(tostring(self._body_len(a)))
        if b == nil then
            self.error("LuaSocket: "..err, 5)
        end
        local stat, package = self._rp:parse(a..b)
        if stat == false then
            self.error("ResponseParser: "..package, 4)
        end
        if package.reply_code == 2 then
            self.error(
                string.format(
                    "TarantoolError: %d - %s",
                    package.error.errcode,
                    package.error.errstr
                ),
                6
            )
        end
        if package.reply_code == 1 then
            return false, string.format(
                            "TarantoolError, retry: %d - %s",
                            package.error.errcode,
                            package.error.errstr
                        )
        end
        return true, package.tuples
    end,

    _insert = function (self, space, flags, ...)
        checkte(space, 'number', 'space', 'Connection.insert')
        checkte(flags, 'number', 'flags', 'Connection.insert')
        local flags = flags + tnt.flags.RETURN_TUPLE

        local tuple = self._schema:pack_space(space, repack_tuple(table.pack(...)))
        local stat, err = self._rb:insert(self:_reqid(), space, flags, tuple)
        if not stat then self.error(string.format("Insert error: %s", err), 4) end

        local stat, pack = self:_send_recv()
        if stat then pack = map(self._schema:unpack_space_closure(space), pack) end
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
        local flags = tnt.flags.RETURN_TUPLE

        local tuple = self._schema:pack_key(space, 0, repack_tuple(table.pack(...)))
        local stat, err = self._rb:delete(self:_reqid(), space, flags, varargs)
        if not stat then self.error(string.format("Delete error: %s", err), 4) end

        local stat, pack = self:_send_recv()
        if stat then pack = map(self._schema:unpack_space_closure(space), pack) end
        return stat, pack
    end,

    update = function (self, space, key, ops)
        checkte(space, 'number', 'space', 'Connection.update')
        if tbl_level(key) == 0 then key = {key} end
        tbl_of_strnum_keys('Connection.select')(key)
        local key = self._schema:pack_key(space, 0, key)

        for k, v in pairs(ops) do
            checkte(v, 'table', 'op tuple', 'Connection.update')
            checkte(v[1], 'string', 'op type', 'Connection.update')
            checkte(v[2], 'number', 'op position', 'Connection.update')
            if update_ops[v[1] ] == nil then
                self.error(string.format("Update error: wrong op-n `%s`", v[0]))
            end
            if #v ~= (update_ops[v[1] ][2] + 1) then
                self.error(string.format("Update error: wrong number"..
                                              " of arguments in op number"..
                                              " %d: must be %d, but %d given",
                                              k, update_ops[v[1] ][2], k))
            end
            v[0] = update_ops[v[1] ][1]
            if v[1] == tnt.ops.OP_DELETE then
                table.insert(v, 1)
            elseif v[1] == tnt.ops.OP_SPLICE and not checkt(v[5], 'string') then
                self.error(string.format("Update error: splice may"..
                                              " work only on strings,"..
                                              " but not on %d", type(v[5])))
            else v[3] = self._schema:pack_field(space, v[2], v[3]) end
        end

        local flags = tnt.flags.RETURN_TUPLE
        local stat, err = self._rb:update(self:_reqid(), space, flags, key, ops)
        if not stat then self.error(string.format("Update error: %s", err), 4) end
        local stat, pack = self:_send_recv()
        if stat then pack = map(self._schema:unpack_space_closure(space), pack) end
        return stat, pack
    end,

    ping = function (self)
        local stat, err = self._rb:ping(self:_reqid())
        if not stat then self.error(string.format("Ping error: %s", err), 4) end

        time = socket.gettime()
        local stat, pack = self:_send_recv()
        return stat, socket.gettime() - time
    end,

    select = function (self, space, index, keys, offset, limit)
        checkte(space, 'number', 'space', 'Connection.select')
        checkte(index, 'number', 'index', 'Connection.select')
        checkte(offset, {'number', 'nil'}, 'offset', 'Connection.select')
        if offset == nil then offset = 0 end
        checkte(limit , {'number', 'nil'}, 'limit' , 'Connection.select')
        if limit == nil then limit = 0xFFFFFFFF end

        keys_level = tbl_level(keys)
        if keys_level > 2 or keys_level < 0 then
            self.error(string.format("keys must have nesting 2 or less, but not %s", keys_level), 3)
        end
        if keys_level == 0 then
            checkte(keys, {'string', 'number'}, 'keys', 'Connection.select')
            keys = {{keys}}
        elseif keys_level == 1 then
            tbl_of_strnum_keys('Connection.select')(keys)
            keys = {keys}
        else
            apply(tbl_of_strnum_keys('Connection.select'), keys)
        end

        keys = map(self._schema:pack_key_closure(space, index), keys)
        local stat, err = self._rb:select(self:_reqid(), space, index,
                                          offset, limit, keys)
        if not stat then self.error(string.format("Select error: %s", err), 4) end

        local stat, pack = self:_send_recv()
        if stat then pack = map(self._schema:unpack_space_closure(space), pack) end
        return stat, pack
    end,

    call = function (self, name, ...)
        checkte(name, 'string', 'name', 'Connection.call')
        checkte(name, {'table', 'nil'}, 'args', 'Connection.call')

        local args = self._schema:pack_func(name, repack_tuple(table.pack(...)))
        local stat, err = self._rb:call(self:_reqid(), name, args)
        if not stat then self.error(string.format("Call error: %s", err), 4) end

        local stat, pack = self:_send_recv()
        if stat then pack = map(self._schema:unpack_func_closure(name), pack) end
        return stat, pack
    end,
}

Connection.__index = Connection

Connection.__gc    = Connection.close

Connection.connect = function (t)
    local function create_connection(host, port, timeout)
        local socket = require("socket").tcp()
        socket:settimeout(timeout)
        local stat, err = socket:connect(host, port)
        if stat == nil then
            Connection.error("LuaSocket: "..err, 5)
        end
        socket:setoption('tcp-nodelay', true)
        return socket
    end
    local default = {
        host    = "127.0.0.1",
        port    = 33013,
        timeout = 15,
        schema  = {},
    }
    setmetatable(t, {__index = default})
    checkte(t.host,   'string', 'host field'   , 'Connection.connect')
    checkte(t.port,   'number', 'port field'   , 'Connection.connect')
    checkte(t.timeout,'number', 'timeout field', 'Connection.connect')
    checkte(t.schema, 'table' , 'schema field' , 'Connection.connect')
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
            fields  = {'string', 'number32', 'number32', 'string', 'number64', 'number32'},
            indexes = {
                [0] = {0},
                [1] = {1, 2},
                [2] = {3, 4, 5}
            },
        },
    }
}


conn = Connection{host='127.0.0.1', port=33013, schema=def_schema}
ans = {conn:store(0, {'1', 2, 3, 'lol', 1111111, 111})}
print(yaml.dump(ans))
ans = {conn:store(0, {'2', 2, 3, 'lol', 1111111, 111})}
print(yaml.dump(ans))
ans = {conn:store(0, {'3', 2, 3, 'lol', 1111111, 111})}
print(yaml.dump(ans))
ans = {conn:select(0, 1, {2})}
print(yaml.dump(ans))
conn = Connection{host='127.0.0.1'}
ans = {conn:select(0, 0, {'2'})}
print(yaml.dump(ans))
conn = Connection{}
ans = {conn:select(0, 0, {'2'})}
print(yaml.dump(ans))

return Connection
