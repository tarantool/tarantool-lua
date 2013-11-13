--------------------------------
-- Tarantool-Connector for LUA

local tarantool = {
    _VERSION     = "tarantool-lua 0.0.1-dev",
    _DESCRIPTION = "Lua library for the tarantool k-v storage system",
}

local tnt    = require("tnt")
local Schema = require("tnt_schema")
local __h    = require("tnt_helpers")
local __s    = require("socket")

local map                = __h.map
local apply              = __h.apply
local checkt             = __h.checkt
local checkte            = __h.checkte
local tbl_level          = __h.tbl_level
local repack_tuple       = __h.repack_tuple
local tbl_of_strnum_keys = __h.tbl_of_strnum_keys

local gettime            = __s.gettime
local tcp                = __s.tcp

function table.pack(...)
    return { n = select("#", ...), ... }
end
----------------- MTBL ---------------------------------

local Connection = {
    --- Function for counter ID.
    -- Increments and returns value.
    -- Uses self._req_id. 0 in the beggining
    -- @function _reqid
    -- @treturn number
    -- @return number of request id
    _reqid = function(self)
        self._req_id = self._req_id + 1
        return self._req_id
    end,
    --- Function for throwing tarantool error.
    -- @function error
    -- @param  msg   error message
    -- @param  level error level
    error = function(msg, level)
        error(msg, (level or 1) + 1)
    end,
    --- Close connection function
    -- @function close
    close = function(self)
        self._socket:close()
    end,
    ---
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
        local stat, tuples, id = false, nil, nil
        local num = 10
        while not stat and num > 0 do
            self:_send_message()
            stat, tuples, id = self:_recv_message()
            num = num - 1
        end
        self._rb:flush()
        return stat, tuples, id
    end,

    _send_message = function (self)
        local stat, err = self._sock:send(self._rb:getvalue())
        if stat == nil then
            self._rb:flush()
            self.error("LuaSocket: "..err, 5)
        end
    end,

    _recv_message = function (self)
        local a, err = self._sock:receive('12')
        if a == nil then
            self._rb:flush()
            self.error("LuaSocket: "..err, 5)
        end
        local b, err, get = "", nil, self._body_len(a)
        if get ~= 0 then
            b, err = self._sock:receive(tostring(get))
        end
        if b == nil then
            self._rb:flush()
            self.error("LuaSocket: "..err, 5)
        end
        local stat, package = self._rp:parse(a..b)
        if stat == false then
            self._rb:flush()
            self.error("ResponseParser: "..package, 4)
        end
        if package.reply_code == 2 then
            self._rb:flush()
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
        return true, package.tuples, package.request_id
    end,

    _insert = function (self, space, flags, ...)
        checkte(space, 'number', 'space', 'Connection.insert')
        checkte(flags, 'number', 'flags', 'Connection.insert')
        local flags = flags + tnt.flags.RETURN_TUPLE

        local tuple = self._schema:pack_space(space, repack_tuple(table.pack(...)))
        local stat, err = self._rb:insert(self:_reqid(), space, flags, tuple)
        if not stat then self.error(string.format("Insert error: %s", err), 4) end

        local stat, pack, id = self:_send_recv()
        if stat then pack = map(self._schema:unpack_space_closure(space), pack) end
        return stat, pack, id
    end,

    --- Insert function.
    -- If there's a tuple in the tarantool with same key - error'll be thrown.
    --
    -- @function insert
    -- @tparam space  number (integer)
    -- @param  space  space to insert tuple into
    -- @param  tuple  may be a table of fields or fields in vararg.
    --
    -- @return[1] true
    -- @return[1] table of tables of strings/numbers) tuple
    -- @return[2] false
    -- @return[2] error message
    insert = function (self, space, ...)
        return self:_insert(space, tnt.flags.BOX_ADD, ...)
    end,

    --- Replace function.
    -- If there's no tuple in the tarantool with same key - error'll be thrown.
    --
    -- @function replace
    -- @tparam space  number (integer)
    -- @param  space  space to insert tuple into
    -- @param  tuple  may be a table of fields or fields in vararg.
    --
    -- @return[1] true
    -- @return[1] table of tables of strings/numbers) tuple
    -- @return[2] false
    -- @return[2] error message
    replace = function (self, space, ...)
        return self:_insert(space, tnt.flags.BOX_REPLACE, ...)
    end,

    --- Insert function.
    -- It'll insert tuple with no matter - there's a key or not.
    --
    -- @function store
    -- @tparam space  number (integer)
    -- @param  space  space to insert tuple into
    -- @param  tuple  may be a table of fields or fields in vararg.
    --
    -- @return[1] true
    -- @return[1] table of tables of strings/numbers) tuple
    -- @return[2] false
    -- @return[2] error message
    store = function (self, space, ...)
        return self:_insert(space, 0x00, ...)
    end,

    --- Delete function.
    -- Delete tuple in tarantool with this primary key
    --
    -- @function delete
    -- @tparam  space number (integer)
    -- @param   space space to insert tuple into
    -- @param   key   may be a table of fields for key, or fields in vararg
    --
    -- @return[1] true
    -- @return[1] table of tables of strings/numbers) tuple
    -- @return[2] false
    -- @return[2] error message
    delete = function (self, space, ...)
        checkte(space, 'number', 'space', 'Connection.delete')
        local flags = tnt.flags.RETURN_TUPLE

        local tuple = self._schema:pack_key(space, 0, repack_tuple(table.pack(...)))
        local stat, err = self._rb:delete(self:_reqid(), space, flags, tuple)
        if not stat then self.error(string.format("Delete error: %s", err), 4) end

        local stat, pack = self:_send_recv()
        if stat then pack = map(self._schema:unpack_space_closure(space), pack) end
        return stat, pack
    end,

    --- Update function.
    -- Update a tuple identified by a primary key. If a key is multipart,
    -- it is passed in as a Lua table.
    -- Operation's can be:
    -- {'set'   , position, value}  - set value in positition `position` to `value`
    -- {'add'   , position, number} - add `value` to field in position `position`
    -- {'and'   , position, number} - binary and `value` to field in position `position`
    -- {'xor'   , position, number} - binary xor `value` to field in position `position`
    -- {'or'    , position, number} - binary or `value` to field in position `position`
    -- {'splice', position, from, to, insert} - cut value on position `position` from `from` and up to `to`, then insert `insert` in the middle.
    -- {'delete', position} - delete value in the position `position`.
    -- {'insert', position, value} - insert `value` before the `position`
    --
    -- @function update
    -- @tparam space number (integer)
    -- @tparam key   field or tabe of fields
    -- @tparam ops   table of table of ops
    -- @oaram  space space number
    -- @param  key   primary key to modify tuples with.
    -- @param  ops   table of operations - it's table with 1, 3 or 5 fields.
    --
    -- @return[1] true
    -- @return[1] table of tables of strings/numbers) tuple
    -- @return[2] false
    -- @return[2] error message
    update = function (self, space, key, ops)
        checkte(space, 'number', 'space', 'Connection.update')
        if tbl_level(key) == 0 then key = {key} end
        tbl_of_strnum_keys('Connection.update')(key)
        local key = self._schema:pack_key(space, 0, key)
        for k, v in pairs(ops) do
            checkte(v, 'table', 'op tuple', 'Connection.update')
            checkte(v[1], 'string', 'op type', 'Connection.update')
            checkte(v[2], 'number', 'op position', 'Connection.update')
            if self.update_ops[v[1] ] == nil then
                self.error(string.format("Update error: wrong op-n `%s`", v[0]))
            end
            if #v ~= (self.update_ops[v[1] ][2] + 1) then
                self.error(string.format("Update error: wrong number"..
                                              " of arguments in op number"..
                                              " %d: must be %d, but %d given",
                                              k, self.update_ops[v[1] ][2], k))
            end
            v[1] = self.update_ops[v[1] ][1]
            if v[1] == tnt.ops.OP_DELETE then
                v[3] = 1
            elseif v[1] == tnt.ops.OP_SPLICE then
                if not checkt(v[5], 'string') then
                    self.error(string.format("Update error: splice may"..
                                                " work only on strings,"..
                                                " but not on %d", type(v[5])))
                 end
            else v[3] = self._schema:pack_field(space, v[2], v[3]) end
        end

        local flags = tnt.flags.RETURN_TUPLE
        local stat, err = self._rb:update(self:_reqid(), space, flags, key, ops)
        if not stat then self.error(string.format("Update error: %s", err), 4) end
        local stat, pack = self:_send_recv()
        if stat then pack = map(self._schema:unpack_space_closure(space), pack) end
        return stat, pack
    end,

    --- Ping funnction.
    -- Ping Tarantool server
    --
    -- @function ping
    --
    -- @return[1] true
    -- @return[1] time in number for pinging.
    -- @return[2] false
    -- @return[2] error message
    ping = function (self)
        local stat, err = self._rb:ping(self:_reqid())
        if not stat then self.error(string.format("Ping error: %s", err), 4) end

        time = gettime()
        local stat, pack = self:_send_recv()
        return stat, gettime() - time
    end,

    --- Select function.
    -- Search for a tuple or tuples in the given space.
    --
    -- @function select
    -- @tparam                  space   number
    -- @param                   space   space number to select from
    -- @tparam                  index   number
    -- @param                   index   index number to search key in
    -- @tparam                  keys    string, number or table of strings/numbers or table of tables of strings/numbers
    -- @param                   keys    keys to select to
    -- @number[opt=0]           offset  optional offset of query.
    -- @number[opt=0xFFFFFFFF]  limit   optional limit for response
    --
    -- @return[1] true
    -- @return[1] table of tables of strings/numbers) tuple
    -- @return[2] false
    -- @return[2] error message
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

    --- Call function
    -- Call stored procedure
    --
    -- @function    call
    -- @tparam      name    string
    -- @tparam      args    table of strings/numbers or nil or unpacked values
    -- @param       name    name of stored procedure
    -- @param       args    arguments for calling stored procedure
    --
    -- @return[1]   true
    -- @return[1]   time in number for pinging.
    -- @return[2]   false
    -- @return[2]   error message
    call = function (self, name, ...)
        checkte(name, 'string', 'name', 'Connection.call')

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

--- Connect function
-- Connect to tarantool instance and create connection object.
-- @tparam  host    string
-- @param   host    string with tarantool address
-- @tparam  port    number
-- @param   port    tarantool primary port (or secondary)
-- @tparam  timeout number
-- @param   timeout connection timeout (in seconds)
-- @tparam  schema  table
-- @param   schema  schema for tarantool connection
-- @return  Connection object of tarantool
function Connection.connect(t)
    local function create_connection(host, port, timeout)
        local socket = tcp()
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
-- def_schema = {
--     default = 'string',
--     spaces = {
--         [0] = {
--             fields  = {'string', 'number32', 'number32', 'string', 'number64', 'number32'},
--             indexes = {
--                 [0] = {0},
--                 [1] = {1, 2},
--                 [2] = {3, 4, 5}
--             },
--         },
--     }
-- }
--
--
-- conn = Connection{host='127.0.0.1', port=33013, schema=def_schema}
-- ans = {conn:store(0, {'1', 2, 3, 'lol', 1111111, 111})}
-- print(yaml.dump(ans))
-- ans = {conn:store(0, {'2', 2, 3, 'lol', 1111111, 111})}
-- print(yaml.dump(ans))
-- ans = {conn:store(0, {'3', 2, 3, 'lol', 1111111, 111})}
-- print(yaml.dump(ans))
-- ans = {conn:select(0, 1, {2})}
-- print(yaml.dump(ans))
-- conn = Connection{host='127.0.0.1'}
-- ans = {conn:select(0, 0, {'2'})}
-- print(yaml.dump(ans))
-- conn = Connection{}
-- ans = {conn:select(0, 0, {'2'})}
-- print(yaml.dump(ans))

return Connection
