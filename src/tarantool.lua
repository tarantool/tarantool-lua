local tarantool = {
    _VERSION     = "tarantool-lua 0.0.1-dev",
    _DESCRIPTION = "Lua library for the tarantool k-v storage system",
}

-- DEBUG
local yaml = require("yaml")
--------

local tnt  = require("tnt")
local pack = require("pack")

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
        local varargs = table.pack(...)
        checkte(space, 'number', 'space', 'Connection.insert')
        checkte(flags, 'number', 'flags', 'Connection.insert')

        local tuple = nil
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
        local stat, err = self._rb:insert(self:_reqid(), space, flags, tuple)
        if not stat then
            tarantool.error(
                string.format("Insert error: %s", err),
                4
            )
        end
        return self:_send_recv()
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
        local stat, err = self._rb:delete(self:_reqid(), space, flags, tuple)
        if not stat then
            tarantool.error(
                string.format("Delete error: %s", err),
                4
            )
        end
        return self:_send_recv()
    end,

    update = function (self, key, ...)
        local vararg = table.pack(...)
        local vararg_lvl = tbl_leve(vararg)
        if vararg.n == 1 and vararg_lvl == 3 then
            vararg = table.pack(table.unpack(vararg[1]))
            vararg_lvl = tbl_leve(vararg)
        end
        if vararg_lvl ~= 2 then
            tarantool.error("connection.update Error: bad ops", 3)
        end
        for i = 1, vararg.n do
            checkte(vararg[i], {'table'}, 'operation', 'Connection.update')
            checkte(vararg[i][1], {'string'}, 'operation name', 'Connection.update')
            checkte(vararg[i][2], {'number'}, 'operation field', 'Connection.update')
            if self.update_ops[vararg[i][1]] == nil then
                tarantool.error(
                    string.format("Connection.update :"..
                        " Wrong op_code \'%s\'",
                        vararg[i][1]
                    ),
                    3
                )
            end
            if self.update_ops[vararg[i][1]][2] ~= #vararg[i] then
                tarantool.error(
                    string.format("Connection.update :"..
                        " Bad number of arguments for OP on place"..
                        " %d - must be %d, but %d given",
                        i,
                        self.update_ops[vararg[i][1]][2],
                        #vararg[i]
                    ),
                    3
                )
            end
            vararg[i][1] = self.update_ops[vararg[i][1]][1]
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
        local stat, err = self._rb:update(self:_reqid(), space, tnt.flags.RETURN_TUPLE, key, vararg)
        if not stat then
            tarantool.error(
                string.format("Update error: %s", err),
                4
            )
        end
        return self:_send_recv()
    end,

    ping = function (self)
        local stat, err = self._rb:ping(self:_reqid())
        if not stat then
            tarantool.error(
                string.format("Ping error: %s", err),
                4
            )
        end
        return self:_send_recv()
    end,

    select = function (self, space, index, keys, offset, limit) --TODO: convert numbers to binstring
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
        local stat, err = self._rb:select(self:_reqid(), space, index, offset, limit, keys)
        if not stat then
            tarantool.error(
                string.format("Select error: %s", err),
                4
            )
        end
        return self:_send_recv()
    end,

    call = function (self, name, args)
        checkte(name, 'string', 'name', 'Connection.call')
        checkte(name, {'table', 'nil'}, 'args', 'Connection.call')
        if args == nil then args = {} end --TODO: convert numbers to binstring
        local stat, err = self._rb:call(self:_reqid(), name, args)
        if not stat then
            tarantool.error(
                string.format("Call error: %s", err),
                4
            )
        end
        return self:send_recv()
    end,
}
Connection.__index = Connection
Connection.connect = function (...)
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
    local self = {}
    setmetatable(self, Connection)
    self._req_id = -1
    self._sock = create_connection(host, port, timeout)
    self._rb = tnt.request_builder_new()
    self._rp = tnt.response_parser_new()
    return self
end
setmetatable(Connection, {
    __call = function (cls, ...)
        return cls.connect(...)
    end,
})


--------
--  possible values: 'string', 'number32', 'number64'
--  {
--      default = 'string',
--      spaces = {
--          [0] = {
--              fields  = {'string', 'number32'},
--              indexes = {
--                  [0] = {0},
--                  [1] = {1, 2},
--              }
--          [1] = {'string', 'string', 'string'}
--      },
--      funcs = {
--          'queue.put' = {
--              from = {...},
--              to = {'string', 'number',...},
--      }
--  }
--
--  Schema.new              (schema)
--  Schema.set              (schema)
--  Schema._check           (schema)
--  Schema.build_to_space   (space, args)
--  Schema.parse_from_space (space, args)
--  Schema.build_to_func    (func,  args)
--  Schema.parse_from_func  (func,  args)
--  Schema.build_key        (space, num, key)
--------
local Schema = {
    pack_int32 = pack.pack_L,
    pack_int64 = pack.pack_Q,

    unpack_int32 = pack.unpack_L,
    unpack_int64 = pack.unpack_Q,

    pack = function (self, schema, tuple)
        local new_tuple = {}
        local first = 1
        if schema ~= nil then
            first = math.min(#schema, #tuple)
            local val = nil
            for i = 1, first do
                if schema[i] == 'number32' and checkt(tuple[i], 'number') then
                    val = self.pack_int32(tuple[i])
                elseif schema[i] == 'number64' and checkt(tuple[i], 'number') then
                    val = self.pack_int64(tuple[i])
                elseif schema[i] == 'string' and checkt(tuple[i], 'string') then
                    val = tuple[i]
                else
                    tarantool.error() --TODO: name error
                end
                new_tuple:append(val)
            end
            first = first + 1
        end
        for i = first, #tuple do
            if type(tuple[i]) == 'number' then
                val = self.pack_int32(args[i])
            elseif type(tuple[i]) == 'string' then
                val = tuple[i]
            else
                tarantool.error() --TODO: name error
            end
            new_tuple:append(val)
        end
        return new_tuple
    end,

    unpack = function (self, schema, tuple)
        local new_tuple = {}
        local first = 1
        if schema ~= nil then
            local first = math.min(#schema, #tuple)
            local val = nil
            for i = 1, first do
                if schema[i] == 'number32' and #tuple[i] == 4 then
                    val = self.unpack_int32(tuple[i])
                elseif schema[i] == 'number64' and #tuple[i] == 8 then
                    val = self.unpack_int32(tuple[i])
                elseif schema[i] == 'string' then
                    val = tuple[i]
                else
                    tarantool.error() --TODO: name error
                end
                new_tuple:append(tuple[i])
            end
            first = first + 1
        end
        for i = first, #tuple do
            local val = nil
            if #tuple[i] == 4 then
                val = self.unpack_int32(tuple[i])
            elseif #tuple[i] == 8 then
                val = self.unpack_int64(tuple[i])
            else
                val = tuple[i]
            end
            new_tuple:append(val)
        end
        return new_tuple
    end,

    build_to_space = function (self, space, args)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.fields end
        return self:pack(space_schema, args)
    end,
    parse_from_space = function (self, space, args)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.fields end
        return self:unpack(space_schema, args)
    end,
    build_to_func = function (self, func, args)
        local space_schema = self._schema.funcs[func]
        if space_schema ~= nil then space_schema = space_schema['in'] end
        return self:pack(space_schema, args)
    end,
    build_from_func = function (self, func, args)
        local space_schema = self._schema.funcs[func]
        if space_schema ~= nil then space_schema = space_schema['out'] end
        return self:unpack(space_schema, args)
    end,
    build_key = function (self, space, index, key)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.indexes[index] end
        return self:pack(space_schema, args)
    end,

    set = function(self, schema) -- TODO: refactor, maybe
        if checkte(schema, 'table', 'schema', 'Schema.set') then
            if schema.spaces == nil then
                schema.spaces = {}
            end
            if checkte(schema.spaces, 'table', 'schema.spaces', 'Schema.set') then
                for _, v in ipairs(schema.spaces) do
                    if checkte(v, 'table', 'item of schema.spaces', 'Schema.set') then
                        if checkt(v.fields, 'nil') then
                            v.fields = {}
                        end
                        checkte(v.fields, 'table', 'item of schema.spaces.fields', 'Schema.set')
                        for _, v1 in ipairs(v.fields) do
                            if v1 ~= 'string' and v1 ~= 'number32' and v1 ~= 'number64' then
                                tarantool.error('') --TODO: name error
                            end
                        end
                        if checkt(v.indexes, 'nil') then
                            v.indexes = {}
                        end
                        checkte(v.indexes, 'table', 'item of schema.spaces.indexes', 'Schema.set')
                        for _, v1 in ipairs(v.indexes) do
                            for k2, v2 in ipairs(v1) do
                                if v.fields[v2] == nil then
                                    tarantool.error('') --TODO: name error
                                else
                                    v1[k2] = v.fields[v2]
                                end
                            end
                        end
                    end
                end
            end
            if schema.funcs == nil then
                schema.funcs = {}
            end
            if checkte(schema.funcs, 'table', 'schema.funcs', 'Schema.set') then
                for _, v in ipairs(schema.funcs) do
                    if checkte(v, 'table', 'item of schema.funcs', 'Schema.set') then
                        if v['in'] == nil then
                            v['in'] = {}
                        end
                        checkte(v['in'], 'table', 'in if schema.funcs', 'Schema.set')
                        for _, v1 in ipairs(v['in']) do
                            if v1 ~= 'string' and v1 ~= 'number32' and v1 ~= 'number64' then
                                tarantool.error('') --TODO: name error
                            end
                        end
                        if v['out'] == nil then
                            v['out'] = {}
                        end
                        checkte(v['out'], 'table', 'in if schema.funcs', 'Schema.set')
                        for _, v1 in ipairs(v['out']) do
                            if v1 ~= 'string' and v1 ~= 'number32' and v1 ~= 'number64' then
                                tarantool.error('') --TODO: name error
                            end
                        end
                    end
                end
            end
        end
        self._schema = schema
    end,
}

Schema.__index = Schema
Schema.new = function (schema)
    local self = {}
    setmetatable(self, Schema)
    self.set(schema)
    return self
end

setmetatable(Schema, {
    __call = function (cls, ...)
        return cls.new(...)
    end,
})

----------------- API ----------------------------------
--
--
conn = Connection('127.0.0.1', 33013)
ans = {conn:store(0, {'1', '2', '3'})}
print(yaml.dump(ans))
ans = {conn:store(0, {'2', '2', '3'})}
print(yaml.dump(ans))
ans = {conn:store(0, {'3', '1', '3'})}
print(yaml.dump(ans))
ans = {conn:select(0, 1, {'2'})}
print(yaml.dump(ans))
conn = Connection('127.0.0.1')
ans = {conn:select(0, 0, {'2'})}
print(yaml.dump(ans))
conn = Connection()
ans = {conn:select(0, 0, {'2'})}
print(yaml.dump(ans))
