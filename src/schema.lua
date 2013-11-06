local pack = require("pack")
local tarantool = {}
function tarantool.error(msg, level)
    error(msg, (level or 1) + 1)
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
--  Schema.new          (schema)
--  Schema.set          (schema)
--  Schema._check       (schema)
--  Schema.pack_space   (space, args)
--  Schema.unpack_space (space, args)
--  Schema.pack_func    (func,  args)
--  Schema.unpack_func  (func,  args)
--  Schema.pack_key     (space, num, key)
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

    pack_space = function (self, space, args)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.fields end
        return self:pack(space_schema, args)
    end,
    pack_space_closure = function(space)
        return function(key)
            self:pack_space(space, key)
        end
    end,
    unpack_space = function (self, space, args)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.fields end
        return self:unpack(space_schema, args)
    end,
    unpack_space_closure = function(space)
        return function(key)
            self:unpack_space(space, key)
        end
    end,
    pack_func = function (self, func, args)
        local space_schema = self._schema.funcs[func]
        if space_schema ~= nil then space_schema = space_schema['in'] end
        return self:pack(space_schema, args)
    end,
    unpack_func = function (self, func, args)
        local space_schema = self._schema.funcs[func]
        if space_schema ~= nil then space_schema = space_schema['out'] end
        return self:unpack(space_schema, args)
    end,
    unpack_func_closure = function(func)
        return function(args)
            self:unpack_func(func, args)
        end
    end,
    pack_key = function (self, space, index, key)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.indexes[index] end
        return self:pack(space_schema, args)
    end,
    pack_key_closure = function (self, space, index)
        return function(key)
            self:pack_key(space, index, key)
        end
    end,

    set = function(self, schema) -- TODO: refactor, maybe
        if checkte(schema, 'table', 'schema', 'Schema.set') then
            if schema.spaces == nil then
                schema.spaces = {}
            end
            if checkte(schema.spaces, 'table', 'schema.spaces', 'Schema.set') then
                for _, v in ipairs(schema.spaces) do
                    if checkte(v, 'table', 'item of schema.spaces', 'Schema.set') then
                        if checkt(v.fields, 'nil') then v.fields = {} end
                        checkte(v.fields, 'table', 'item of schema.spaces.fields', 'Schema.set')
                        for _, v1 in ipairs(v.fields) do
                            if v1 ~= 'string' and v1 ~= 'number32' and v1 ~= 'number64' then
                                tarantool.error('') --TODO: name error
                            end
                        end
                        if checkt(v.indexes, 'nil') then v.indexes = {} end
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
            if schema.funcs == nil then schema.funcs = {} end
            if checkte(schema.funcs, 'table', 'schema.funcs', 'Schema.set') then
                for _, v in ipairs(schema.funcs) do
                    if checkte(v, 'table', 'item of schema.funcs', 'Schema.set') then
                        for _, v1 in ipairs({'in', 'out'}) do
                            if v[v1] == nil then v[v1] = {} end
                            checkte(v[v1], 'table', v1..' of schema.funcs', 'Schema.set')
                            for _, v2 in ipairs(v[v1]) do
                                if v2 ~= 'string' and v2 ~= 'number32' and v2 ~= 'number64' then
                                    tarantool.error('') --TODO: name error
                                end
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


return Schema
