-- DEBUG
local yaml = require("yaml")
--------
local pack = require("pack")
local tarantool = {}

local function checkt(var, types)
    if type(types) == 'string' then
       types = {types}
    end
    for i, j in pairs(types) do
        if type(var) == j then
            return true
        end
    end
    return false
end

local function checkte(var, types, nvar, nfunc)
    if type(types) == 'string' then
       types = {types}
    end
    for i, j in pairs(types) do
        if type(var) == j then
            return true
        end
    end
    error(
        string.format("%s type error: %s must be one of {%s}, but not %s",
            nfunc, nvar, table.concat(types, ", "), type(var)), 3)
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

    error = function(msg, level)
        error(msg, (level or 1) + 1)
    end,

    pack = function (self, schema, tuple)
        local new_tuple = {}
        local first = 1
        if schema ~= nil then
            first = math.min(#schema, #tuple)
            local val = nil
            for i = 1, first do
                if schema[i] == 'number32' then
                    if checkt(tuple[i], 'number') then
                        val = self.pack_int32(tuple[i])
                    elseif tonumber(tuple[i]) ~= nil then
                        val = self.pack_int32(tonumber(tuple[i]))
                    else
                        self.error(string.format('Schema error: type in schema is number32, but real is %s', type(tuple[i])))
                    end
                elseif schema[i] == 'number64' then
                    if checkt(tuple[i], 'number') then
                        val = self.pack_int64(tuple[i])
                    elseif tonumber(tuple[i]) ~= nil then
                        val = self.pack_int64(tonumber(tuple[i]))
                    else
                        self.error(string.format('Schema error: type in schema is number64, but real is %s', type(tuple[i])))
                    end
                elseif schema[i] == 'string' then
                    if checkt(tuple[i], 'string') then
                        val = tuple[i]
                    else
                        self.error(string.format('Schema error: type in schema is string, but real is %s', type(tuple[i])))
                    end
                end
                table.insert(new_tuple, val)
            end
            first = first + 1
        end
        for i = first, #tuple do
            if type(tuple[i]) == 'number' then
                val = self.pack_int32(args[i])
            elseif type(tuple[i]) == 'string' then
                val = tuple[i]
            else
                self.error('error 2') --TODO: name error
            end
            table.insert(new_tuple, val)
        end
        return new_tuple
    end,

    unpack = function (self, schema, tuple)
        local new_tuple = {}
        local first = 1
        if schema ~= nil then
            first = math.min(#schema, #tuple)
            local val = nil
            for i = 1, first do
                if schema[i] == 'number32' and #tuple[i] == 4 then
                    val = self.unpack_int32(tuple[i])
                elseif schema[i] == 'number64' and #tuple[i] == 8 then
                    val = self.unpack_int32(tuple[i])
                elseif schema[i] == 'string' then
                    val = tuple[i]
                else
                    self.error('error') --TODO: name error
                end
                table.insert(new_tuple, val)
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
            table.insert(new_tuple, val)
        end
        return new_tuple
    end,

    pack_space = function (self, space, args)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.fields end
        return self:pack(space_schema, args)
    end,
    pack_space_closure = function(self, space)
        return function(key)
            return self:pack_space(space, key)
        end
    end,
    unpack_space = function (self, space, args)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.fields end
        return self:unpack(space_schema, args)
    end,
    unpack_space_closure = function(self, space)
        return function(key)
            return self:unpack_space(space, key)
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
    unpack_func_closure = function(self, func)
        return function(args)
            return self:unpack_func(func, args)
        end
    end,
    pack_key = function (self, space, index, key)
        local space_schema = self._schema.spaces[space]
        if space_schema ~= nil then space_schema = space_schema.indexes[index] end
        return self:pack(space_schema, key)
    end,
    pack_key_closure = function (self, space, index)
        return function(key)
            return self:pack_key(space, index, key)
        end
    end,

    set = function(self, schema) -- TODO: refactor, maybe
        checkte(schema, 'table', 'schema', 'Schema.set')
        if schema.spaces == nil then schema.spaces = {} end
        checkte(schema.spaces, 'table', 'schema.spaces', 'Schema.set')
        for _, v in pairs(schema.spaces) do
            checkte(v, 'table', 'item of schema.spaces', 'Schema.set')
            if checkt(v.fields, 'nil') then v.fields = {} end
            if checkt(v.indexes, 'nil') then v.indexes = {} end
            checkte(v.fields, 'table', 'item of schema.spaces.fields', 'Schema.set')
            checkte(v.indexes, 'table', 'item of schema.spaces.indexes', 'Schema.set')
            for _, v1 in pairs(v.fields) do
                if v1 ~= 'string' and v1 ~= 'number32' and v1 ~= 'number64' then
                    self.error('error') --TODO: name error
                end
            end
            for k1, v1 in pairs(v.indexes) do
                for k2, v2 in pairs(v1) do
                    if v.fields[v2 + 1] == nil then
                        self.error('error') --TODO: name error
                    else
                        v1[k2] = v.fields[v2 + 1]
                    end
                end
            end
        end
        if schema.funcs == nil then schema.funcs = {} end
        checkte(schema.funcs, 'table', 'schema.funcs', 'Schema.set')
        for _, v in pairs(schema.funcs) do
            checkte(v, 'table', 'item of schema.funcs', 'Schema.set')
            for _, v1 in pairs({'in', 'out'}) do
                if v[v1] == nil then v[v1] = {} end
                checkte(v[v1], 'table', v1..' of schema.funcs', 'Schema.set')
                for _, v2 in pairs(v[v1]) do
                    if v2 ~= 'string' and v2 ~= 'number32' and v2 ~= 'number64' then
                        self.error('error') --TODO: name error
                    end
                end
            end 
        end
        self._schema = schema
    end
}

Schema.__index = Schema
Schema.new = function (schema)
    local self = {}
    setmetatable(self, Schema)
    self:set(schema)
    return self
end

setmetatable(Schema, {
    __call = function (cls, ...)
        return cls.new(...)
    end,
})

return Schema
