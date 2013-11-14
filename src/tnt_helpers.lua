helpers = {}

function helpers.checkte(var, types, nvar, nfunc)
    if type(types) == 'string' then
       types = {types}
    end
    for i, j in pairs(types) do
        if type(var) == j then
            return true
        end
    end
    error(string.format("%s type error: %s must be one of {%s}, but not %s",
                        nfunc, nvar, table.concat(types, ", "), type(var)), 3)
end

function helpers.checkt(var, types)
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

function helpers.apply(func, array)
    for _, v in pairs(array) do func(v) end
end

function helpers.map(func, array)
    local new_array = {}
    cur = 1
    while array[cur] ~= nil do
        table.insert(new_array, cur, func(array[cur]))
        cur = cur + 1
    end
    return new_array
end

function helpers.tbl_level(element)
    if helpers.checkt(element, 'table') then
        if #element == 0 then
            return 1
        end
        return math.max(unpack(helpers.map(helpers.tbl_level, element))) + 1
    end
    return 0
end

function helpers.tbl_of_strnum_keys(func)
    return function (tbl)
        helpers.checkte(tbl, 'table', 'keys', func)
        helpers.apply(function(x) helpers.checkte(x, {'string', 'number'}, 'keys', func) end, tbl)
    end
end

function helpers.repack_tuple(varargs)
    if varargs.n == 1 and helpers.checkt(varargs[1], 'table') then
        varargs = varargs[1]
        varargs.n = #varargs
    end
    for pos = 1, varargs.n do
        helpers.checkte(varargs[pos], {'string', 'number'}, 'tuple elements' ,'Connection.delete')
    end
    return varargs
end

return helpers
