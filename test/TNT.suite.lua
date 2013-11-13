----- Default parameters of server for testing.
default = {}
default.host = '127.0.0.1'
default.port = 33013
-----
do
    tarantool = require "tarantool"
    stat, err = pcall(tarantool, {host=default.host, port=default.port})
    if stat == false then
        print("ERROR: start tarantool with ./tarantool_box in test catalog, please")
        print("ERROR is: "..err)
        os.exit(1)
    end
end

function bfr()
    ----- Default parameters of server for testing.
    default = {}
    default.host = '127.0.0.1'
    default.port = 33013
    -----
    tarantool = require("tarantool")
    yaml = require("yaml")
    schema_1 = {
        spaces = {
            [0] = {
                fields = {'string', 'string', 'string'},
                indexes = {
                    [0] = {0},
                    [1] = {1, 2},
                    [2] = {}
                }
            },
            [1] = {
                fields = {'string', 'string'},
                indexes = {
                    [0] = {0, 1},
                    [1] = {0},
                }
            },
            [2] = {
                fields = {'number32', 'number64'},
                indexes = {
                    [0] = {0, 1},
                    [1] = {1, 0}
                }
            }
        },
        funcs = {
            ['test.a'] = {
                ['in'] = {'number32', 'number32', 'string', 'string'},
                ['out'] = {'number32', 'number32', 'string', 'string'}
            },
            ['test.b'] = {
                ['in'] = {'string'},
                ['out'] = {'string'}
            }
        }
    }

    conn = tarantool{host=default.host, port=default.port, schema = schema_1, timeout=3}
    function deepcompare(t1,t2)
        local ty1, ty2 = type(t1), type(t2)
        if ty1 ~= ty2 then return false end
        if ty1 ~= 'table' then return t1 == t2 end
        for k, v1 in pairs(t1) do
            local v2 = t2[k]
            if k ~= 'n' and (v2 == nil or not deepcompare(v1, v2)) then return false end
        end
        for k, v2 in pairs(t2) do
            local v1 = t1[k]
            if k ~= 'n' and (v1 == nil or not deepcompare(v1, v2)) then return false end
        end
        return true
    end

    function test_response(table, status, answer)
        if table[1] == status and deepcompare(table[2], answer) then return true end
        return false
    end

end
context(
    "Pure Requests",
    function()
        before(bfr)
    test(
        "INSERT, REPLACE, STORE",
        function()
            list_0 = {
                {'hell1', 'mikk1', 'mouse'},
                {'hell2', 'mikki', 'mous1'},
                {'hell3', 'mikki', 'mouse'},
                {'hell4', 'mikk1', 'mous1'}
            }
            list_1 = {
                {'hell1', 'mikk1', 1},
                {'hell1', 'mikk2', 2},
                {'hell2', 'mikk1', 3},
                {'hell2', 'mikk2', 4}
            }
            list_2 = {
                {1, 68719476736},
                {1, 17179869184},
                {1, 34359738368},
                {2, 34359738368},
                {3, 8589934592 },
                {4, 17179869184}
            }
            for _, v in pairs(list_0) do assert_true(test_response({conn:store(0, v)}, true, {v})) end
            for _, v in pairs(list_1) do assert_true(test_response({conn:store(1, v)}, true, {v})) end
            for _, v in pairs(list_2) do assert_true(test_response({conn:store(2, v)}, true, {v})) end
            assert_true(test_response({conn:replace(0, list_0[1])}, true, {list_0[1]}))
            assert_true(test_response({pcall(conn.replace, conn, 0, {'hell5', 'a', 'b'})}, false,
                "TarantoolError: 49 - Tuple doesn't exist in index 0"))
            assert_true(test_response({pcall(conn.insert, conn, 0, list_0[1])}, false,
                "TarantoolError: 55 - Duplicate key exists in unique index 0"))
            assert_true(test_response({conn:insert(0, {'hell5', 'a', 'b'})}, true, {{'hell5', 'a', 'b'}}))
            assert_true(test_response({conn:replace(0, table.unpack(list_0[1]))}, true, {list_0[1]}))
        end
    )
    test(
        "SELECT",
        function()
            assert_true(test_response({conn:select(0, 0, {{}}, 0)}, true, {
                {'hell1', 'mikk1', 'mouse'},
                {'hell2', 'mikki', 'mous1'},
                {'hell3', 'mikki', 'mouse'},
                {'hell4', 'mikk1', 'mous1'},
                {'hell5', 'a', 'b'}}))
            assert_true(test_response({conn:select(0, 0, 'hell1')}, true, {
                {'hell1', 'mikk1', 'mouse'}}))
            assert_true(test_response({conn:select(0, 0, {'hell1'})}, true, {
                {'hell1', 'mikk1', 'mouse'}}))
            assert_true(test_response({conn:select(0, 1, {'mikki'})}, true, {
                {'hell2', 'mikki', 'mous1'},
                {'hell3', 'mikki', 'mouse'}}))
            assert_true(test_response({conn:select(0, 1, {'mikki', 'mous1'})}, true, {
                {'hell2', 'mikki', 'mous1'}}))
            assert_true(test_response({conn:select(0, 1, {'mikki', 'mouse'})}, true, {
                {'hell3', 'mikki', 'mouse'}}))
            assert_true(test_response({conn:select(0,1,{{'mikki','mous1'},{'mikki','mouse'}})},true,{
                {'hell2', 'mikki', 'mous1'},
                {'hell3', 'mikki', 'mouse'}}))
            assert_true(test_response({pcall(conn.select, conn, 0, 1, {{'mikki'}, 'mikk1'})}, false,
                './tnt_helpers.lua:29: Connection.select type error: keys must be one of {table}, but not string'))
            assert_true(test_response({conn:select(0, 1, {{'mikki'}, {'mikk1'}})}, true, {
                {'hell2', 'mikki', 'mous1'},
                {'hell3', 'mikki', 'mouse'},
                {'hell4', 'mikk1', 'mous1'},
                {'hell1', 'mikk1', 'mouse'}}))

            assert_true(test_response({conn:select(2, 0, 1, 0)}, true, {
                {1, 17179869184},
                {1, 34359738368},
                {1, 68719476736}}))
            assert_true(test_response({conn:select(2, 0, {1, 68719476736}, 0)}, true, {
                {1, 68719476736}}))
            assert_true(test_response({conn:select(2, 1, {34359738368}, 0)}, true, {
                {1, 34359738368},
                {2, 34359738368}}))
            assert_true(test_response({conn:select(2, 1, {34359738368, 1}, 0)}, true, {
                {1, 34359738368}}))
        end
    )
    test(
        "PING",
        function ()
            a, b = conn:ping()
            assert_true(a)
            assert_true((function() return (b < 1) end)())
        end
    )
    test(
        "DELETE",
        function ()
            list = {
                {{'hell1', 'mikk1'}, {'hell1', 'mikk1', 1}},
                {{'hell1', 'mikk2'}, {'hell1', 'mikk2', 2}},
                {{'hell2', 'mikk1'}, {'hell2', 'mikk1', 3}},
                {{'hell2', 'mikk2'}, {'hell2', 'mikk2', 4}}
            }
            for _, v in pairs(list) do assert_true(test_response({conn:delete(1, v[1])}, true, {v[2]})) end
        end
    )
    test(
        "UPDATE",
        function ()
            assert_true(test_response({conn:update(2, {1, 17179869184}, {{'+', 1, 1}, {'=', 2, 1}})}, true, {
                {1, 17179869185, 1}}))
            assert_true(test_response({conn:update(2, {1, 17179869185}, {{'#', 2}})}, true, {
                {1, 17179869185}}))
            assert_true(test_response({conn:update(0, 'hell1', {{':', 2, 2, 2, 'boy'}})}, true, {
                {'hell1', 'mikk1', 'moboye'}}))
        end
    )
    test(
        "CALL"
    )
    end
)
