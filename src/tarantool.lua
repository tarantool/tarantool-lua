local tarantool = {
    _VERSION     = "tarantool-lua 0.0.1-dev",
    _DESCRIPTION = "Lua library for the tarantool k-v storage system"
}

local tnt = require("tnt")

local defaults = {
    host    = "127.0.0.1",
    port    = 33013,
    timeout = 15
}

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
    st, err = sock:send(rb:getvalue())
    if st == nil then
        tarantool.error("LuaSocket: "..err, 5)
    end
end

local function recv_message(socket, rp)
    ans, toread, header = '', 12, true
    while toread != 0 do
        st, err = socket:recieve(tostring(toread))
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
    return ans
end

----------------- API ----------------------------------
function tarantool.error(msg, level)
    error(msg, (level or 1) + 1)
end

--
-- [host = 'localhost'[, port = 33013[, timeout=5]]]
--
function tarantool.connect(...)
    local args = {...}
    local host, port, timeout = nil, nil, nil
    if #args > 0 then
        if #args > 1 then
            if #args > 2 then
                if #args > 3 then
                    error("Too many arguments for tarantool.connect()")
                end
                timeout = tonumber(args[3]) 
            end
            port = tonumber(args[2])
        end
        host = tostring(args[1])
    end
end
