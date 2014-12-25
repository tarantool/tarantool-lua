module("lib.tarantool", package.seeall)
local mp       = require("MessagePack")
local math     = require("math")
local string   = string
local table    = table
local ngx      = ngx
local type     = type
local ipairs   = ipairs
local assert   = assert
local error    = error
local tostring = tostring

-- constants
local REQUEST_PER_CONNECTION = 10000
local GREETING_SIZE          = 128
local LEN_HEADER_SIZE        = 5

-- packet codes
local OK         = 0
local SELECT     = 1
local INSERT     = 2
local REPLACE    = 3
local UPDATE     = 4
local DELETE     = 5
local CALL       = 6
local AUTH       = 7
local PING       = 64
local ERROR_TYPE = 65536

-- packet keys
local TYPE          = 0x00
local SYNC          = 0x01
local SPACE_ID      = 0x10
local INDEX_ID      = 0x11
local LIMIT         = 0x12
local OFFSET        = 0x13
local ITERATOR      = 0x14
local KEY           = 0x20
local TUPLE         = 0x21
local FUNCTION_NAME = 0x22
local USER          = 0x23
local DATA          = 0x30
local ERROR         = 0x31

mp.set_integer'unsigned'

function prepare_key(value)
    if type(value) == 'table' then
        return value
    elseif value == nil then
        return { }
    else
        return { value }
    end
end

function prepare_request(h, b)
    local header = mp.pack(h)
    local body   = mp.pack(b)
    local len    = mp.pack(string.len(header) + string.len(body))
    return len .. header .. body
end

function _tarantool_request(host, port, header, body)
    local sock = ngx.socket.tcp()

    local hp = "host: " .. tostring(host or "nil") .. " port: " .. tostring(port or "nil") .. " error: "
    local ok, err = sock:connect(host, port)
    if not ok then
        ngx.log(ngx.ERR, "Failed to connect to tarantool: " .. hp .. err)
        return
    end


    local count, err = sock:getreusedtimes()
    if count == 0 then
        local greeting, err = sock:receive(GREETING_SIZE)
        if not greeting or greeting_err then
            ngx.log(ngx.ERR, "Client get response size: ", greeting, " error: ", err)
            sock:close()
            return
        end
    end

    local sync_num = math.floor(math.random(REQUEST_PER_CONNECTION))
    if not header[SYNC] then
        header[SYNC] = sync_num
    else
        sync_num = header[SYNC]
    end

    local request    = prepare_request(header, body)
    local bytes, err = sock:send(request)
    if bytes == nil then
        ngx.log(ngx.ERR, "Failed to send request: " .. hp .. err)
        sock:close()
        return
    end

    local size, err = sock:receive(LEN_HEADER_SIZE)
    if not size then
        ngx.log(ngx.ERR, "Failed to get response size: ", err)
        sock:close()
        return
    end

    size = mp.unpack(size)
    if not size then
        ngx.log(ngx.ERR, "Client get response size: ", size)
        sock:close()
        return
    end

    local header_body, err = sock:receive(size)
    if not header_body then
        ngx.log(ngx.ERR, "Failed to get response header and body: ", err)
        sock:close()
        return
    end

    local ok, err = sock:setkeepalive()
    if not ok then
        sock:close()
        ngx.log(ngx.ERR, "failed to setkeepalive: " .. hp .. err)
    end

    local iterator = mp.unpacker(header_body)

    local value, res_header = iterator()
    if not res_header then
        ngx.log(ngx.ERR, "Failed to parse response header: ", res_header)
        return
    end
    if type(res_header) ~= 'table' then
        ngx.log(ngx.ERR, "Invalid header type ", type(res_header))
        return
    end
    if res_header[SYNC] ~= sync_num then
        ngx.log(ngx.ERR, "Invalid header SYNC: request: ", sync_num, " response: ", res_header[SYNC])
        return
    end

    local value, res_body = iterator()
    if header[TYPE] == PING then
        return 'PONG'
    end
    if not res_body then
        ngx.log(ngx.ERR, "Failed to parse response body: ", res_body)
        return
    end
    if type(res_body) ~= 'table' then
        ngx.log(ngx.ERR, "Invalid header type ", type(res_body))
        return
    end

    if res_header[TYPE] ~= OK then
        return nil, res_body[ERROR]
    end

    return res_body[DATA]
end

function select(host, port, spaceno, indexno, key, opts)
    if opts == nil then
        opts = {}
    end
    if spaceno == nil or type(spaceno) ~= 'number' then
        ngx.log(ngx.ERR, 'no such space #', spaceno)
    end

    if indexno == nil or type(indexno) ~= 'number' then
        ngx.log(ngx.ERR, 'no such index #', indexno)
    end

    local body = {
        [SPACE_ID] = spaceno,
        [INDEX_ID] = indexno,
        [KEY]      = prepare_key(key)
    }

    if opts.limit ~= nil then
        body[LIMIT] = tonumber(opts.limit)
    else
        body[LIMIT] = 0xFFFFFFFF
    end
    if opts.offset ~= nil then
        body[OFFSET] = tonumber(opts.offset)
    else
        body[OFFSET] = 0
    end

    -- TODO: handle iterator

    return _tarantool_request(host, port, { [ TYPE ] = SELECT }, body )
end

function insert(host, port, spaceno, tuple)
    return _tarantool_request(host, port, { [TYPE] = INSERT }, { [SPACE_ID] = spaceno, [TUPLE] = tuple })
end

function replace(host, port, spaceno, tuple)
    return _tarantool_request(host, port, { [TYPE] = REPLACE }, { [SPACE_ID] = spaceno, [TUPLE] = tuple })
end

function delete(host, port, spaceno, key)
    return _tarantool_request(host, port, { [TYPE] = DELETE }, { [SPACE_ID] = spaceno, [KEY] = prepare_key(key) })
end

function update(host, port, spaceno, key, oplist)
    -- TODO: handle oplist
    return _tarantool_request(host, port, { [TYPE] = UPDATE }, { [SPACE_ID] = spaceno, [TUPLE] = oplist, [KEY] = prepare_key(key) })
end

function ping(host, port)
    return _tarantool_request(host, port, { [ TYPE ] = PING }, {} )
end

function call(host, port, proc, args)
    return _tarantool_request(host, port, { [ TYPE ] = CALL }, { [FUNCTION_NAME] = proc, [TUPLE] = args } )
end

local class_mt = {
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}

setmetatable(_M, class_mt)
