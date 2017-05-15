lua-tarantool-client
===================

Driver for tarantool 1.7 on nginx cosockets and plain lua sockets

Introduction
------------

A pure Lua driver for the NoSQL database [Tarantool](http://tarantool.org/) using fast nginx cosockets when available, or [luasocket](https://github.com/diegonehab/luasocket) as a fallback.

Requires [lua-MessagePack](https://github.com/fperrad/lua-MessagePack).

luasock
-------

For `luasock` sockets, [lua-resty-socket](https://github.com/thibaultcha/lua-resty-socket) and [sha1.lua](https://github.com/kikito/sha1.lua) are required.

These can be installed using `luarocks install lua-resty-socket` and `luarocks install sha1`


Synopsis
------------

```lua

tarantool = require("tarantool")

-- initialize connection
local tar, err = tarantool:new()

local tar, err = tarantool:new({ connect_now = false })
local ok, err = tar:connect()

local tar, err = tarantool:new({
    host           = '127.0.0.1',
    port           = 3301,
    user           = 'gg_tester',
    password       = 'pass',
    socket_timeout = 2000,
    connect_now    = true,
})

-- requests
local data, err = tar:ping()
local data, err = tar:insert('profiles', { 1, "nick 1" })
local data, err = tar:insert('profiles', { 2, "nick 2" })
local data, err = tar:select(2, 0, 3)
local data, err = tar:select('profiles', 'uid', 3)
local data, err = tar:replace('profiles', {3, "nick 33"})
local data, err = tar:delete('profiles', 3)
local data, err = tar:update('profiles', 'uid', 3, {{ '=', 1, 'nick new' }})
local data, err = tar:update('profiles', 'uid', 3, {{ '#', 1, 1 }})

-- disconnect or set_keepalive at the end
local ok, err = tar:disconnect()
local ok, err = tar:set_keepalive()

```
