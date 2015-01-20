lua-nginx-tarantool
===================

Driver for tarantool 1.6 on nginx cosockets

Introduction
------------

A driver for a NoSQL database in a Lua script [Tarantool](http://tarantool.org/) build on fast nginx cosockets.

Requires [lua-MessagePack](https://github.com/fperrad/lua-MessagePack).

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
