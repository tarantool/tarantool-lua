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

local tarantool = require("tarantool")

local host    = "127.0.0.1"
local port    = 3301
local spaceno = 1
local indexno = 0
local key     = { 1 }
local tuple   = { "first field", "second field" }

local result, err = tarantool.select(host, port, spaceno, indexno, key)

local result, err = tarantool.insert(host, port, spaceno, tuple)

local result, err = tarantool.replace(host, port, spaceno, tuple)

local result, err = tarantool.delete(host, port, spaceno, key)

local result, err = tarantool.ping(host, port)

local result, err = tarantool.call(host, port, "proc_name", { "first arg" })

```
