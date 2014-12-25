lua-nginx-tarantool
===================

Driver for tarantool 1.6 on nginx cosockets

Introduction
------------

A driver for a NoSQL database in a Lua script [Tarantool](http://tarantool.org/).

Requires [lua-MessagePack](https://github.com/fperrad/lua-MessagePack).

Synopsis
========

```lua

local tarantool = require("tarantool")

local host    = '127.0.0.1'
local port    = 3301
local spaceno = 1
local indexno = 0
local key     = { 1 }
local opts    = { }

local result, err = tarantool.select(host, port, spaceno, indexno, key, opts)

```
