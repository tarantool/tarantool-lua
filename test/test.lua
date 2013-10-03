local tarantool = require 'lua_tarantool'
local socket = require "socket"
local yaml = require "yaml"

print("---------------- MODULE -----------------")
io.write(yaml.dump(tarantool))
print("--------------- USERDATA ----------------")
print("RB function : ", tarantool.request_builder_new)
local rb = tarantool.request_builder_new()
print("RB instance : ", rb)
--print("RB metaclas : ", tarantool.RequestBuilder)
print("RP function : ", tarantool.response_parser_new)
local rp = tarantool.response_parser_new()
print("RP instance : ", rp)
--print("RP metaclas : ", tarantool.ResponseParser)

function HexDumpString(str, spacer)
    return (
        string.gsub(str,"(.)",
            function (c)
                return string.format("%02X%s",string.byte(c), spacer or "")
            end)
    )
end

print("---------- REQUEST TESTS ----------------")
rb:insert(0, 1, 0x01, {"hello", "world"})
print("insert test: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:delete(0, 1, 0x01, {"hello"})
print("delete: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:call(0, "box.select", {"hello"})
print("call: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:ping(0)
print("ping: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:select(0, 1, 0, 10, 100, {{"hello"}, {"hello", "world"}})
print("select: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:update(0, 1, 0x01, {"hello"}, {{0, 1, 1}, {5, 1, 2, 3, "lol"}})
print("update: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

print("-----------------------------------------")


local sock = socket.tcp()
local rp = tarantool.response_parser_new()
rb:insert(0, 0, 0x01, {"hel1", "world1"})
rb:insert(0, 0, 0x01, {"hel2", "world1"})
rb:insert(0, 0, 0x01, {"hel3", "world1"})
rb:insert(0, 0, 0x01, {"hel4", "world1"})
rb:select(0, 0, 1, 0, 1000, {{"world1"}})
rb:select(0, 0, 2, 0, 1000, {{"world1"}})
sock:connect('localhost', 33013)
sock:send(rb:getvalue())
local a = sock:receive("12")
local b = sock:receive(tostring(tarantool.get_body_len(a)))
io.write(yaml.dump(rp:parse(a .. b)))
local a = sock:receive("12")
local b = sock:receive(tostring(tarantool.get_body_len(a)))
io.write(yaml.dump(rp:parse(a .. b)))
local a = sock:receive("12")
local b = sock:receive(tostring(tarantool.get_body_len(a)))
io.write(yaml.dump(rp:parse(a .. b)))
local a = sock:receive("12")
local b = sock:receive(tostring(tarantool.get_body_len(a)))
io.write(yaml.dump(rp:parse(a .. b)))
local a = sock:receive("12")
local b = sock:receive(tostring(tarantool.get_body_len(a)))
io.write(yaml.dump(rp:parse(a .. b)))
local a = sock:receive("12")
local b = sock:receive(tostring(tarantool.get_body_len(a)))
io.write(yaml.dump(rp:parse(a .. b)))
