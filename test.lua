tarantool = require 'lua_tarantool'
print("---------------- MODULE -----------------")
print(tarantool)
for key,value in pairs(tarantool) do 
    print(key,value) 
end
print("--------------- USERDATA ----------------")
print("RB func : ", tarantool.request_builder_new)
local rb = tarantool.request_builder_new()
print("RB insta : ", rb)
print("RP func : ", tarantool.response_parser_new)
local rb = tarantool.response_parser_new()
print("RP insta : ", rb)

function HexDumpString(str, spacer)
    return (
        string.gsub(str,"(.)",
            function (c)
                return string.format("%02X%s",string.byte(c), spacer or "")
            end)
    )
end

print("---------- REQUEST TESTS ----------------")
local rb = tarantool.request_builder_new()

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

local socket = require "luasocket.socket"

rb:insert(0, 0, 0x01, {"hell", "world"})
local sock = socket.tcp()
sock:connect('localhost', 33013)
sock:send(rb:getvalue())
local a = sock:receive("12")
local b = sock:receive(tostring(tarantool.get_body_len(a)))
local b = a .. b 
local rp = tarantool.response_parser_new()
ans = rp:parse(b)

for key,value in pairs(ans) do 
    print(key,value) 
end
if ans.error ~= nil then
    for key,value in pairs(ans.error) do 
        print(key,value) 
    end
end
if ans.tuples ~= nil then
    for key,value in pairs(ans.tuples) do 
        print(key .. ": ")
        for key1,value1 in pairs(ans.tuples[key]) do 
            print(key1,value1) 
        end 
    end
end
