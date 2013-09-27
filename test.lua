tarantool = require 'lua_tarantool'

print(tarantool)
for key,value in pairs(tarantool) do 
    print(key,value) 
end


function HexDumpString(str,spacer)
    return (
        string.gsub(str,"(.)",
            function (c)
                return string.format("%02X%s",string.byte(c), spacer or "")
            end)
    )
end

print(tarantool.request_builder_new)
rb = tarantool.request_builder_new()
print(rb)

rb:insert(0, 1, 0x01, {"hello", "world"})
print("insert: ")
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
