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

rb:insert(123, 1, 0x01, {"hello", "world"})
print("insert: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:delete(123, 1, 0x01, {"hello"})
print("delete: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:call(123, "box.select", {"hello"})
print("call: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:ping(123)
print("ping: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()

rb:select(123, 1, 0, 10, 100, {{"hello"}, {"hello", "world"}})
print("select: ")
print(HexDumpString(rb:getvalue(), " "))
rb:flush()
