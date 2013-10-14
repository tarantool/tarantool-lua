context(
    "RequestParser",
    function()
        before(
            function ()
                function HexDump(str, spacer)
                    return (
                        string.gsub(str,"(.)",
                            function (c)
                                return string.format("%02X%s",string.byte(c), spacer or "")
                            end
                        )
                    )
                end
                function compare(table, )
                tnt = require("tnt")
                rb = tnt.request_builder_new()
            end
        )
        after(
            function ()
                rb:flush()
            end
        )
        test(
            "insert",
            function()
                rb:insert(0, 1, 0x01, {"hello", "world"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "0D00000018000000000000000100000001000000020000000568656C6C6F05776F726C64"
                    )
                rb:flush()
                rb:insert(0, 2, 0x02, {"1", "hello", "wor"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "0D000000180000000000000002000000020000000300000001310568656C6C6F03776F72"
                    )
                rb:flush()
                rb:insert(0, 255, 0x04, {"2", "hel", "w"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "0D0000001400000000000000FF000000040000000300000001320368656C0177"
                    )
                rb:flush()
            end
        )
        test(
            "select",
            function()
            rb:select() 
            end
        )
   end
)
