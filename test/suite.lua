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
            "Insert",
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
--Troubles with empty tuple in tp.h
        test(
            "Select",
            function()
                rb:select(0, 1, 255, 0, 0, {{}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "11000000180000000000000001000000FF00000000000000000000000100000000000000"
                )
                rb:flush()
                rb:select(0, 2, 2, 10, 10, {{'1', 'hello'}, {'wor'}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "11000000280000000000000002000000020000000A0000000A000000020000000200000001310568656C6C6F0100000003776F72"
                )
                rb:flush()
                rb:select(0, 255, 1, 100, 100, {{'2', 'hel', 'w'}, {}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "110000002400000000000000FF000000010000006400000064000000020000000300000001320368656C017700000000"
                )
                rb:flush()
                rb:select(0, 1, 255, 150, 200, {{}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "11000000180000000000000001000000FF00000096000000C80000000100000000000000"
                )
                rb:flush()
                rb:select(0, 2, 2, 0, 100, {{'1'}, {'world', 'of'}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "11000000270000000000000002000000020000000000000064000000020000000100000001310200000005776F726C64026F66"
                )
                rb:flush()
                rb:select(0, 255, 1, 100, 0, {{1}, {2}, {3}, {'hi', 'man'}, {'good'}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "110000003A00000000000000FF0000000100000064000000000000000500000001000000013101000000013201000000013302000000026869036D616E0100000004676F6F64"
                )
                rb:flush()
            end
        )
        test(
            "Delete",
            function()
                rb:delete(0, 1, 0x01, {"hello", "world"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "1500000018000000000000000100000001000000020000000568656C6C6F05776F726C64"
                )
                rb:flush()
                rb:delete(0, 2, 0x02, {"1", "hello", "wor"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "15000000180000000000000002000000020000000300000001310568656C6C6F03776F72"
                )
                rb:flush()
                rb:delete(0, 255, 0x04, {"2", "hel", "w"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "150000001400000000000000FF000000040000000300000001320368656C0177"
                )
                rb:flush()
            end
        )
        test(
            "Update",
            function()
                rb:update(0, 1,   0x01, {"hello"}, {{1, 1, "\x02\x00\x00\x00"}, {0, 0, "\x03\x00\x00\x00"}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "130000002A000000000000000100000001000000010000000568656C6C6F020000000100000001040200000000000000000403000000"
                )
                rb:flush()
                rb:update(0, 2,   0x02, {"1"},     {{2, 3, "\x03\x00\x00\x00"}})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "130000001C0000000000000002000000020000000100000001310100000002000000030403000000"
                )
                rb:flush()
                rb:update(0, 255, 0x04, {""},      {})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "130000001100000000000000FF00000004000000010000000000000000"
                )
                rb:flush()
            end
        )
        test(
            "Call",
            function()
                rb:call(0, "mumba", {})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "160000000E0000000000000000000000056D756D626100000000"
                )
                rb:flush()
                rb:call(0, "mamba", {"1", "hello", "wor"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "160000001A0000000000000000000000056D616D62610300000001310568656C6C6F03776F72"
                )
                rb:flush()
                rb:call(0, "caramba", {"2", "hel", "w", "size", "fize", "kize", "loice"})
                assert_equal(
                    HexDump(rb:getvalue()),
                    "160000002D000000000000000000000007636172616D62610700000001320368656C01770473697A650466697A65046B697A65056C6F696365"
                )
                rb:flush()
            end

        )
        test(
            "Ping",
            function()
                rb:ping(0)
                assert_equal(
                    HexDump(rb:getvalue()),
                    "00FF00000000000000000000"
                )
                rb:flush()
            end
        )
   end
)

--  Debug helper :3
--                print("\""..HexDump(rb:getvalue()).."\"")
--                print("\"\"")
--
--  Assert body
--                assert_equal(
--                    HexDump(rb:getvalue()),
--                    ""
--                )
--                rb:flush()
