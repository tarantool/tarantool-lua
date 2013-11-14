package = "tarantool"
version = "0.0.1-1"

source = {
    url = "git://github.com/bigbes92/tarantool-lua.git",
    tag = "v0.0.1"
}

description = {
    summary = "A library to work with Tarantool In-Memory DB",
    detailed = [[
        The library currently allows user to INSERT, DELETE, SELECT, UPDATE
        and CALL stored procedures for Tarantool.
    ]],
    homepage = "http://bigbes92.github.io/tarantool-lua",
    maintainer = "Eugine Blikh <bigbes@gmail.com>",
    license = "MIT",
}

dependencies = {
    "lua >= 5.1",
    "luasocket"
}

build = {
    type = "builtin",
    modules = {
        ["tnt"] = {
            sources = {
                "src/tnt.c",
                "src/tnt_helper.c",
                "src/tnt_requestbuilder.c",
                "src/tnt_responseparser.c"
            },
            incdirs = {
                "include/",
                "./"
            }
        },
        ["tarantool"] = 'src/tarantool.lua',
        ["tnt_schema"] = 'src/tnt_schema.lua',
        ["tnt_helpers"] = 'src/tnt_helpers.lua',
        ["tnt_pack"] = {
            sources = {
                "3rdparty/pack/pack.c"
            },
        },
    },
}
