package = "lua-tarantool"
version = "scm-1"

source = {
  url = "git://github.com/tarantool/tarantool-lua.git",
  branch = "master"
}

description = {
  summary = "Pure Lua library for querying the tarantool NoSQL database",
  homepage = "https://github.com/tarantool/tarantool-lua",
  maintainer = "Conrad Steenberg <conrad.steenberg@gmail.com>",
  license = "BSD 2-Clause"
}

dependencies = {
  "lua ~> 5.1",
  "lua-messagepack",
  "lua-resty-socket",
  "sha1"
}

build = {
  type = "builtin",
  modules = {
    ["tarantool"] = "tarantool.lua",
    ["const"] = "const.lua",
  },
  install = {
    lua = {
      ["tarantool"] = "tarantool.lua",
      ["const"] = "const.lua",
    }
  }
}

