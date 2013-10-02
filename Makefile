LUA_CFLAGS = `pkg-config lua5.2 --cflags`

SRC = lua_tarantool.c \
	  lua_tnt_helper.c \
	  lua_tnt_requestbuilder.c \
	  lua_tnt_responseparser.c 

all: lib luasocket test

luasocket:
	make -C luasocket all
	cp -f luasocket/socket.so* ./socket.so

lib: 
	gcc -shared -fpic -o lua_tarantool.so -I. $(LUA_CFLAGS) ${SRC}

test:
	lua test.lua

.PHONY: luasocket
