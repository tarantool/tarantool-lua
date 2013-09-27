LUA_CFLAGS = `pkg-config lua5.2 --cflags`
all: 
	gcc -shared -fpic lua_tarantool.c lua_tnt_helper.c lua_tnt_requestbuilder.c $(LUA_CFLAGS) -o lua_tarantool.so -I./
