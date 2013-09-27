#if defined (__cplusplus)
extern "C" {
#endif

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>


#if defined (__cplusplus)
}
#endif

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

#include <tp/tp.h>
#include <include/lua_tnt_responseparser.h>
#include <include/lua_tnt_helper.h>

int ltnt_responseparser_new(lua_State *L) {
	struct tp **iproto = lua_newuserdata(L, sizeof(struct tp*));
	*iproto = (struct tp *)malloc(sizeof(struct tp));
	luaL_getmetatable(L, "tarantool.ResponseParser");
	lua_setmetatable(L, -2);
	tp_init(*iproto, NULL, 0, tp_realloc, NULL);
	return 1;
}

