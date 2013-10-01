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
#include <include/lua_tnt_helper.h>


inline const char *
ltnt_checkstring(lua_State *L, int narg, size_t *len) {
	if (!lua_isstring(L, narg))
		luaL_error(L, "RequesBuilder: Incorrect method call");
	return lua_tolstring(L, narg, len);
}

inline struct tp **
ltnt_checkrequestbuilder(lua_State *L, int narg) {
	return (struct tp **) luaL_checkudata(L, narg, "tarantool.RequestParser");
}
inline struct tp **
ltnt_checkrequestbuilder(lua_State *L, int narg) {
	return (struct tp **) luaL_checkudata(L, narg, "tarantool.RequestBuilder");
}

int ltnt_pushtuple(lua_State *L, struct tp **iproto, int narg) {
	if (narg < 0)
		narg = lua_gettop(L) + narg + 1;
	lua_pushnil(L);
	while(lua_next(L, narg) != 0) {
		size_t len = 0;
		const void *str = ltnt_checkstring(L, -1, &len);
		tp_field(*iproto, (const void *)str, len);
		lua_pop(L, 1);
	}
}

inline void ltnt_pushsntable(lua_State *L, int narg, const char *str, int num) {
	if (narg < 0)
		narg = lua_gettop(L) + narg + 1;
	lua_pushstring(L, str);
	lua_pushnumber(L, num);
	lua_settable(L, narg);
}

inline void
ltnt_pushnstable(lua_State *L, int narg, int num,
			const char *str, ssize_t len) {
	if (narg < 0)
		narg = lua_gettop(L) + narg + 1;
	lua_pushnumber(L, num);
	lua_pushlstring(L, str, len);
	lua_settable(L, narg);
}
