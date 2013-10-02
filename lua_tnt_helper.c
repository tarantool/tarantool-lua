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


void ltnt_register_enum(struct lua_State *L, int narg,
			const struct luatarantool_Enum *e,
			const char *str) {
	if (narg < 0)
		narg = lua_gettop(L) + narg + 1;
	lua_createtable(L, 0, 10);
	for (; e->name; ++e) {
		luaL_checkstack(L, 1, "enum too large");
		lua_pushinteger(L, e->val);
		lua_setfield(L, -2, e->name);
	}
	lua_setfield(L, -2, str);
}

inline const char *
ltnt_checkstring(struct lua_State *L, int narg, size_t *len) {
	if (!lua_isstring(L, narg))
		luaL_error(L, "RequesBuilder: Incorrect method call");
	return lua_tolstring(L, narg, len);
}

inline struct tp **
ltnt_checkresponseparser(struct lua_State *L, int narg) {
	return (struct tp **) luaL_checkudata(L, narg, "tarantool.ResponseParser");
}
inline struct tp **
ltnt_checkrequestbuilder(struct lua_State *L, int narg) {
	return (struct tp **) luaL_checkudata(L, narg, "tarantool.RequestBuilder");
}

void ltnt_pushtuple(struct lua_State *L, struct tp **iproto, int narg) {
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
