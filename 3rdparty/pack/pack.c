#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#if !defined(LUA_VERSION_NUM) || LUA_VERSION_NUM == 501
#	define luaL_newlib(L, num) (lua_newtable((L)),luaL_setfuncs((L), (num), 0))
	void luaL_setfuncs (lua_State *L, const luaL_Reg *l, int nup) {
		luaL_checkstack(L, nup+1, "too many upvalues");
		for (; l->name != NULL; l++) {  /* fill the table with given functions */
			int i;
			lua_pushstring(L, l->name);
			for (i = 0; i < nup; i++)  /* copy upvalues to the top */
				lua_pushvalue(L, -(nup + 1));
			lua_pushcclosure(L, l->func, nup);  /* closure with those upvalues */
			lua_settable(L, -(nup + 3)); /* table must be below the upvalues, the name and the closure */
		}
		lua_pop(L, nup);  /* remove upvalues */
	}
#endif /* !defined(LUA_VERSION_NUMBER) || LUA_VERSION_NUMBER == 501 */

static int pack_bytes(lua_State *L, uint8_t bytes) {
	uint64_t b;

	b = (uint64_t)lua_tonumber(L, -1);

	lua_pop(L, 1);

	char p[bytes + 1];
	int i;
	for (i = 0; i < bytes; ++i) {
		p[i] = b & 0xFF;
		b >>= 8;
	}
	p[bytes] = '\0';

	lua_pushlstring(L, p, bytes);
	return 1;
}

static int unpack_bytes(lua_State *L, int bytes) {
	if (!lua_isstring(L, -1))
		return 0;

	size_t length = 0;
	const char *p = lua_tolstring(L, -1, &length);
	lua_pop(L, 1);

	if (length != bytes)
		return 0;

	uint64_t b = 0;
	uint64_t r = 0;
	int i;
	for (i = 0; i < bytes; ++i) {
		r = (uint8_t)p[i];
		b |= r << (8 * i);
	}

	lua_pushnumber(L, b);
	return 1;
}

static int pack_B(lua_State *L) {
	return pack_bytes(L, 1);
}

static int unpack_B(lua_State *L) {
	return unpack_bytes(L, 1);
}

static int pack_S(lua_State *L) {
	return pack_bytes(L, 2);
}

static int unpack_S(lua_State *L) {
	return unpack_bytes(L, 2);
}

static int pack_L(lua_State *L) {
	return pack_bytes(L, 4);
}

static int unpack_L(lua_State *L) {
	return unpack_bytes(L, 4);
}

static int pack_Q(lua_State *L) {
	return pack_bytes(L, 8);
}

static int unpack_Q(lua_State *L) {
	return unpack_bytes(L, 8);
}

static const struct luaL_Reg packlib[] = {
	{"pack_B", pack_B},
	{"unpack_B", unpack_B},
	{"pack_S", pack_S},
	{"unpack_S", unpack_S},
	{"pack_L", pack_L},
	{"unpack_L", unpack_L},
	{"pack_Q", pack_Q},
	{"unpack_Q", unpack_Q},
	{NULL, NULL}
};

int luaopen_tnt_pack(lua_State *L) {
	luaL_newlib(L, packlib);
	return 1;
}
