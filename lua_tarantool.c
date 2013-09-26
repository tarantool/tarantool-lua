/*
 * lua_tarantool.c
 * Bindings for tp.h
 */

#if defined (__cplusplus)
extern "C" {
#endif

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "tp/tp.h"

#if defined (__cplusplus)
}
#endif

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

#define NUM_MAX         UINT32_MAX
#define NUM64_MAX       UINT64_MAX

static inline const char *
ltnt_checkstring(lua_State *L, int narg, size_t *len) {
	if (!lua_isstring(L, narg))
		luaL_error(L, "RequesBuilder: Incorrect method call");
	return lua_tolstring(L, narg, len);
}

static inline struct tp **
ltnt_checkrequestbuilder(lua_State *L, int narg) {
	return (struct tp **) luaL_checkudata(L, narg, "tarantool.RequestBuilder");
}

static int ltnt_pushtuple(lua_State *L, struct tp **iproto, int narg) {
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

/*
 * Creating of PING request.
 * Must be called with:
 * reqid: LUA_TNUMBER
 *
 * returns LUA_TSTRING with binary packed request
 */
int ltnt_requestbuilder_ping(struct lua_State *L) {
	if (lua_gettop(L) != 2)
		luaL_error(L, "bad number of arguments (1 expected, got %d)",
				lua_gettop(L) - 1);
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	uint32_t reqid = (uint32_t )luaL_checkint(L, 2);
	tp_ping(*iproto);
	tp_reqid(*iproto, reqid);
	return 0;
}

/*
 * Creating of INSERT request.
 * Must be called with:
 * reqid: LUA_TNUMBER
 * space: LUA_TNUMBER
 * flags: LUA_TNUMBER
 * table: LUA_TTABLE as folowing:
 * { val_1, val_2, ... }
 * where val_N is converted to binary
 * string representation of value
 *
 * returns LUA_TSTRING with binary packed request
*/
int ltnt_requestbuilder_insert(struct lua_State *L) {
	if (lua_gettop(L) != 5)
		luaL_error(L, "bad number of arguments (4 expected, got %d)",
				lua_gettop(L) - 1);
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	uint32_t reqid = (uint32_t )luaL_checkint(L, 2);
	uint32_t space = (uint32_t )luaL_checkint(L, 3);
	uint32_t flags = (uint32_t )luaL_checknumber(L, 4);
	if (!lua_istable(L, 5))
		luaL_error(L, "Bad argument #4: (table expected, got %s)",
				lua_typename(L, lua_type(L, 5)));

	tp_insert(iproto[0], space, flags);
	tp_reqid(*iproto, reqid);
	tp_tuple(*iproto);
	ltnt_pushtuple(L, iproto, 5);
	return 0;
}
/*
 * Creating of SELECT request.
 * Must be called with:
 * reqid : LUA_TNUMBER
 * space : LUA_TNUMBER
 * index : LUA_TNUMBER
 * offset: LUA_TNUMBER
 * limit : LUA_TNUMBER
 * table : LUA_TTABLE as following:
 * { 0 : {key1_p1, key1_p2, ...},
 *   1 : {key2_p1, ...},
 *   ...
 *   N : {keyN_p1, ...}
 * }
 * where keyN_pM is converted to binary
 * string representation of value
 *
 * returns LUA_TSTRING with binary packed request
 */
int ltnt_requestbuilder_select(struct lua_State *L) {
	if (lua_gettop(L) != 7)
		luaL_error(L, "bad number of arguments (6 expected, got %d)",
				lua_gettop(L) - 1);
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	uint32_t reqid  = (uint32_t )luaL_checkint(L, 2);
	uint32_t space  = (uint32_t )luaL_checkint(L, 3);
	uint32_t index  = (uint32_t )luaL_checkint(L, 4);
	uint32_t offset = (uint32_t )luaL_checkint(L, 5);
	uint32_t limit  = (uint32_t )luaL_checkint(L, 6);
	if (!lua_istable(L, 7))
		luaL_error(L, "Bad argument #6: (table expected, got %s)",
				lua_typename(L, lua_type(L, 7)));

	tp_select(*iproto, space, index, offset, limit);
	tp_reqid(*iproto, reqid);
	lua_pushnil(L);
	while (lua_next(L, 7) != 0) {
		if (!lua_istable(L, -1))
			luaL_error(L, "Bad table construction: (table expected, got %s)",
					lua_typename(L, lua_type(L, -1)));
		tp_tuple(*iproto);
		ltnt_pushtuple(L, iproto, -1);
		lua_pop(L, 1);
	}
	return 0;
}

/*
 * Creating of DELETE request.
 * Must be called with:
 * reqid : LUA_TNUMBER
 * space : LUA_TNUMBER
 * flags : LUA_TNUMBER
 * tuple : LUA_TTABLE as following:
 * { key_p1, key_p2, ... }
 * where key_pM is converted to binary
 * string representation of value
 *
 * returns LUA_TSTRING with binary packed request
 */
int ltnt_requestbuilder_delete(struct lua_State *L) {
	if (lua_gettop(L) != 5)
		luaL_error(L, "bad number of arguments (4 expected, got %d)",
				lua_gettop(L) - 1);
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	uint32_t reqid = (uint32_t )luaL_checkint(L, 2);
	uint32_t space = (uint32_t )luaL_checkint(L, 3);
	uint32_t flags = (uint32_t )luaL_checknumber(L, 4);
	if (!lua_istable(L, 5))
		luaL_error(L, "Bad argument #4: (table expected, got %s)",
				lua_typename(L, lua_type(L, 5)));
	tp_delete(*iproto, space, flags);
	tp_reqid(*iproto, reqid);
	tp_tuple(*iproto);
	ltnt_pushtuple(L, iproto, 5);
	return 0;
}

/*
 * Creating of CALL request.
 * Must be called with:
 * reqid : LUA_TNUMBER
 * name  : LUA_TSTRING
 * tuple : LUA_TTABLE as following:
 * { arg_1, arg_2, ... }
 * where arg_M is converted to binary
 * string representation of value
 */
int ltnt_requestbuilder_call(struct lua_State *L) {
	if (lua_gettop(L) != 4)
		luaL_error(L, "bad number of arguments (3 expected, got %d)",
				lua_gettop(L) - 1);
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	uint32_t reqid = (uint32_t )luaL_checkint(L, 2);
	ssize_t name_size = 0;
	const char *name = ltnt_checkstring(L, 3, &name_size);
	if (!lua_istable(L, 4))
		luaL_error(L, "Bad argument #3: (table expected, got %s)",
				lua_typename(L, lua_type(L, 4)));

	tp_call(*iproto, 0, name, name_size);
	tp_reqid(*iproto, reqid);
	tp_tuple(*iproto);
	ltnt_pushtuple(L, iproto, 4);
	return 0;
}


int ltnt_requestbuilder_new(lua_State *L) {
	struct tp **iproto = lua_newuserdata(L, sizeof(struct tp*));
	*iproto = (struct tp *)malloc(sizeof(struct tp));
	luaL_getmetatable(L, "tarantool.RequestBuilder");
	lua_setmetatable(L, -2);
	tp_init(*iproto, NULL, 0, tp_realloc, NULL);
	return 1;
}


int ltnt_requestbuilder_getval(lua_State *L) {
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	lua_pushlstring(L, tp_buf(*iproto), tp_used(*iproto));
	return 1;
}

int ltnt_requestbuilder_flush(lua_State *L) {
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	tp_init(*iproto, tp_buf(*iproto), tp_size(*iproto), tp_realloc, NULL);
	return 0;
}

int ltnt_requestbuilder_gc(lua_State *L) {
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	tp_free(*iproto);
	free(*iproto);
	return 0;
}

static const struct luaL_Reg ltnt_requestbuilder[] = {
	{ "request_builder_new"	,ltnt_requestbuilder_new	},
	{ NULL			,NULL				}
};

static const struct luaL_Reg ltnt_requestbuilder_meta[] = {
	{ "ping"	,ltnt_requestbuilder_ping	},
	{ "insert"	,ltnt_requestbuilder_insert	},
	{ "select"	,ltnt_requestbuilder_select	},
	{ "delete"	,ltnt_requestbuilder_delete	},
//	{ "update"	,ltnt_requestbuilder_update	},
	{ "call"	,ltnt_requestbuilder_call	},
	{ "getvalue"	,ltnt_requestbuilder_getval	},
	{ "flush"	,ltnt_requestbuilder_flush	},
	{ "__gc"	,ltnt_requestbuilder_gc		},
	{ NULL		,NULL				}
};

/*
 * Register 'RequestBuilder' "class";
 */
int ltnt_requestbuilder_open(lua_State *L) {
	luaL_newmetatable(L, "tarantool.RequestBuilder");
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, ltnt_requestbuilder_meta, 0);
	return 1;
}

int luaopen_lua_tarantool(struct lua_State *L) {

	ltnt_requestbuilder_open(L);
	luaL_newlib(L, ltnt_requestbuilder);
	/*
	 * Register other functions
	*/
	return 1;
}

