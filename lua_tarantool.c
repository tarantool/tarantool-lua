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

#include <tp/tp.h>

#if defined (__cplusplus)
}
#endif

#include <inttypes.h>
#include <stdint.h>

#define NUM_MAX         UINT32_MAX
#define NUM64_MAX       UINT64_MAX

/*
 * Creating of PING request.
 * Must be called with:
 * reqid: LUA_TNUMBER
 *
 * returns LUA_TSTRING with binary packed request
 */
int ltnt_connection_ping(struct lua_State *L) {
	if (lua_gettop(L) != 1):
		luaL_error(L, "bad number of arguments (1 expected, got %d)",
				lua_gettop(L));
	struct tp tnt_iproto;
	uint32_t reqid = (uint32_t )luaL_checkint(L, 1);
	tp_init(&tnt_iproto, NULL, 0, tp_realloc, NULL);
	tp_ping(&tnt_iproto);
	tp_reqid(&tnt_iproto, reqid);
	lua_pushlstring(L, tp_buf(&tnt_iproto), tp_used(&tnt_iproto));
	tp_free(&tnt_iproto);
	return 1;
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
int ltnt_connection_insert(struct lua_State *L) {
	if (lua_gettop(L) != 4):
		luaL_error(L, "bad number of arguments (4 expected, got %d)",
				lua_gettop(L));
	struct tp tnt_iproto;
	uint32_t reqid = (uint32_t )luaL_checkint(L, 1);
	uint32_t space = (uint32_t )luaL_checkint(L, 2);
	uint32_t flags = (uint32_t )luaL_checknumber(L, 3);
	if (!lua_istable(L, 4))
		luaL_error(L, "Bad argument #4: (table expected, got %s)",
				lua_typename(L, lua_type(L, 4)));

	tp_init(&tnt_iproto, NULL, 0, tp_realloc, NULL);
	tp_insert(&tnt_iproto, space, flags);
	tp_reqid(&tnt_iproto, reqid);
	tp_tuple(&tnt_iproto);
	ptrdiff_t i = 0;
	for (;; ++i) {
		lua_pushinteger(L, i);
		lua_gettable(L, -2);
		size_t len = 0;
		if (lua_isnil(L, -1))
			break;
		const char *str = lua_tolstring(L, -1, &len);
		tp_field(&tnt_iproto, (const void *)str, len);
	}
	lua_pushlstring(L, tp_buf(&tnt_iproto), tp_used(&tnt_iproto));
	tp_free(&tnt_iproto);
	return 1;
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
int ltnt_connection_select(struct lua_State *L) {
	if (lua_gettop(L) != 6):
		luaL_error(L, "bad number of arguments (6 expected, got %d)",
				lua_gettop(L));
	struct tp tnt_iproto;
	uint32_t reqid  = (uint32_t )luaL_checkint(L, 1);
	uint32_t space  = (uint32_t )luaL_checkint(L, 2);
	uint32_t index  = (uint32_t )luaL_checkint(L, 3);
	uint32_t offset = (uint32_t )luaL_checkint(L, 4);
	uint32_t limit  = (uint32_t )luaL_checkint(L, 5);
	if (!lua_istable(L, -1))
		luaL_error(L, "Bad argument #6: (table expected, got %s)",
				lua_typename(L, lua_type(L, -1)));

	tp_init(&tnt_iproto, NULL, 0, tp_realloc, NULL);
	tp_select(&tnt_iproto, space, index, offset, limit);
	tp_reqid(&tnt_iproto, reqid);
	ptrdiff_t i = 0, j = 0;
	for(;; ++i) {
		lua_pushinteger(L, i);
		lua_gettable(L, -2);
		if (lua_isnil(L, -1))
			break;
		if (!lua_istable(L, -1))
			luaL_error(L, "Bad table construction: (table expected, got %s)",
					lua_typename(L, lua_type(L, -1)));
		tp_tuple(&tnt_iproto);
		for (;; ++j) {
			lua_pushinteger(L, j);
			lua_gettable(L, -2);
			if (lua_isnil(L, -1))
				break;
			const char *str = lua_tolstring(L, -1, &len);
			tp_field(&tnt_iproto, (const void *)str, len);
		}
	}
	lua_pushlstring(L, tp_buf(&tnt_iproto), tp_used(&tnt_iproto));
	tp_free(&tnt_iproto);
	return 1;
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
int ltnt_connection_delete(struct lua_State *L) {
	if (lua_gettop(L) != 4):
		luaL_error(L, "bad number of arguments (4 expected, got %d)",
				lua_gettop(L));
	struct tp tnt_iproto;
	uint32_t reqid = (uint32_t )luaL_checkint(L, 1);
	uint32_t space = (uint32_t )luaL_checkint(L, 2);
	uint32_t flags = (uint32_t )luaL_checknumber(L, 3);
	if (!lua_istable(L, 4))
		luaL_error(L, "Bad argument #4: (table expected, got %s)",
				lua_typename(L, lua_type(L, 4)));

	tp_init(&tnt_iproto, NULL, 0, tp_realloc, NULL);
	tp_delete(&tnt_iproto, space, flags);
	tp_reqid(&tnt_iproto, reqid);
	tp_tuple(&tnt_iproto);
	ptrdiff_t i = 0;
	for (;; ++i) {
		lua_pushinteger(L, i);
		lua_gettable(L, -2);
		size_t len = 0;
		if (lua_isnil(L, -1))
			break;
		const char *str = lua_tolstring(L, -1, &len);
		tp_field(&tnt_iproto, (const void *)str, len);
	}
	lua_pushlstring(L, tp_buf(&tnt_iproto), tp_used(&tnt_iproto));
	tp_free(&tnt_iproto);
	return 1;
}

/*
 * Creating of CALL request.
 * Must be called with:
 * reqid : LUA_TNUMBER
 * name  : LUA_TSTRING
 * tuple : LUA_TTABLE as following:
 * { arg_1, arg_2, ... }
 * where key_pM is converted to binary
 * string representation of value
 */

static const struct luaL_reg ltnt_connection_meta[] = {
	{ "__gc"	,ltnt_connection_gc	},
	{ "new"		,ltnt_connection_new	},
	{ "ping"	,ltnt_connection_ping	},
	{ "insert"	,ltnt_connection_insert	},
	{ "select"	,ltnt_connection_select	},
	{ "delete"	,ltnt_connection_delete	},
	{ "update"	,ltnt_connection_update	},
	{ "call"	,ltnt_connection_call	},
	{ NULL		,NULL			}
}

int ltnt_init(struct lua_State *L) {

	/*
	 * Register 'tarantool_connection' "class";
	 */
	luaL_newmetatable(L, "tarantool_connection");
	lua_pushvalue(L, -1);
	lua_pushstring(L, -2, "tarantool_connection");
	lua_setfield(L, -2, "__metatable");
	luaL_register(L, NULL, ltnt_connection_meta);
	lua_pop(L, 1);
	/*
	 * Register other functions
	*/
	lua_getfield(L, LUA_GLOBALSINDEX, "tarantool");
	lua_pushstring(L, "create_connection");
	lua_pushcfunction(L, ltnt_create_connection);
	lua_settable(L, -3);

	lua_pop(L, 1);
	return 0;
}

