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
#include <include/lua_tnt_requestbuilder.h>
#include <include/lua_tnt_helper.h>

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
	size_t name_size = 0;
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

/*
 * Creating of UPDATE request.
 * Must be called with:
 * reqid: LUA_TNUMBER
 * space: LUA_TNUMBER
 * flags: LUA_TNUMBER
 * table: LUA_TTABLE as folowing:
 * { val_1, val_2, ... }
 * where val_N is converted to binary
 * string representation of value
 * ops  : LUA_TTABLE as following:
 * { 0 : {OP, field, data}
 *   1 : {SPLICE, field, offset, cut, data}
 *   ...
 * }
 *
 * returns LUA_TSTRING with binary packed request
*/
int ltnt_requestbuilder_update(struct lua_State *L) {
	if (lua_gettop(L) != 6)
		luaL_error(L, "bad number of arguments (5 expected, got %d)",
				lua_gettop(L) - 1);
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	uint32_t reqid = (uint32_t )luaL_checkint(L, 2);
	uint32_t space = (uint32_t )luaL_checkint(L, 3);
	uint32_t flags = (uint32_t )luaL_checknumber(L, 4);
	if (!lua_istable(L, 5))
		luaL_error(L, "Bad argument #4: (table expected, got %s)",
				lua_typename(L, lua_type(L, 5)));
	if (!lua_istable(L, 6))
		luaL_error(L, "Bad argument #5: (table expected, got %s)",
				lua_typename(L, lua_type(L, 6)));
	int ops = 6;
	int opcur = 8;
	tp_update(*iproto, space, flags);
	tp_reqid(*iproto, reqid);
	tp_tuple(*iproto);
	ltnt_pushtuple(L, iproto, 5);
	tp_updatebegin(*iproto);
	lua_pushnil(L);
	while (lua_next(L, ops) != 0) {
		if (!lua_istable(L, opcur))
			luaL_error(L, "Bad table construction: (table expected, got %s)",
					lua_typename(L, lua_type(L, -1)));
		lua_pushnil(L);
		lua_next(L, opcur);
		uint8_t op = (uint8_t )luaL_checkint(L, -1);
		lua_pop(L, 1);
		lua_next(L, opcur);
		uint32_t field = (uint32_t )luaL_checkint(L, -1);
		lua_pop(L, 1);
		lua_next(L, opcur);
		if (op == TNT_OP_SPLICE) {
			uint32_t offset = (uint32_t )luaL_checkint(L, -1);
			lua_pop(L, 1);
			lua_next(L, opcur);
			uint32_t cut = (uint32_t )luaL_checkint(L, -1);
			lua_pop(L, 1);
			lua_next(L, opcur);
			size_t len = 0;
			const char *data = ltnt_checkstring(L, -1, &len);
			tp_opsplice(*iproto, field, offset, cut, data, len);
		}
		else {
			const char *data = NULL;
			size_t len = 0;
			if (lua_isstring(L, -1)) {
				data = ltnt_checkstring(L, -1, &len);
			} else if (lua_isnumber(L, -1)) {
				data = (char *)(intptr_t )luaL_checkint(L, -1);
				len = 4;
			} else {
				luaL_error(L, "Bad op argument: (string or number expected, got %s)",
					lua_typename(L, lua_type(L, -1)));
			}
			tp_op(*iproto, op, field, data, len);
		}
		lua_pop(L, 3);
	}
	return 0;
}

/* Get request for RequestBuilder `class` */
int ltnt_requestbuilder_getval(lua_State *L) {
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	lua_pushlstring(L, tp_buf(*iproto), tp_used(*iproto));
	return 1;
}

/* Clear buffer for RequestBuilder `class` */
int ltnt_requestbuilder_flush(lua_State *L) {
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	tp_init(*iproto, tp_buf(*iproto), tp_size(*iproto), tp_realloc, NULL);
	return 0;
}

/* GC method for RequestBuilder `class` */
int ltnt_requestbuilder_gc(lua_State *L) {
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	tp_free(*iproto);
	free(*iproto);
	return 0;
}


int ltnt_requestbuilder_new(lua_State *L) {
	struct tp **iproto = lua_newuserdata(L, sizeof(struct tp*));
	*iproto = (struct tp *)malloc(sizeof(struct tp));
	luaL_getmetatable(L, "RequestBuilder");
	lua_setmetatable(L, -2);
	tp_init(*iproto, NULL, 0, tp_realloc, NULL);
	return 1;
}
