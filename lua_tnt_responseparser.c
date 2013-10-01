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

int ltnt_response_checker(lua_State *L) {
	ltnt_response_parser
	ssize_t resps = 0;
	char *resp = (char *)ltnt_checkstring(L, 2, &resps);
	
}

int ltnt_responseparser_parse(lua_State *L) {
	lua_checkstack(L, 10);
	struct tp **iproto = ltnt_checkresponseparser(L, 1);
	ssize_t resps = 0;
	char *resp = (char *)ltnt_checkstring(L, 2, &resps);
	if (resps < HEADER_LEN)
		luaL_error(L, "ResponseParser: expected at least"
				" 12 bytes, got %d", resps);
	tp_init(*iproto, resp, 12, NULL, NULL);
	ssize_t required = tp_reqbuf(*tp, resp);
	if (required + 12 != resps)
		luaL_error(L, "ResponseParser: expected"
				"%d bytes, got %d", resps+required, resps);
	tp_init(*iproto, resp, resps, NULL, NULL);
	ssize_t  sc  = tp_reply(*iproto);
	ssize_t  ec  = sc >> 8;
	sc = sc & 0xFF;
	uint32_t op  = tp_replyop(*iproto);
	uint32_t cnt = tp_replycount(*iproto);
	lua_pushtable(L, 0, 5);
	ltnt_pushsntable(L, -1, "reply_code", sc);
	ltnt_pushsntable(L, -1, "operation_code", op);
	ltnt_pushsntable(L, -1, "tuple_count", cnt);

	if (sc != 0) {
		lua_pushstring(L, "error");
		lua_pushtable(L, 0, 2);
		ltnt_pushsntable(L, -1, "errcode", ec);
		lua_pushstring(L, "errstr");
		lua_pushstring(L, tp_replyerror(*iproto),
				tp_replyerrorlen(*iproto));
		lua_settable(L, -3);
		lua_settable(L, -3);
	} else {
		lua_pushstring(L, "tuples");
		lua_pushtable(L, 0, cnt);
		ssize_t tup_num = 1;
		while (tp_next(*iproto)) {
			lua_pushnumber(L, tup_num);
			lua_pushtable(L, tp_tuplecount(*iproto), 0);
			ssize_t fld_num = 1;
			while(tp_nextfield(&rep)) {
				lua_pushnumber(L, fld_num);
				lua_pushlstring(L, tp_getfield(*iproto),
						tp_getfieldsize(*iproto));
				lua_settable(L, -3);
				fld_num++;
			}
			lua_settable(L, -2);
			tup_num++;
		}
		lua_settable(L, -2);
	}
	return 1;
}

/* GC method for RequestParser `class` */
int ltnt_requestbuilder_gc(lua_State *L) {
	struct tp **iproto = ltnt_checkrequestbuilder(L, 1);
	tp_free(*iproto);
	free(*iproto);
	return 0;
}

int ltnt_responseparser_new(lua_State *L) {
	struct tp **iproto = lua_newuserdata(L, sizeof(struct tp*));
	*iproto = (struct tp *)malloc(sizeof(struct tp));
	luaL_getmetatable(L, "tarantool.ResponseParser");
	lua_setmetatable(L, -2);
	return 1;
}

