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

#include <3rdparty/tp/tp.h>
#include <include/tnt_responseparser.h>
#include <include/tnt_helper.h>

int ltnt_response_bodylen(struct lua_State *L) {
	size_t resps = 0;
	char *resp = (char *)ltnt_checkstring(L, 1, &resps);
	lua_pushnumber(L, tp_reqbuf(resp, 12));
	return 1;
}

int ltnt_responseparser_parse(struct lua_State *L) {
	lua_checkstack(L, 10);
	struct tp **iproto = ltnt_checkresponseparser(L, 1);
	size_t resps = 0;
	char *resp = (char *)ltnt_checkstring(L, 2, &resps);
	/* Check HEADER_LEN */
	if (resps < 12)
		luaL_error(L, "ResponseParser: expected at least"
				" 12 bytes, got %d", resps);
	tp_init(*iproto, resp, 12, NULL, NULL);
	ssize_t required = tp_reqbuf(resp, 12);
	if (required + 12 != resps)
		luaL_error(L, "ResponseParser: expected"
				" %d bytes, got %d", resps+required, resps);
	tp_init(*iproto, resp, resps, NULL, NULL);
	ssize_t  sc  = tp_reply(*iproto);
	if (sc == -1) {
		lua_pushboolean(L, 0);
		lua_pushstring(L, "tp.h bad answer");
		return 2;
	}
	ssize_t ec  = sc >> 8;
	sc = sc & 0xFF;
	lua_pushboolean(L, 1);
	lua_createtable(L, 0, 5);
	ltnt_pushsntable(L, -1, "reply_code", sc);
	ltnt_pushsntable(L, -1, "operation_code", tp_replyop(*iproto));
	ltnt_pushsntable(L, -1, "tuple_count", tp_replycount(*iproto));
	ltnt_pushsntable(L, -1, "request_id", tp_getreqid(*iproto));

	if (sc != 0) {
		lua_pushstring(L, "error");
		lua_createtable(L, 0, 2);
		ltnt_pushsntable(L, -1, "errcode", ec);
		lua_pushstring(L, "errstr");
		lua_pushlstring(L, tp_replyerror(*iproto),
				tp_replyerrorlen(*iproto));
		lua_settable(L, -3);
		lua_settable(L, -3);
	} else {
		lua_pushstring(L, "tuples");
		lua_createtable(L, 0, tp_replycount(*iproto));
		ssize_t tup_num = 1;
		while (tp_next(*iproto)) {
			lua_pushnumber(L, tup_num);
			lua_createtable(L, tp_tuplecount(*iproto), 0);
			ssize_t fld_num = 1;
			while(tp_nextfield(*iproto)) {
				lua_pushnumber(L, fld_num);
				lua_pushlstring(L, tp_getfield(*iproto),
						tp_getfieldsize(*iproto));
				lua_settable(L, -3);
				fld_num++;
			}
			lua_settable(L, -3);
			tup_num++;
		}
		lua_settable(L, -3);
	}
	return 2;
}

/* GC method for ResponseParser `class` */
int ltnt_responseparser_gc(struct lua_State *L) {
	struct tp **iproto = ltnt_checkresponseparser(L, 1);
	free(*iproto);
	return 0;
}

int ltnt_responseparser_new(struct lua_State *L) {
	struct tp **iproto = lua_newuserdata(L, sizeof(struct tp*));
	*iproto = (struct tp *)malloc(sizeof(struct tp));
	luaL_getmetatable(L, "ResponseParser");
	lua_setmetatable(L, -2);
	return 1;
}

