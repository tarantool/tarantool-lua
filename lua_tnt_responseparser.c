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

int ltnt_response_former(lua_State *L) {
	ltnt_response_parser
	struct tp **iproto = lua_userdata(L, -1);
	if (len(buf) < 12)
		return -1;
	else
	
}

int ltnt_responseparser_parse(lua_State *L) {
	struct tp **iproto = ltnt_checkresponseparser(L, 1);
	ssize_t resps = 0;
	char *resp = (char *)ltnt_checkstring(L, 2, &resps);
	if (resps < HEADER_LEN)
		luaL_error(L, "ResponseParser: expected at least"
				" 12 bytes, got %d", resps);
	tp_init(*iproto, resp, 12, NULL, NULL);
	ssize_t required = tp_reqbuf(*tp, resp);
	if (required > 0)
		luaL_error(L, "ResponseParser: expected"
				"%d bytes, got %d", resps+required, resps);
	lua_pushtable(L, 0, 5);
	ssize_t  sc  = tp_reply(*iproto);
	ssize_t  ec  = sc >> 8;
	sc = sc & 0xFF;
	uint32_t op  = tp_replyop(*iproto);
	uint32_t cnt = tp_replycount(*iproto);
	lua_pushstring(L, );
	lua_pushnumber(L, sc);
	lua_rawsetp(L, -2, "reply_code");
	lua_pushnumber(L, op);
	lua_rawsetp(L, -2, "operation_code");
	lua_pushnumber(L, cnt);
	lua_rawsetp(L, -2, "tuple_count");
	if (sc != 0) {
		lua_pushtable(L, 0, 2);
		lua_pushnumber(L, ec);
		lua_rawsetp(L, -2, "errcode");
		lua_pushstring(L, tp_replyerror(*iproto),
				tp_replyerrorlen(*iproto));
		lua_rawsetp(L, -2, "errstr");
	}
	lua_rawsetp(L, -2, "error");
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

