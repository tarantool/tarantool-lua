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


#if defined (__cplusplus)
}
#endif

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

#include "tp/tp.h"
#include "include/lua_tnt_helper.h"
#include "include/lua_tnt_requestbuilder.h"
#include "include/lua_tnt_responseparser.h"

static const struct luaL_Reg ltnt_requestresponse[] = {
	{ "request_builder_new"	,ltnt_requestbuilder_new},
//	{ "response_parser_new"	,ltnt_responseparser_new},
	{ NULL			,NULL			}
};

/*
static const struct luaL_Reg ltnt_responseparser_meta[] = {
	{ "parse"	,ltnt_responseparser_parse	},
	{ "get_unused"	,ltnt_responseparser_end	},
	{ "flush"	,ltnt_responseparser_flush	},
	{ "__gc"	,ltnt_responseparser_gc		},
	{ NULL		,NULL				}
};
*/

static const struct luaL_Reg ltnt_requestbuilder_meta[] = {
	{ "ping"	,ltnt_requestbuilder_ping	},
	{ "insert"	,ltnt_requestbuilder_insert	},
	{ "select"	,ltnt_requestbuilder_select	},
	{ "delete"	,ltnt_requestbuilder_delete	},
	{ "update"	,ltnt_requestbuilder_update	},
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

/*
 * Register 'ResponseParser' "class";
 */
/*
int ltnt_requestparser_open(lua_State *L) {
	luaL_newmetatable(L, "tarantool.ResponseParser");
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, ltnt_responseparser_meta, 0);
	return 1;
}
*/
int luaopen_lua_tarantool(struct lua_State *L) {
	ltnt_requestbuilder_open(L);
//	ltnt_responseparser_open(L);
	luaL_newlib(L, ltnt_requestresponse);
	/*
	 * Register other functions
	*/
	return 1;
}

