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

static const struct luatarantool_Enum ops[] = {
	{ "OP_SET"	,TNT_OP_SET		},
	{ "OP_ADD"	,TNT_OP_ADD		},
	{ "OP_AND"	,TNT_OP_AND		},
	{ "OP_XOR"	,TNT_OP_XOR		},
	{ "OP_OR"	,TNT_OP_OR		},
	{ "OP_SPLICE"	,TNT_OP_SPLICE		},
	{ "OP_DELETE"	,TNT_OP_DELETE		},
	{ "OP_INSERT"	,TNT_OP_INSERT		},
	{ NULL		,0			}
};

static const struct luatarantool_Enum flags[] = {
	{ "RETURN_TUPLE",TNT_BOX_RETURN_TUPLE	},
	{ "BOX_ADD"	,TNT_BOX_ADD		},
	{ "BOX_REPLACE"	,TNT_BOX_REPLACE	},
	{ NULL		,0			}
};


static const struct luaL_Reg ltnt_requestresponse[] = {
	{ "request_builder_new"	,ltnt_requestbuilder_new},
	{ "response_parser_new"	,ltnt_responseparser_new},
	{ "get_body_len"	,ltnt_response_bodylen	},
	{ NULL			,NULL			}
};

static const struct luaL_Reg ltnt_responseparser_meta[] = {
	{ "parse"	,ltnt_responseparser_parse	},
	{ "__gc"	,ltnt_responseparser_gc		},
	{ "__call"	,ltnt_responseparser_new	},
	{ NULL		,NULL				}
};

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
	{ "__call"	,ltnt_requestbuilder_new	},
	{ NULL		,NULL				}
};

/*
 * Register 'RequestBuilder' "class";
 */
int ltnt_requestbuilder_open(lua_State *L) {
	luaL_newmetatable(L, "RequestBuilder");
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, ltnt_requestbuilder_meta, 0);
	return 1;
}

/*
 * Register 'ResponseParser' "class";
 */
int ltnt_responseparser_open(lua_State *L) {
	luaL_newmetatable(L, "ResponseParser");
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, ltnt_responseparser_meta, 0);
	return 1;
}

int luaopen_lua_tarantool(struct lua_State *L) {
	luaL_newlib(L, ltnt_requestresponse);
	ltnt_requestbuilder_open(L);
	lua_setfield(L, 3, "RequestBuilder");
	ltnt_responseparser_open(L);
	lua_setfield(L, 3, "ResponseParser");
	ltnt_register_enum(L, 3, ops, "ops");
	ltnt_register_enum(L, 3, flags, "flags");
	/*
	 * Register other functions
	 */
	return 1;
}

