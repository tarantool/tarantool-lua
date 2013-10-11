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

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

#include <3rdparty/tp/tp.h>
#include <include/tnt_helper.h>
#include <include/tnt_requestbuilder.h>
#include <include/tnt_responseparser.h>

static const struct tnt_Enum ops[] = {
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

static const struct tnt_Enum flags[] = {
	{ "RETURN_TUPLE",TNT_BOX_RETURN_TUPLE	},
	{ "BOX_ADD"	,TNT_BOX_ADD		},
	{ "BOX_REPLACE"	,TNT_BOX_REPLACE	},
	{ NULL		,0			}
};


static const struct luaL_Reg lrequestresponse[] = {
	{ "request_builder_new"	,ltnt_requestbuilder_new},
	{ "response_parser_new"	,ltnt_responseparser_new},
	{ "get_body_len"	,ltnt_response_bodylen	},
	{ NULL			,NULL			}
};

static const struct luaL_Reg lresponseparser_meta[] = {
	{ "parse"	,ltnt_responseparser_parse	},
	{ "__gc"	,ltnt_responseparser_gc		},
	{ NULL		,NULL				}
};

static const struct luaL_Reg lrequestbuilder_meta[] = {
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
int lrequestbuilder_open(lua_State *L) {
	luaL_newmetatable(L, "RequestBuilder");
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, lrequestbuilder_meta, 0);
	return 1;
}

/*
 * Register 'ResponseParser' "class";
 */
int lresponseparser_open(lua_State *L) {
	luaL_newmetatable(L, "ResponseParser");
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_setfuncs(L, lresponseparser_meta, 0);
	return 1;
}

int luaopen_tnt(struct lua_State *L) {
	luaL_newlib(L, lrequestresponse);
	lrequestbuilder_open(L);
	lua_setfield(L, 3, "RequestBuilder");
	lresponseparser_open(L);
	lua_setfield(L, 3, "ResponseParser");
	lregister_enum(L, 3, ops, "ops");
	lregister_enum(L, 3, flags, "flags");
	/*
	 * Register other functions
	 */
	return 1;
}

