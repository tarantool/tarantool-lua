#ifndef H_LUA_TNT_REQUESTBUILDER
#define H_LUA_TNT_REQUESTBUILDER
typedef enum {
	TNT_OP_SET = 0,
	TNT_OP_ADD,
	TNT_OP_AND,
	TNT_OP_XOR,
	TNT_OP_OR,
	TNT_OP_SPLICE,
	TNT_OP_DELETE,
	TNT_OP_INSERT,
} tnt_requestbuilder_op;

typedef enum {
	TNT_BOX_RETURN_TUPLE = 0x01,
	TNT_BOX_ADD = 0x02,
	TNT_BOX_REPLACE = 0x04,
} tnt_requetbuilder_flag;

int ltnt_requestbuilder_ping(struct lua_State *L);
int ltnt_requestbuilder_insert(struct lua_State *L);
int ltnt_requestbuilder_select(struct lua_State *L);
int ltnt_requestbuilder_delete(struct lua_State *L);
int ltnt_requestbuilder_update(struct lua_State *L);
int ltnt_requestbuilder_call(struct lua_State *L);
int ltnt_requestbuilder_getval(struct lua_State *L);
int ltnt_requestbuilder_flush(struct lua_State *L);
int ltnt_requestbuilder_gc(struct lua_State *L);

int ltnt_requestbuilder_new(struct lua_State *L);
#endif /* H_LUA_TNT_REQUESTBIULDER */
