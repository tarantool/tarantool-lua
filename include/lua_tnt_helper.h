#ifndef H_LUA_TNT_HELPER
#define H_LUA_TNT_HELPER
inline const char *ltnt_checkstring(struct lua_State *L, int narg, size_t *len);
inline struct tp **ltnt_checkrequestbuilder(struct lua_State *L, int narg);
inline struct tp **ltnt_checkresponseparser(struct lua_State *L, int narg);
int ltnt_pushtuple(struct lua_State *L, struct tp **iproto, int narg);
void ltnt_pushnstable(struct lua_State *L, int narg, int num,
		const char *str, ssize_t len);
#endif /* H_LUA_TNT_HELPER */
