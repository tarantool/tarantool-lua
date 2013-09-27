#ifndef H_LUA_TNT_HELPER
#define H_LUA_TNT_HELPER
inline const char *ltnt_checkstring(struct lua_State *L, int narg, size_t *len);
inline struct tp **ltnt_checkrequestbuilder(struct lua_State *L, int narg);
int ltnt_pushtuple(struct lua_State *L, struct tp **iproto, int narg);
#endif /* H_LUA_TNT_HELPER */
