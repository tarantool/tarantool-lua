#ifndef H_LUA_TNT_HELPER
#define H_LUA_TNT_HELPER
typedef struct luatarantool_Enum {
	const char *name;
	const int val;
} luatarantool_Enum;

void ltnt_register_enum(struct lua_State *L, int narg,
			const struct luatarantool_Enum *e,
			const char *str);

inline const char *ltnt_checkstring(struct lua_State *L, int narg, size_t *len);
inline struct tp **ltnt_checkrequestbuilder(struct lua_State *L, int narg);
inline struct tp **ltnt_checkresponseparser(struct lua_State *L, int narg);
void ltnt_pushtuple(struct lua_State *L, struct tp **iproto, int narg);
void ltnt_pushsntable(struct lua_State *L, int narg,
			const char *str, int num);
#endif /* H_LUA_TNT_HELPER */
