#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdlib.h>
#include <string.h>

#define TYPE_NUMBER 0
#define TYPE_STRING 1

#define MAX_NUMBER_LENGTH 20
#define MAX_BASE128_LENGTH 5

static int pack_bytes(lua_State *L, int bytes, int type) {

	unsigned long long b;

	if (((type == TYPE_NUMBER) && !lua_isnumber(L, -1)) || ((type == TYPE_STRING) && !lua_isstring(L, -1)))
		return 0;

	if (type == TYPE_NUMBER)
		b = (unsigned long long)lua_tonumber(L, -1);
	else
		b = strtoull(lua_tostring(L, -1), NULL, 10);

	lua_pop(L, 1);

	char p[bytes + 1];
	int i;
	for (i = 0; i < bytes; ++i) {
		p[i] = b & 0xFF;
		b >>= 8;
	}
	p[bytes] = '\0';
	
	lua_pushlstring(L, p, bytes);
	return 1;
}

static int unpack_bytes(lua_State *L, int bytes, int type) {

	if (!lua_isstring(L, -1))
		return 0;

	size_t length = 0;
	const char *p = lua_tolstring(L, -1, &length);
	lua_pop(L, 1);

	if (length != bytes)
		return 0;

	unsigned long long b = 0;
	unsigned long long r = 0;
	int i;
	for (i = 0; i < bytes; ++i) {
		r = (unsigned char)p[i];
		b |= r << (8 * i);
	}

	if (type == TYPE_NUMBER)
		lua_pushnumber(L, b);
	else {
		char s[MAX_NUMBER_LENGTH];
		sprintf(s, "%llu", b);
		lua_pushstring(L, s);
	}

	return 1;
}

static int pack_B(lua_State *L) {

	return pack_bytes(L, 1, TYPE_NUMBER);
}

static int unpack_B(lua_State *L) {

	return unpack_bytes(L, 1, TYPE_NUMBER);
}

static int pack_S(lua_State *L) {

	return pack_bytes(L, 2, TYPE_NUMBER);
}

static int unpack_S(lua_State *L) {

	return unpack_bytes(L, 2, TYPE_NUMBER);
}

static int pack_L(lua_State *L) {

	return pack_bytes(L, 4, TYPE_NUMBER);
}

static int unpack_L(lua_State *L) {

	return unpack_bytes(L, 4, TYPE_NUMBER);
}

static int pack_Q_string(lua_State *L) {

	return pack_bytes(L, 8, TYPE_STRING);
}

static int unpack_Q_string(lua_State *L) {

	return unpack_bytes(L, 8, TYPE_STRING);
}

static int pack_int_base128(lua_State *L) {

	if (!lua_isnumber(L, -1))
		return 0;

	unsigned long long b = lua_tonumber(L, -1);
	lua_pop(L, 1);

	char p[MAX_BASE128_LENGTH + 1];

	p[MAX_BASE128_LENGTH] = '\0';
	p[MAX_BASE128_LENGTH - 1] = b & 0x7F;
	b >>= 7;

	int i = MAX_BASE128_LENGTH - 2;
	while (b > 0 && i >= 0) {
		p[i] = (b & 0x7F) | 0x80;
		b >>= 7;
		i--;
	}

	lua_pushlstring(L, p + i + 1, MAX_BASE128_LENGTH - 1 - i);
	return 1;
}

static int unpack_int_base128(lua_State *L) {

	if (!lua_isstring(L, -1))
		return 0;

	size_t length;
	const char *p = lua_tolstring(L, -1, &length);
	lua_pop(L, -1);

	unsigned long long b = 0;

	int i = 0;
	for (;; ++i) {
		if (p[i] & 0x80)
			b = (b << 7) | ((unsigned char)p[i] ^ 0x80);
		else {
			b = (b << 7) | (unsigned char)p[i];
			break;
		}
	}

	lua_pushnumber(L, b);
	lua_pushnumber(L, i + 1);
	return 2;
}

static int pack_str(lua_State *L) {

	size_t string_length;
	const char *string = lua_tolstring(L, -1, &string_length);

	lua_pushnumber(L, string_length);
	if (!pack_int_base128(L))
		return 0;

	size_t prefix_length;
	const char *prefix = lua_tolstring(L, -1, &prefix_length);

	char r[prefix_length + string_length];
	memcpy(r, prefix, prefix_length);
	memcpy(r + prefix_length, string, string_length);

	lua_pushlstring(L, r, prefix_length + string_length);
	return 1;
}

static const struct luaL_Reg packlib[] = {
	{"pack_B", pack_B},
	{"unpack_B", unpack_B},
	{"pack_S", pack_S},
	{"unpack_S", unpack_S},
	{"pack_L", pack_L},
	{"unpack_L", unpack_L},
	{"pack_Q_string", pack_Q_string},
	{"unpack_Q_string", unpack_Q_string},
	{"pack_int_base128", pack_int_base128},
	{"unpack_int_base128", unpack_int_base128},
	{"pack_str", pack_str},
	{NULL, NULL}
};

int luaopen_pack(lua_State *L) {
	luaL_register(L, "pack", packlib);
	return 1;
}
