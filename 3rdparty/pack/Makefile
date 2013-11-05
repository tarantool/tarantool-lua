# 5.2, jit
LUAV?=5.2

LUA_CFLAGS 	= `pkg-config lua$(LUAV) --cflags`
LUA_LDFLAGS	= `pkg-config lua$(LUAV) --libs` -g -shared

CFLAGS		= -O2 -Wall -shared -fPIC -fexceptions $(LUA_CFLAGS) -I.
LDFLAGS		= -shared -g $(LUA_LDFLAGS)

CC 			= gcc
OUTPUT		= pack.so

OBJS = pack.o

.PHONY: all clean dist test t

all: pack.so

pack.so: pack.o
	$(CC) $(LDFLAGS) -o ${OUTPUT} ${OBJS}

clean:
	rm -f *.so *.o pack/*.so

