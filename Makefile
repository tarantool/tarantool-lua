LUA_CFLAGS 	= `pkg-config lua5.2 --cflags`
LUA_LDFLAGS	= `pkg-config lua5.2 --cflags` -g -shared

CFLAGS		= -O2 -Wall -shared -fPIC -fexceptions $(LUA_CFLAGS) -I.
LDFLAGS		= -shared -g $(LUA_LDFLAGS)

CC 			= gcc
OUTPUT		= tnt.so

OBJS = tnt.o \
	   tnt_helper.o \
	   tnt_requestbuilder.o \
	   tnt_responseparser.o 

all: $(OBJS)
	$(CC) -o $(OUTPUT) $(LDFLAGS) ${OBJS}
	cp -f tnt.so test/

libs: yaml luasocket

luasocket:
	make -C luasocket all
	cp -f luasocket/socket.lua test/socket.lua
	mv -f luasocket/socket.so.3.0-rc1 test/socket.so

yaml:
	make -C yaml all
	mv -f yaml/yaml.so test/yaml.so

clean-all:
	make -C luasocket clean
	make -C yaml clean
	make clean

clean:
	rm -f test/*.so
	rm -f *.o

test:
	lua test/test.lua

.PHONY: luasocket yaml test
