LUA_CFLAGS 	= `pkg-config lua5.2 --cflags`
LUA_LDFLAGS	= `pkg-config lua5.2 --cflags` -g -shared

CFLAGS		= -O2 -Wall -shared -fPIC -fexceptions $(LUA_CFLAGS) -I.
LDFLAGS		= -shared -g $(LUA_LDFLAGS)

CC 			= gcc
OUTPUT		= tnt.so

OBJS = src/tnt.o \
	   src/tnt_helper.o \
	   src/tnt_requestbuilder.o \
	   src/tnt_responseparser.o

all: $(OBJS)
	$(CC) -o $(OUTPUT) $(LDFLAGS) ${OBJS}
	cp -f tnt.so test/

libs: yaml luasocket telescope

luasocket:
	make -C 3rdparty/luasocket all
	cp -f 3rdparty/luasocket/socket.lua test/socket.lua
	cp -f 3rdparty/luasocket/socket.so.3.0-rc1 test/socket.so

yaml:
	make -C 3rdparty/yaml all
	cp -f 3rdparty/yaml/yaml.so test/yaml.so

telescope:
	cp -f 3rdparty/telescope/* test/

clean-all:
	make -C 3rdparty/luasocket clean
	make -C 3rdparty/yaml clean
	make clean

clean:
	rm -f test/*.so
	rm -f *.so
	rm -f $(OBJS)

test:
	lua test/test.lua

.PHONY: luasocket yaml test
