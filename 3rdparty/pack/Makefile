version=0.01

name=pack
dist=$(name)-$(version)

LUA_VERSION =   5.1

# See http://lua-users.org/wiki/BuildingModules for platform specific
# details.

## Linux/BSD
PREFIX ?=          /usr/local
LDFLAGS +=         -shared

## OSX (Macports)
#PREFIX ?=          /opt/local
#LDFLAGS +=         -bundle -undefined dynamic_lookup

LUA_INCLUDE_DIR ?= $(PREFIX)/include
LUA_LIB_DIR ?=     $(PREFIX)/lib/lua/$(LUA_VERSION)

#CFLAGS ?=          -g -Wall -pedantic -fno-inline
CFLAGS ?=          -g -O -Wall
CFLAGS += -fpic -I$(LUA_INCLUDE_DIR) -I/usr/local/rb2/include/ -I/usr/local/mpop/include

INSTALL ?= install

.PHONY: all clean dist test t

#CC = gcc
RM = rm -f

all: pack.so

pack.so: pack.o
	$(CC) $(LDFLAGS) -o $@ $^

install:
	$(INSTALL) -d $(DESTDIR)/$(LUA_LIB_DIR)
	$(INSTALL) pack.so $(DESTDIR)/$(LUA_LIB_DIR)

clean:
	$(RM) *.so *.o pack/*.so

