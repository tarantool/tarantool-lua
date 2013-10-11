# Default installation prefix
LUAV?=5.2
PREFIX=/usr

# System's libraries directory (where binary libraries are installed)
LUA_LIBDIR=$(PREFIX)/lib/lua$(LUAV)

# System's lua directory (where Lua libraries are installed)
LUA_DIR=$(PREFIX)/share/lua$(LUAV)

LUAINC=$(PREFIX)/include
LUALIB=$(PREFIX)/lib

CC=gcc
# -fexceptions is necessary if your Lua was built with a C++ compiler and 
# uses exceptions internally; can be removed
CFLAGS=-O2 -Wall $(INC) -shared -fPIC -fexceptions
LDFLAGS=-shared `pkg-config lua5.2 --libs`
INC=`pkg-config lua$(LUAV) --cflags`

OBJS=lyaml.o api.o dumper.o emitter.o loader.o parser.o reader.o scanner.o writer.o b64.o

all:    yaml.so
       
yaml.so: $(OBJS)
	$(CC) -o $@ $(LDFLAGS) $(OBJS)

clean:
	rm -f $(OBJS) yaml.so core core.* a.out
