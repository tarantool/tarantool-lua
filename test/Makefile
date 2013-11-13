LUAV?=5.2

test:
	rm -rf *.xlog
	~/src/stable/test/var/tarantool_box &
	sleep 0.5
	lua$(LUAV) tsc -f RPRB.suite.lua
	lua$(LUAV) tsc -f TNT.suite.lua
	killall tarantool_box

test_new:
	lua$(LUAV) tarantool.lua
