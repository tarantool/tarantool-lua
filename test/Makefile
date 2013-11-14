LUAV?=5.2
TARANTOOL_PATH=~/src/stable/test/var/tarantool_box

test:
	rm -rf *.xlog *.snap
	$(TARANTOOL_PATH) --init-storage
	$(TARANTOOL_PATH) &
	sleep 0.5
	echo
	lua$(LUAV) tsc -f RPRB.suite.lua
	echo
	lua$(LUAV) tsc -f TNT.suite.lua
	killall tarantool_box

test_new:
	lua$(LUAV) tarantool.lua
