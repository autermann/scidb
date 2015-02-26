#!/bin/sh
set -eux
function do_restart()
{
./runN.py 1 scidb --istart
sleep 1
}

R_PATH=testcases/r/perf/repart
T_PATH=testcases/t/perf/repart

function do_test()
{
    rm -rf ${R_PATH}/*
    for kind in sparse dense; do
	for name in `(cd ${T_PATH}; ls *.test | grep $kind) | sed -e "s/\.test//g"`; do 
	    echo "Mode: $1 Test: $name"
	    do_restart
	    ../../bin/scidbtestharness --root-dir=testcases/ --test-id=perf.repart.$name --record
	done;
    done;
    rm -rf $1
    mkdir -p $1
    for name in `find ${R_PATH} -name "*.timer"`; do cp $name $1; done;
}
unset REPART_ENABLE_TILE_MODE
unset TILE_SIZE
echo "Value mode"
do_test "value"
export REPART_ENABLE_TILE_MODE=1
unset TILE_SIZE
echo "Tile mode"
do_test "tile"
