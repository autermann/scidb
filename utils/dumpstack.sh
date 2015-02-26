gdb scidb `pgrep scidb` -batch -x dumpthreads.gdb
for node in node2 node3 node4 
do 
     ssh $node "cd bin; gdb scidb `pgrep scidb` -batch -x dumpthreads.gdb"
done
