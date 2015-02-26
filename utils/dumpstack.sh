gdb scidb `pgrep scidb` -batch -x dumpthreads.gdb
for instance in instance2 instance3 instance4 
do 
     ssh $instance "cd bin; gdb scidb `pgrep scidb` -batch -x dumpthreads.gdb"
done
