sudo opcontrol --no-vmlinux
sudo opcontrol --callgraph=2
sudo opcontrol --reset
sudo opcontrol --start
"$@"
sudo opcontrol --shutdown
opreport --callgraph --exclude-dependent --demangle=smart --symbols scidb

