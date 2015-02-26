#!/bin/bash
echo "Setup ccache"
set -eu
ccache -F 1000000 -M 10G
for filename in ~/.bash_profile ~/.bashrc; do
    if [ "0" == `cat ${filename} | grep ccache | wc -l` ]; then
	echo 'export PATH=/usr/lib/ccache:${PATH}' >> ${filename};
    fi;
done;
