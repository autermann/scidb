#!/bin/bash

set -u

username=${1}
scidb.py startall ${username}
until iquery -aq "list()" > /dev/null 2>&1; do sleep 1; done
