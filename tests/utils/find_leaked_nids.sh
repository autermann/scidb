#!/bin/sh

#$1 - db name = password = instance
#$2 - PG host

PGPASSWORD="$1" psql -U ${1} -h ${2} --dbname ${1} --command "select AR.name from \"array\" as AR where not exists (select AD.array_id from array_dimension as AD where AR.name=AD.mapping_array_name) and AR.name like '%:%'"
