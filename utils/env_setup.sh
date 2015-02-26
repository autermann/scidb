#
#  This script illustrates how we might set the range of 
# environment variables needed by scidb.py to initialize, 
# start and stop SciDB compute instance instances. The idea is 
# that SciDB (currently) uses a Postgres DBMS instance to 
# manage all of the meta-data. And these details are needed
# to connecto to that server. 
#
#  To set up variables that are relevant to your instance, 
# replace each of the env variables exported below w/ 
# appropriate values, and 'source' this file. 
#
# Pick which one you want - set or unset 
# 
# SCIDB_DB_HOST - Name or IP address of the machine on which 
#                 the meta-data service runs. 
# 
# SCIDB_DB_NAME - The name of the Postgres database within the 
#                 Postgres instance running on SCIDB_DB_HOST.
# 
# SCIDB_DB_USER - The name of a user with appropriate privileges
#                 over the SCIDB_DB_NAME database on the 
#                 SCIDB_DB_HOST server. 
#
# SCIDB_DB_PASSWD - Password for SCIDB_DB_USER. 
# SCIDB_DATA_DIR  - DATA_DIR for the SciDB engine. 
#                
# Yes. This is a "password in the clear". My bad. We'll 
# fix this when it becomes a priority.
#
# unset SCIDB_DB_HOST
# unset SCIDB_DB_NAME
# unset SCIDB_DB_USER
# unset SCIDB_DB_PASSWD
#
export SCIDB_DB_HOST='localhost'
export SCIDB_DB_NAME='scidb_test'
export SCIDB_DB_USER='scidb_user'
export SCIDB_DB_PASSWD='scidb_user'
export SCIDB_DATA_DIR=`pwd`
