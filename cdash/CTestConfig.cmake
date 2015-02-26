# replace PROJECT_NAME with name of your CDASH project
# replace TIME_ZONE with your time zone
# replace CDASH_SERVER_IP_ADDR with the IP address of the machine on which CDASH is installed
#
# alternatively you can get this file from CDASH administrator

set(CTEST_PROJECT_NAME "scidb")
set(CTEST_NIGHTLY_START_TIME "00:00:00 UTC")

set(CTEST_DROP_METHOD "http")
set(CTEST_DROP_SITE "build.scidb.org")
set(CTEST_DROP_LOCATION "/CDash/submit.php?project=${CTEST_PROJECT_NAME}")
set(CTEST_DROP_SITE_CDASH TRUE)
