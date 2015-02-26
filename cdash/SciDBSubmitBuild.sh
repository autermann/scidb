#!/bin/sh

#
# if required set EXECUTABLE_CMAKE_PATH and EXECUTABLE_CTEST_PATH to those paths with cmake, ctest versions >= 2.8
# and then use this script
# <source_path> is a path of the directory containing scidb code
# <build_path> is a path to be used for out-of-source builds

if test $# -ne 4
then
echo "Usage : SciDBSubmitBuild.sh Nightly|Experimental|Continuous <source_path> <build_path> <build_type>"
echo "        <source_path> : is a path of the directory containing scidb code"
echo "        <build_path>  : is a path to be used for out-of-source builds"
echo "        <build_type>  : is the type of build (CC vs. Release vs. RelWithDebInfo vs. Debug vs. Profile)"
echo ""
exit 1
fi

TESTMODEL=$1
if test "$TESTMODEL" != Nightly && test "$TESTMODEL" != Experimental && test "$TESTMODEL" != Continuous
then
echo "Usage : SciDBSubmitBuild.sh Nightly|Experimental|Continuous <source_path> <build_path> <build_type>"
echo "        <source_path> : is a path of the directory containing scidb code"
echo "        Check if (Nightly|Experimental|Continuous) is spelled correctly."
echo "        <build_path>  : is a path to be used for out-of-source builds"
echo "        <build_type>  : is the type of build (CC, Release, RelWithDebInfo, Debug, Profile)"
echo ""
exit 1
fi

SOURCE_PATH="$2"
BUILD_PATH="$3"
export BUILD_TYPE="$4"
CTESTScriptLOCATION="$SOURCE_PATH/cdash"

if test "$BUILD_TYPE" != "CC" && test "$BUILD_TYPE" != "Release" && test "$BUILD_TYPE" != "RelWithDebInfo" && test "$BUILD_TYPE" != "Debug" && test "$BUILD_TYPE" != "Profile"
then
echo "<build_type> should be CC or Release or RelWithDebInfo or Debug or Profile"
echo ""
exit 1
fi

if ! test -f "$CTESTScriptLOCATION/SciDB_DashboardSubmission.cmake"
then
echo "File $CTESTScriptLOCATION/SciDB_DashboardSubmission.cmake does not exist"
exit 1
fi

if ! test -f "$SOURCE_PATH/CTestConfig.cmake"
then
echo "File $SOURCE_PATH/CTestConfig.cmake does not exist"
exit 1
fi



export TESTMODEL=$TESTMODEL
export EXECUTABLE_CMAKE_PATH=/usr/bin/cmake
export EXECUTABLE_CTEST_PATH=/usr/bin/ctest
export PATH=/opt/$TESTMODEL/share/scidb:/opt/$TESTMODEL/bin:$PATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/$TESTMODEL/lib:/usr/local/apr/lib/

echo "updating SciDB_DashboardSubmission.cmake script..."
svn up $CTESTScriptLOCATION/SciDB_DashboardSubmission.cmake > /dev/null

# ip of the machine submitting the build to the CDASH server.
# required for generating URLs to test case and test result files while preparing customized test case report. 
# If you are accessing this machine using some other external IP address then please set value for this variable to that external IP.
#export cdashclientip=`wget -q -O - http://169.254.169.254/latest/meta-data/public-hostname`
export cdashclientip=

if test -z $cdashclientip
then
echo ""
echo "Warning : value for \"cdashclientip\" in this script is not set. Will use IP of this machine."
echo ""
fi

# mention path for the localhost server's root directory on client machine, and for the custom cdash folder in that directory

export LOCALHOST_ROOT_DIRECTORY=/var/www
CDASH_CUSTOM_FOLDER="cdash_logs"
echo "Info : Using localhost server directory path : \"$LOCALHOST_ROOT_DIRECTORY/$CDASH_CUSTOM_FOLDER\""
echo "Removing older build files..."
find $LOCALHOST_ROOT_DIRECTORY/$CDASH_CUSTOM_FOLDER -maxdepth 1 -type d -mtime +10 -exec /bin/rm -rf {} \; -print;
echo ""

origdir=$(pwd)
echo "Info : Cleaning up build directory $BUILD_PATH"
rm -rf $BUILD_PATH
mkdir -p $BUILD_PATH

if ! test -w "$BUILD_PATH"
then
echo ""
echo "ERROR : No write permissions for directory $BUILD_PATH"
exit 1
fi

echo "Info : Copying files to $BUILD_PATH"
cp -r $SOURCE_PATH $BUILD_PATH
echo "Info : Updating svn in $SOURCE_PATH"
cd $SOURCE_PATH
svn up > /dev/null
SOURCE_PATH=$BUILD_PATH/trunk/
cd $SOURCE_PATH
cmake . > /dev/null 2>&1

TIMESTAMP=$(date +%H-%M-%S_%d.%m.%Y)

LOGFILE="/tmp/scidb_${TESTMODEL}Build.log_${TIMESTAMP}"
export CDASH__LOGFILE=$LOGFILE

echo "Performing update, compile, test, coverage report submission for this build. Please wait ..."
echo "Please check the log in file [$LOGFILE]"
echo ""

export CDASH_TESTCASES_FOLDER="$CDASH_CUSTOM_FOLDER/scidb_${TESTMODEL}Build.log_${TIMESTAMP}"
mkdir -p "$LOCALHOST_ROOT_DIRECTORY/$CDASH_TESTCASES_FOLDER"
export scidbtestresultsURL=$CDASH_TESTCASES_FOLDER/r/
export scidbtestcasesURL=$CDASH_TESTCASES_FOLDER/t/

$EXECUTABLE_CTEST_PATH -S "cdash/SciDB_DashboardSubmission.cmake" -VV > "$LOGFILE" 2>&1
cd $origdir

echo "Done."
echo ""
exit 0
