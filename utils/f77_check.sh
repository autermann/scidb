#!/bin/bash
#

FAIL=0
for FILE in $* ; do
    echo "checking $FILE" >&2
    awk '
    BEGIN  { FAIL=0; }
           { if(length() >72) { FAIL=1; printf("line %s too long: %s\n",NR, $0); } }
    END    { exit(FAIL); }
    ' $FILE

    if [ "$?" -ne 0 ] ; then
        FAIL=1
    fi
done

exit $FAIL
