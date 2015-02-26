# Remember to remove the extern directory from local workspace.

# Not including R suffixes.
find_files_missing_cr() {
    suffixes="cpp h i"
    for s in $suffixes; do 
#    echo $s
	find . -name "*.$s" -print | xargs egrep -L BEGIN_COPYRIGHT
    done    
}

add_copyright() {
    local fname=$(basename $1)
    rm -f /tmp/$fname.tmp
    cat license.txt $1 >> /tmp/$fname.tmp
    mv /tmp/$fname.tmp $1
    diff $1 /tmp/$fname > /tmp/d
    diff license.txt /tmp/d
}

find_files_old_cr() {
    suffixes="cpp h i"
    for s in $suffixes; do 
	find . -name "*.$s" -print | xargs egrep -l "2008-2011 SciDB, Inc."
    done
}

# Place the copyright diff (remove context filename, only linenumber)
change_copyright() {
    local fname=$(basename $1)
    rm -f /tmp/${fname}
    cp $1 /tmp/${fname}
    cd /tmp
    patch -p0 $fname < p2 
    cd -
    cp /tmp/${fname} $1
#    rm /tmp/${fname}
}

files=`find_files_missing_cr`
for f in $files; do 
    echo "Add copyright to $f"
    add_copyright $f
    echo "Done"
done


files2=`find_files_old_cr`
for g in $files2; do 
    echo "Change copyright in $g"
    change_copyright $g
    echo "Done"
done
