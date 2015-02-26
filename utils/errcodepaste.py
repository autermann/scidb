#!/usr/bin/python

import os
import sys
import re

if len(sys.argv) not in (3, 4):
    print "Usage: python " + sys.argv[0] + " [-s] <constant name (without SCIDB_E_ prefix))> <error message>"
    print "Flag -s is for adding short error messages"
    exit(1)

long_error = True
script_dir = os.path.dirname(os.getcwd() + '/' + sys.argv[0])
error_codes_path = script_dir + '/../include/system/ErrorCodes.h'
long_error_msg_path = script_dir + '/../src/system/LongErrorsList.h'
short_error_msg_path = script_dir + '/../src/system/ShortErrorsList.h'
align_len = 52

const_arg = True
for arg in sys.argv[1:]:
    if arg == '-s':
        long_error = False
    else:
        if const_arg:
            err_const = arg
            const_arg = False
        else:
            err_msg = arg

error_msg_path = long_error_msg_path if long_error else short_error_msg_path
error_code_placeholder = "Next " + ("long" if long_error else "short") + " ERRCODE"

print "Error codes file '" + error_codes_path + "'"
print "Error messages file '" + error_msg_path + "'"
print "Error code placeholder '" + error_code_placeholder + "'"

try:
    f = open(error_codes_path, 'r')
    codes_string = f.read()
    f.close()

    f = open(error_msg_path, 'r')
    msg_string = f.read()
    f.close()
except:
    print "Can not read file '" + error_codes_path + "'"
    exit(1)

res = re.search('.*ERRCODE\(.*,[\s]*([0-9]*)\).*\n.*//' + error_code_placeholder + '.*', codes_string)

if not res:
    print "Can not obtain number of last error code"
    exit(1)

err_code = int(res.group(1))

print "Last error code is " + str(err_code) + " next will be " + str(err_code + 1)

err_const = ("SCIDB_LE_" if long_error else "SCIDB_SE_") + err_const
err_code_define = "ERRCODE(" + err_const + ","
err_code_define = err_code_define + (' ' * (align_len - len(err_code_define))) + str(err_code + 1) + "); //" + err_msg

msg_define = "ERRMSG(" + err_const + ","
msg_define = msg_define + (' ' * (align_len - len(msg_define))) + '"' + err_msg + '"' + ");";

try:
    print "Inserting '" + err_code_define + "' into '" + error_codes_path + "'"

    f = open(error_codes_path, 'w')
    f.write(re.sub(r'(.*).*(?=//' + error_code_placeholder + '.*)', err_code_define + "\n", codes_string))
    f.close()

    print "Inserting '" + msg_define + "' into '" + error_msg_path + "'"
    f = open(error_msg_path, 'w')
    f.write(re.sub(r'(.*).*(?=//Next ERRMSG.*)', msg_define + "\n", msg_string))
    f.close()
except:
    print "Can not frite file '" + error_codes_path + "'"
    exit(1)

