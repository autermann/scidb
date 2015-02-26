########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2011 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the GNU General Public License for the complete license terms.
#
# You should have received a copy of the GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
#
# END_COPYRIGHT
########################################

#
# Finding docbook executable.
#
# Once done this will define:
#	DOCBOOK_XSL_FILE- Full path to docbook.xsl file 

include(FindPackageHandleStandardArgs)

# Ubuntu: /usr/share/xml/docbook/stylesheet/docbook-xsl/fo
# Red Hat 5.4: /usr/share/sgml/docbook/xsl-stylesheets-1.69.1-5.1/fo/
# Fedora 16: /usr/share/sgml/docbook/xsl-stylesheets-1.76.1/fo
# Fedora 17: /usr/share/sgml/docbook/xsl-stylesheets-1.76-1/fo
find_program(DOCBOOK_XSL_FILE docbook.xsl ${DOCBOOK_XSL_FILE_DIR}
  PATHS /usr/share/xml/docbook/stylesheet/docbook-xsl/fo
        /usr/share/sgml/docbook/xsl-stylesheets-1.69.1-5.1/fo
        /usr/share/sgml/docbook/xsl-stylesheets-1.76.1/fo
        /usr/share/sgml/docbook/xsl-stylesheets-1.76-1/fo)
find_package_handle_standard_args(docbook.xsl DEFAULT_MSG DOCBOOK_XSL_FILE)

mark_as_advanced(DOCBOOK_XSL_FILE)
