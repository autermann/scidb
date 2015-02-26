#/bin/bash
# make the Ref Guide pdf
xsltproc --xinclude --output refbook.fo /usr/share/xml/docbook/stylesheet/docbook-xsl/custom.xsl scidb_ref_guide.xml
fop refbook.fo ../pdf/scidb_ref_guide.pdf

# make the user guide pdf
xsltproc --xinclude --output ugbook.fo /usr/share/xml/docbook/stylesheet/docbook-xsl/custom.xsl scidb_ug_snowdrop.xml
fop ugbook.fo ../pdf/scidb_ug_snowdrop.pdf
