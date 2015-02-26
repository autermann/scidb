#/bin/bash
# make the Quick Start Guide
xsltproc --xinclude --output refbook.fo ./custom.xsl quickStart_post.xml
fop -c fopconfig.xml refbook.fo ../pdf/Quick-Start-Guide-13.3.pdf
