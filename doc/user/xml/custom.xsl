<?xml version='1.0'?> 
<xsl:stylesheet  
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:fo="http://www.w3.org/1999/XSL/Format"
    version="1.0"> 

<xsl:import href="/usr/share/xml/docbook/stylesheet/docbook-xsl/fo/docbook.xsl"/> 

<xsl:template match="symbol[@role = 'symbolfont']">
  <fo:inline font-family="Symbol">
    <xsl:call-template name="inline.charseq"/>
  </fo:inline>
</xsl:template>

<xsl:template match="processing-instruction('linebreak')">
  <fo:block/>
</xsl:template>

<xsl:attribute-set name="admonition.properties">
 <xsl:attribute name="border">0.5pt solid black</xsl:attribute>
  <xsl:attribute name="padding">0.1in</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="admonition.title.properties">
  <xsl:attribute name="font-size">12pt</xsl:attribute>
  <xsl:attribute name="font-weight">bold</xsl:attribute>
  <xsl:attribute name="hyphenate">false</xsl:attribute>
  <xsl:attribute name="keep-with-next.within-column">always</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="formal.object.properties">
  <xsl:attribute name="space-before.minimum">0.5em</xsl:attribute>
  <xsl:attribute name="space-before.optimum">1em</xsl:attribute>
  <xsl:attribute name="space-before.maximum">2em</xsl:attribute>
  <xsl:attribute name="space-after.minimum">0.5em</xsl:attribute>
  <xsl:attribute name="space-after.optimum">1em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">2em</xsl:attribute>
  <xsl:attribute name="keep-together.within-column">auto</xsl:attribute>
</xsl:attribute-set>

<xsl:param name="generate.section.toc.level" select="1"/>

<xsl:attribute-set name="monospace.verbatim.properties" use-attribute-sets="verbatim.properties monospace.properties">
<xsl:attribute name="background-color">#E0E0E0</xsl:attribute>
  <xsl:attribute name="text-align">start</xsl:attribute>
  <xsl:attribute name="wrap-option">no-wrap</xsl:attribute>
</xsl:attribute-set>

<xsl:param name="section.autolabel" select="1"/>

<xsl:param name="section.label.includes.component.label" select="1"/>


</xsl:stylesheet>  
