<!-- xslt will normalize the Sinopia Work RDFXML structure to the Alma Work RDFXML structure  -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <xsl:output method="xml" indent="yes"/>

  <!-- Template to handle bf:Work elements -->
  <xsl:template match="bf:Work">
    <xsl:element name="bf:Work">
      <xsl:attribute name="rdf:about">
        <xsl:value-of select="@rdf:about"/>
      </xsl:attribute>
      <xsl:apply-templates select="node()"/>
    </xsl:element>
  </xsl:template>

  <!-- Template to handle bf:date elements and add rdf:datatype attribute -->
  <xsl:template match="bf:date">
    <xsl:element name="bf:date">
      <xsl:attribute name="rdf:datatype">http://id.loc.gov/datatypes/edtf</xsl:attribute>
      <xsl:apply-templates select="node()"/>
    </xsl:element>
  </xsl:template>

  <!-- Identity template to copy all nodes and attributes by default -->
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>

  <!-- Root template to wrap content in the desired structure -->
  <xsl:template match="/*" priority="1">
    <bib>
      <record_format>lcbf_work</record_format>
      <suppress_from_publishing>false</suppress_from_publishing>
      <record>
        <rdf:RDF xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:sinopia="http://sinopia.io/vocabulary/" xmlns:bflc="http://id.loc.gov/ontologies/bflc/" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
          <xsl:apply-templates select="node()"/>
        </rdf:RDF>
      </record>
    </bib>
  </xsl:template>
</xsl:stylesheet>