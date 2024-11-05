<!-- xslt will normalize the Sinopia Work RDFXML structure to the Alma Work RDFXML structure  -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:bflc="http://id.loc.gov/ontologies/bflc/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
  <xsl:output method="xml" indent="yes"/>

  <!-- Template to handle bf:Work elements that should be transformed into bf:Hub -->
  <xsl:template match="bf:Work[contains(@rdf:about, 'http://id.loc.gov/resources/hubs')]">
    <xsl:element name="bf:Hub">
      <xsl:attribute name="rdf:about">
        <xsl:value-of select="@rdf:about"/>
      </xsl:attribute>
      <xsl:apply-templates select="node()"/>
    </xsl:element>
  </xsl:template>

  <!-- Template to handle bf:Work elements -->
  <xsl:template match="bf:Work">
    <xsl:text disable-output-escaping="yes">&lt;bf:Work rdf:about="</xsl:text>
    <xsl:value-of select="@rdf:about"/>
    <xsl:text disable-output-escaping="yes">" xmlns:bf="http://id.loc.gov/ontologies/bibframe/"&gt;</xsl:text>
    <xsl:apply-templates select="node()"/>
    <xsl:text disable-output-escaping="yes">&lt;/bf:Work&gt;</xsl:text>
  </xsl:template>

  <!-- Template to handle bf:date elements and add rdf:datatype attribute -->
  <xsl:template match="bf:date">
    <xsl:element name="bf:date">
      <xsl:attribute name="rdf:datatype">http://id.loc.gov/datatypes/edtf</xsl:attribute>
      <xsl:apply-templates select="node()"/>
    </xsl:element>
  </xsl:template>

  <!-- Template to handle incorrect ns1:relationship elements and replace with bflc:relationship -->
  <xsl:template match="*[namespace-uri()='ttp://id.loc.gov/ontologies/bflc/' and local-name()='relationship']">
    <bflc:relationship>
      <xsl:apply-templates select="@*|node()"/>
    </bflc:relationship>
  </xsl:template>

  <!-- Identity template to copy all nodes and attributes by default -->
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>

  <!-- Root template to wrap content in the desired structure -->
  <xsl:template match="/*" priority="1">
    <xsl:element name="bib">
      <xsl:element name="record_format">lcbf_work</xsl:element>
      <xsl:element name="suppress_from_publishing">false</xsl:element>
      <xsl:element name="record">
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:bflc="http://id.loc.gov/ontologies/bflc/" xmlns:sinopia="http://sinopia.io/vocabulary/"  xmlns:owl="http://www.w3.org/2002/07/owl#" xmlns:skos="http://www.w3.org/2004/02/skos/core#" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:cc="http://creativecommons.org/ns#" xmlns:foaf="http://xmlns.com/foaf/0.1/">
          <xsl:apply-templates select="node()"/>
        </rdf:RDF>
      </xsl:element>
    </xsl:element>
  </xsl:template>
</xsl:stylesheet>