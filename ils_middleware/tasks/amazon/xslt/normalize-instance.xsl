<!-- XSLT to normalize the Sinopia Instance RDF/XML structure to the Alma Instance RDF/XML structure -->
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:sinopiabf="http://sinopia.io/vocabulary/bf/"
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:bf="http://id.loc.gov/ontologies/bibframe/"
    exclude-result-prefixes="sinopiabf rdf">

  <xsl:output method="xml" indent="yes"/>

  <!-- Template to handle bf:instance elements -->
  <xsl:template match="bf:Instance">
    <xsl:text disable-output-escaping="yes">&lt;bf:Instance rdf:about="</xsl:text>
    <xsl:value-of select="@rdf:about"/>
    <xsl:text disable-output-escaping="yes">" xmlns:bf="http://id.loc.gov/ontologies/bibframe/"&gt;</xsl:text>
    <xsl:apply-templates select="node()"/>
    <xsl:text disable-output-escaping="yes">&lt;/bf:Instance&gt;</xsl:text>
  </xsl:template>
  
  <!-- Copy all other elements and attributes -->
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>

  <!-- Template to handle sinopiabf:nonfiling elements -->
  <xsl:template match="sinopiabf:nonfiling">
    <bflc:nonSortNum xmlns:bflc="http://id.loc.gov/ontologies/bflc/">
      <xsl:value-of select="."/>
    </bflc:nonSortNum>
  </xsl:template>

  <!-- Ensure edtf datatype URIs are correctly formatted -->
  <xsl:template match="bf:date/@rdf:datatype" xmlns:bf="http://id.loc.gov/ontologies/bibframe/">
    <xsl:attribute name="rdf:datatype">
      <xsl:choose>
        <xsl:when test="contains(., 'http://id.loc.gov/datatypes/edtf/')">
          <xsl:value-of select="concat('http://id.loc.gov/datatypes/edtf', substring-after(., 'http://id.loc.gov/datatypes/edtf/'))"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="."/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:attribute>
  </xsl:template>

  <!-- Template to handle bf:date elements and add rdf:datatype attribute -->
  <xsl:template match="bf:date" xmlns:bf="http://id.loc.gov/ontologies/bibframe/">
    <xsl:element name="bf:date">
      <xsl:attribute name="rdf:datatype">http://id.loc.gov/datatypes/edtf</xsl:attribute>
      <xsl:apply-templates select="node()"/>
    </xsl:element>
  </xsl:template>

  <!-- Root template to wrap content in the desired structure -->
  <xsl:template match="/*" priority="1">
    <xsl:element name="bib">
      <xsl:element name="record_format">lcbf_instance</xsl:element>
      <xsl:element name="suppress_from_publishing">false</xsl:element>
      <xsl:element name="record">
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" xmlns:bf="http://id.loc.gov/ontologies/bibframe/" xmlns:bflc="http://id.loc.gov/ontologies/bflc/" xmlns:sinopia="http://sinopia.io/vocabulary/"  xmlns:owl="http://www.w3.org/2002/07/owl#" xmlns:skos="http://www.w3.org/2004/02/skos/core#" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:cc="http://creativecommons.org/ns#" xmlns:foaf="http://xmlns.com/foaf/0.1/">
          <xsl:apply-templates select="node()"/>
        </rdf:RDF>
      </xsl:element>
    </xsl:element>
  </xsl:template>

</xsl:stylesheet>