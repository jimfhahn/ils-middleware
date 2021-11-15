date_of_publication = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?date
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:provisionActivity ?activity .
    ?activity a bf:Publication .
    ?activity bf:date ?date .
}}
"""

identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?identifier
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:identifiedBy ?ident_bnode .
    ?ident_bnode a {bf_class} .
    ?ident_bnode rdf:value ?identifier .
}}
"""

instance_format_category = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?format_category
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:media ?format_category .
}}
"""

instance_format_term = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?format_term
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:carrier ?format_term .
}}
"""

local_identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?identifier
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:identifiedBy ?ident_bnode .
    ?ident_bnode a bf:Local .
    ?ident_bnode bf:source ?source_bnode .
    ?ident_bnode rdf:value ?identifier .
    ?source_bnode a bf:Source .
    OPTIONAL {{
        ?source_bnode rdfs:label "OColC" .
    }}
    OPTIONAL {{
        ?source_bnode rdfs:label "OCLC" .
    }}
}}
"""

mode_of_issuance = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SElECT ?mode_of_issuance
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:issuance ?mode_of_issuance .
}}
"""

note = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?note
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:note ?note_bnode .
    ?note_bnode a bf:Note .
    ?note_bnode rdfs:label ?note .
}}
"""

physical_description_dimensions = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?dimensions
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:dimensions ?dimensions .
}}
"""

physical_description_extent = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?extent
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:extent ?extent_bnode .
    ?extent_bnode a bf:Extent .
    ?extent_bnode rdfs:label ?extent .
}}
"""

place = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?place
WHERE {{
   <{bf_instance}> a bf:Instance .
   <{bf_instance}> bf:provisionActivity ?activity .
   ?activity a bf:Publication .
   ?activity bf:place ?place_holder .
   ?place_holder rdfs:label ?place .
}}
"""

publisher = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?publisher
WHERE {{
   <{bf_instance}> a bf:Instance .
   <{bf_instance}> bf:provisionActivity ?activity .
   ?activity a bf:Publication .
   ?activity bf:agent ?agent .
   ?agent a bf:Agent .
   ?agent rdfs:label ?publisher .
}}
"""

title = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?main_title ?subtitle ?part_number ?part_name
WHERE {{
  <{bf_instance}> a bf:Instance .
  <{bf_instance}> bf:title ?title .
  ?title a {bf_class} .
  ?title bf:mainTitle ?main_title .
  OPTIONAL {{
     ?title bf:subtitle ?subtitle .
  }}
  OPTIONAL {{
     ?title bf:partNumber ?part_number .
  }}
  OPTIONAL {{
     ?title bf:partName ?part_name
  }}
}}
"""
