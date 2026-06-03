/*
  File: create_bitcoin_addr_graph_opencypher_README.cypher
  Purpose: Explain why a pure openCypher schema-creation file cannot create
           a NebulaGraph graph space, tags, or edge types.

  Important:
  NebulaGraph supports openCypher-style query syntax mainly for graph querying.
  However, NebulaGraph schema objects are created with nGQL statements:

    CREATE SPACE
    CREATE TAG
    CREATE EDGE
    CREATE TAG INDEX
    CREATE EDGE INDEX

  Pure openCypher does not provide NebulaGraph-compatible commands to create:

    - graph space: bitcoin_addr_graph
    - tag: address
    - tag: tx
    - edge type: input_to_tx
    - edge type: tx_to_output

  Therefore, this file is NOT a replacement for the executable nGQL schema file:

    create_bitcoin_addr_graph.ngql

  Correct production process:

    1. Execute create_bitcoin_addr_graph.ngql to create the NebulaGraph schema.
    2. Load data with the nGQL ETL script, or test openCypher writes only if your
       NebulaGraph version supports the required write clauses.
    3. Use openCypher mainly for MATCH/path queries.

  Final production data model:

    (:address)-[:input_to_tx]->(:tx)-[:tx_to_output]->(:address)

  Meaning:

    input_to_tx:
      An address appears on the input side of a transaction.

    tx_to_output:
      A transaction creates an output to an address.

    One address hop:
      Address -> input_to_tx -> Tx -> tx_to_output -> Address
*/

/* --------------------------------------------------------------------------
   OpenCypher verification queries after the nGQL schema and ETL are complete
   -------------------------------------------------------------------------- */

/* Count address vertices. */
MATCH (a:address)
RETURN count(a) AS address_count;

/* Count transaction vertices. */
MATCH (t:tx)
RETURN count(t) AS tx_count;

/* Count input bridge edges. */
MATCH (a:address)-[e:input_to_tx]->(t:tx)
RETURN count(e) AS input_edge_count;

/* Count output bridge edges. */
MATCH (t:tx)-[e:tx_to_output]->(a:address)
RETURN count(e) AS output_edge_count;

/* One address-hop query template.
   Replace addr:SOURCE_ADDRESS with the real Nebula VID.
*/
MATCH (src:address)-[i:input_to_tx]->(t:tx)-[o:tx_to_output]->(dst:address)
WHERE id(src) == "addr:SOURCE_ADDRESS"
RETURN
  id(src) AS source_address_vid,
  id(t) AS tx_vid,
  id(dst) AS destination_address_vid,
  i.value AS input_value,
  o.value AS output_value,
  t.block_height AS block_height
LIMIT 100;

/* Shortest path query template.
   Replace the two VIDs with real address VIDs.
*/
MATCH p = shortestPath(
  (src:address)-[*..6]-(dst:address)
)
WHERE id(src) == "addr:SOURCE_ADDRESS"
  AND id(dst) == "addr:DESTINATION_ADDRESS"
RETURN p
LIMIT 10;
