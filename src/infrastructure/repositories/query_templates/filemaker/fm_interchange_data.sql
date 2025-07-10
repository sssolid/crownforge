/* src/infrastructure/repositories/query_templates/filemaker/fm_interchange_data.sql
   Filemaker query for interchange data
 */
SELECT
    "i"."ICPCD",
    "i"."ICPNO",
    "i"."IPTNO"
FROM "as400_ininter" AS "i"
ORDER BY "i"."IPTNO", "i"."ICPCD"