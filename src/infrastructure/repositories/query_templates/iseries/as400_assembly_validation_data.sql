-- src/infrastructure/repositories/query_templates/iseries/as400_assembly_validation_data.sql
-- AS400/Iseries query for assembly validation data
SELECT
    b.ASASEM AS "Assembly",
    b.ASCOMP AS "Component",
    b.ASQTY AS "Quantity",
    b.ASUNIT AS "Unit",
    p1.SDESCL AS "AssemblyDescription",
    p2.SDESCL AS "ComponentDescription",
    p1.SRCOST AS "AssemblyCost",
    p2.SRCOST AS "ComponentCost",
    (b.ASQTY * p2.SRCOST) AS "ExtendedComponentCost"
FROM
    DSTDATA.BMASEM b
LEFT JOIN DSTDATA.INSMFH p1 ON b.ASASEM = (p1.SFRAN || p1.SPART)
LEFT JOIN DSTDATA.INSMFH p2 ON b.ASCOMP = (p2.SFRAN || p2.SPART)
WHERE
    b.ASASEM IS NOT NULL
    AND b.ASCOMP IS NOT NULL
ORDER BY
    b.ASASEM, b.ASCOMP;