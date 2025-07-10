-- src/infrastructure/repositories/query_templates/iseries/as400_kit_components_hierarchy.sql
-- AS400/Iseries query for kit components hierarchy with cost discrepancies
WITH RECURSIVE "ComponentHierarchy" ("Assembly", "Component", "Quantity", "Level") AS (
    -- Base case: Get all direct components of assemblies
    SELECT
        b.ASASEM AS "Assembly",
        b.ASCOMP AS "Component",
        b.ASQTY AS "Quantity",
        1 AS "Level"
    FROM
        DSTDATA.BMASEM b

    UNION ALL

    -- Recursive case: Get subcomponents of the components
    SELECT
        ch."Assembly",
        b.ASCOMP AS "Component",
        b.ASQTY AS "Quantity",
        ch."Level" + 1 AS "Level"
    FROM
        "ComponentHierarchy" ch
    JOIN
        DSTDATA.BMASEM b ON ch."Component" = b.ASASEM
    WHERE
        ch."Level" < 10  -- Prevent infinite recursion
),
-- Subquery to get the latest component cost based on ASDATE and ASTIME
"LatestComponentCost" AS (
    SELECT
        b1.ASCOMP,
        b1.ASCCST,
        b1.ASASEM,
        b1.ASDATE,
        b1.ASTIME
    FROM
        DSTDATA.BMASEMP b1
    INNER JOIN (
        SELECT ASCOMP, MAX(ASDATE) AS "MaxDate"
        FROM DSTDATA.BMASEMP
        GROUP BY ASCOMP
    ) "LATEST" ON b1.ASCOMP = "LATEST".ASCOMP AND b1.ASDATE = "LATEST"."MaxDate"
    INNER JOIN (
        SELECT ASCOMP, ASDATE, MAX(ASTIME) AS "MaxTime"
        FROM DSTDATA.BMASEMP
        GROUP BY ASCOMP, ASDATE
    ) "time_filter" ON b1.ASCOMP = "time_filter".ASCOMP AND b1.ASDATE = "time_filter".ASDATE AND b1.ASTIME = "time_filter"."MaxTime"
)
SELECT
    ch."Assembly",
    ch."Component",
    ch."Quantity",
    ch."Level",
    p.SRCOST AS "CostFromINSMFH",
    lcc.ASCCST AS "LatestComponentCost",
    (p.SRCOST - lcc.ASCCST) AS "CostDiscrepancy"
FROM
    "ComponentHierarchy" ch
LEFT JOIN
    DSTDATA.INSMFH p ON ch."Component" = (p.SFRAN || p.SPART)
LEFT JOIN
    "LatestComponentCost" lcc ON ch."Component" = lcc.ASCOMP
WHERE
    1=1
    {assembly_filter}
ORDER BY
    ch."Assembly", ch."Level";