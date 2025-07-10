-- src/infrastructure/repositories/query_templates/iseries/as400_dimensional_weight_data.sql
-- AS400/Iseries query for dimensional weight calculation data
SELECT
    mf.SNSCHR AS "PartNumber",
    mf.SDESCL AS "Description",
    mf.SLEN1 AS "Length",
    mf.SWIT1 AS "Width",
    mf.SHGT1 AS "Height",
    mf.SWGHT AS "Weight",
    -- Calculate dimensional weight (Length * Width * Height / 166)
    CASE
        WHEN mf.SLEN1 > 0 AND mf.SWIT1 > 0 AND mf.SHGT1 > 0
        THEN (mf.SLEN1 * mf.SWIT1 * mf.SHGT1) / 166.0
        ELSE 0
    END AS "CalculatedDimensionalWeight"
FROM DSTDATA.INSMFH mf
WHERE
    mf.SNSCHR IS NOT NULL
    AND (mf.SLEN1 > 0 OR mf.SWIT1 > 0 OR mf.SHGT1 > 0 OR mf.SWGHT > 0)
ORDER BY mf.SNSCHR;