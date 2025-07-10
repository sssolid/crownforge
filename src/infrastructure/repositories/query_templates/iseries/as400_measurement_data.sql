-- src/infrastructure/repositories/query_templates/iseries/as400_measurement_data.sql
-- AS400/Iseries query for measurement data
SELECT
    mf.SNSCHR AS "PartNumber",
    mf.SDESCL AS "Description",
    mf.SLEN1 AS "Length_AS400",
    mf.SWIT1 AS "Width_AS400",
    mf.SHGT1 AS "Height_AS400",
    mf.SWGHT AS "Weight_AS400"
FROM DSTDATA.INSMFH mf
WHERE
    mf.SNSCHR IS NOT NULL
ORDER BY mf.SNSCHR;