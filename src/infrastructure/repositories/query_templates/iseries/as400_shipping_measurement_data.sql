-- src/infrastructure/repositories/query_templates/iseries/as400_shipping_measurement_data.sql
-- AS400/Iseries query for shipping measurement validation
SELECT
    mf.SNSCHR AS "PartNumber",
    mf.SDESCL AS "Description",
    mf.SLEN1 AS "PackageLength",
    mf.SWIT1 AS "PackageWidth",
    mf.SHGT1 AS "PackageHeight",
    mf.SWGHT AS "ShippingWeight",
    mf.SRET1 AS "RetailPrice",
    -- Flag oversized packages
    CASE
        WHEN mf.SLEN1 > 108 OR mf.SWIT1 > 70 OR mf.SHGT1 > 70
        THEN 'OVERSIZED'
        WHEN mf.SWGHT > 150
        THEN 'OVERWEIGHT'
        ELSE 'STANDARD'
    END AS "ShippingCategory"
FROM DSTDATA.INSMFH mf
WHERE
    mf.SNSCHR IS NOT NULL
ORDER BY mf.SNSCHR;