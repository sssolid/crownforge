-- src/infrastructure/repositories/query_templates/iseries/as400_cost_validation_data.sql
-- AS400/Iseries query for cost validation data
SELECT
    mf.SNSCHR AS "PartNumber",
    mf.SDESCL AS "Description",
    mf.SRET1 AS "RetailPrice",
    mf.SRCOST AS "StandardCost",
    mf.SLCOST AS "LastCost",
    mf.SACOST AS "AverageCost",
    -- Calculate margin
    CASE
        WHEN mf.SRET1 > 0 AND mf.SRCOST > 0
        THEN ((mf.SRET1 - mf.SRCOST) / mf.SRET1) * 100
        ELSE 0
    END AS "MarginPercentage",
    -- Flag cost anomalies
    CASE
        WHEN mf.SRCOST <= 0 THEN 'ZERO_COST'
        WHEN mf.SRET1 <= mf.SRCOST THEN 'NEGATIVE_MARGIN'
        WHEN mf.SRET1 > (mf.SRCOST * 10) THEN 'HIGH_MARGIN'
        ELSE 'NORMAL'
    END AS "CostFlag"
FROM DSTDATA.INSMFH mf
WHERE
    mf.SNSCHR IS NOT NULL
ORDER BY mf.SNSCHR