-- src/infrastructure/repositories/query_templates/iseries/as400_popularity_codes.sql
-- AS400/Iseries query for popularity codes calculation
SELECT
    mf.SNSCHR AS "Number",
    mf.SDESCL AS "Description",
    ABS(SUM(pm.PMQTY)) AS "Sold",
    it.SCLSK AS "Stock",
    it.SALLOC AS "Allocated",
    it.SCLSK - it.SALLOC AS "Stock Less Allocated",
    mf.SRET1 AS "Jobber",
    ABS(SUM(pm.PMQTY) * mf.SRET1) AS "Revenue",
    SUM(pm.PMQTY * pm.PMCOST) AS "Cost"
FROM
    DSTDATA.INSMFH mf
LEFT JOIN DSTDATA.INPMOVE pm ON
    mf.SPART = pm.PMPART
LEFT JOIN DSTDATA.INSMFT it ON
    it.SPART = mf.SPART
WHERE
    -- Records only from the beginning of the year
    pm.PMDATE >= '{date}'
    -- Branch
    {nobranch}AND pm.PMBRAN = {branch}
    -- Filter out Assembly movements
    AND pm.PMDOC#<> 'ASMBLY'
    -- Only sales
    AND pm.PMTYPE = 'PSL'
    -- Ignore from account 28420
    AND pm.PMFRTO <> '28420'
    {nobranch}AND it.SBRAN = {branch}
GROUP BY
    mf.SNSCHR, mf.SRET1, mf.SDESCL, it.SCLSK, it.SALLOC
ORDER BY
    "Sold" DESC;