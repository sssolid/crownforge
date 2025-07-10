-- src/infrastructure/repositories/query_templates/iseries/as400_stock_data.sql
-- AS400/Iseries query for stock data
SELECT
    mf.SNSCHR,
    mf.SDESCL,
    it.SCLSK AS "Stock",
    it.SALLOC AS "Allocated",
    it.SCLSK - it.SALLOC AS "Stock Less Allocated",
    mf.SRET1
FROM DSTDATA.INSMFH mf
LEFT JOIN DSTDATA.INSMFT it ON
    it.SPART = mf.SPART
WHERE it.SBRAN = {branch}
GROUP BY
    mf.SNSCHR, mf.SDESCL, it.SCLSK, it.SALLOC, mf.SRET1;