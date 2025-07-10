SELECT
    "i"."ICPCD",
    "i"."ICPNO",
    "i"."IPTNO"
FROM "as400_ininter" AS "i"
ORDER BY "i"."IPTNO", "i"."ICPCD"