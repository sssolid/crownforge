SELECT
    "m"."AS400_NumberStripped" AS "PartNumber",
    "m"."AS400_UPC" AS "UPC",
    "m"."PartBrand",
    "m"."PartDescription"
FROM "Master" "m"
WHERE
    ToggleActive = 'Yes'
ORDER BY AS400_NumberStripped