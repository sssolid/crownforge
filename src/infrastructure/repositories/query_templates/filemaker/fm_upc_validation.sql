-- src/infrastructure/repositories/query_templates/filemaker/fm_upc_validation.sql
-- Filemaker query for UPC validation data
SELECT
    "m"."AS400_NumberStripped" AS "PartNumber",
    "m"."AS400_UPC" AS "UPC",
    "m"."PartBrand",
    "m"."PartDescription"
FROM "Master" "m"
WHERE
    ToggleActive = 'Yes'
ORDER BY AS400_NumberStripped;