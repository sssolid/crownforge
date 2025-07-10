-- src/infrastructure/repositories/query_templates/filemaker/fm_kit_components.sql
-- Filemaker query for kit components data
SELECT
    "m"."AS400_NumberStripped" AS "Assembly",
    "m"."PartDescription" AS "AssemblyDescription",
    "m"."PartSoldAs" AS "SoldAs",
    "m"."PartBrand" AS "Brand"
FROM "Master" "m"
WHERE
    ToggleActive = 'Yes'
    AND PartSoldAs IN ('Kit', 'Set')
    {assembly_filter}
ORDER BY AS400_NumberStripped;