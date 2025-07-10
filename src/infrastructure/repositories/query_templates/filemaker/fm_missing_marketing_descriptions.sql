-- src/infrastructure/repositories/query_templates/filemaker/fm_missing_marketing_descriptions.sql
-- Filemaker query for finding missing marketing descriptions
SELECT DISTINCT "m"."SDC_PartTerminologyID"
FROM "Master" "m"
LEFT JOIN "MarketingDescriptions" "md"
ON "m"."SDC_PartTerminologyID" = "md"."PartTerminologyID"
WHERE "m".ToggleActive='Yes'
AND "md"."PartTerminologyID" IS NULL
AND "m"."SDC_PartTerminologyID" IS NOT NULL
AND "m"."SDC_PartTerminologyID" != ''
ORDER BY "m"."SDC_PartTerminologyID";