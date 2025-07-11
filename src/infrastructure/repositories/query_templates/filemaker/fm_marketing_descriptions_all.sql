/*src/infrastructure/repositories/query_templates/filemaker/fm_marketing_descriptions_all.sql
  Filemaker query for all marketing descriptions with master data
 */
SELECT
    "m"."AS400_NumberStripped",
    "m"."SDC_PartTerminologyID",
    "md"."PartTerminologyID",
    "md"."Jeep",
    "md"."Validation",
    "md"."NonJeep",
    "md"."NonJeepValidation",
    "md"."ReviewNotes",
    "md"."PartTerminologyIDToBeAdded"
FROM "Master" "m"
LEFT JOIN "MarketingDescriptions" "md"
ON "m"."SDC_PartTerminologyID" = "md"."PartTerminologyID"
WHERE "m".ToggleActive='Yes'
ORDER BY "m"."SDC_PartTerminologyID"