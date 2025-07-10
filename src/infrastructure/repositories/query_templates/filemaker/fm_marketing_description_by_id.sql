/*src/infrastructure/repositories/query_templates/filemaker/fm_marketing_description_by_id.sql
  Filemaker query for finding marketing description by terminology ID
 */
SELECT
    "md"."PartTerminologyID",
    "md"."Jeep",
    "md"."JeepResult",
    "md"."Validation",
    "md"."NonJeep",
    "md"."NonJeepResult",
    "md"."NonJeepValidation",
    "md"."ReviewNotes",
    "md"."PartTerminologyIDToBeAdded"
FROM "MarketingDescriptions" "md"
WHERE "md"."PartTerminologyID" = '{terminology_id}'