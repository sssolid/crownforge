/* src/infrastructure/repositories/query_templates/filemaker/fm_measurement_validation.sql
   Filemaker query for measurement validation data
 */
SELECT
    "m"."AS400_NumberStripped" AS "PartNumber",
    "m"."PartDescription",
    "m"."AS400_Length" AS "Length_FM",
    "m"."AS400_Width" AS "Width_FM",
    "m"."AS400_Height" AS "Height_FM",
    "m"."AS400_Weight" AS "Weight_FM",
    "m"."PartDimensionalWeight" AS "DimWeight_FM"
FROM "Master" "m"
WHERE
    ToggleActive = 'Yes'
ORDER BY AS400_NumberStripped