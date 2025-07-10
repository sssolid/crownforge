-- src/infrastructure/repositories/query_templates/filemaker/fm_application_data_active.sql
-- Filemaker query for active application data processing
SELECT
    "m"."AS400_NumberStripped",
    "m"."PartApplication",
    "m"."PartNotes_NEW",
    "m"."PartNotesExtra",
    "m"."PartNotes"
FROM "Master" "m"
WHERE ToggleActive='Yes'
ORDER BY AS400_NumberStripped;