SELECT
    "m"."AS400_NumberStripped",
    "m"."PartApplication",
    "m"."PartNotes_NEW",
    "m"."PartNotesExtra",
    "m"."PartNotes"
FROM "Master" "m"
WHERE ToggleActive='Yes'
ORDER BY AS400_NumberStripped