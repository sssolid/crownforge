SELECT
    "m"."AS400_NumberStripped",
    "m"."PartBrand",
    "m"."PartTertiaryCategory",
    "m"."PartDescription"
FROM "Master" "m"
WHERE ToggleActive='Yes'
ORDER BY AS400_NumberStripped