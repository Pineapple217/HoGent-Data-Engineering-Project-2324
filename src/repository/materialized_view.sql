/* Gebruik de correlation.ipynb onder notebooks voor een heat map */

-- CREATE VIEW materialized_view_ml AS
WITH ThemasPerPersoon AS (
    SELECT 
        PersoonId,
        ThemaDuurzaamheid,
        ThemaFinancieelFiscaal,
        ThemaInnovatie,
        ThemaInternationaalOndernemen,
        ThemaMobiliteit,
        ThemaOmgeving,
        ThemaSalesMarketingCommunicatie,
        ThemaStrategieEnAlgemeenManagement,
        ThemaTalent,
        ThemaWelzijn
    FROM Persoon
)
SELECT * 
FROM ThemasPerPersoon;