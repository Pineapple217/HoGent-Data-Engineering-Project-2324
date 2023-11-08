/* Gebruik de correlation.ipynb onder notebooks voor een heat map */

-- CREATE VIEW materialized_view_ml AS
WITH ThemasPerPersoon AS (
    SELECT 
        Persoon.PersoonId,
        CAST(ThemaDuurzaamheid AS INT) AS ThemaDuurzaamheid,
        CAST(ThemaFinancieelFiscaal AS INT) AS ThemaFinancieelFiscaal,
        CAST(ThemaInnovatie AS INT) AS ThemaInnovatie,
        CAST(ThemaInternationaalOndernemen AS INT) AS ThemaInternationaalOndernemen,
        CAST(ThemaMobiliteit AS INT) AS ThemaMobiliteit,
        CAST(ThemaOmgeving AS INT) AS ThemaOmgeving,
        CAST(ThemaSalesMarketingCommunicatie AS INT) AS ThemaSalesMarketingCommunicatie,
        CAST(ThemaStrategieEnAlgemeenManagement AS INT) AS ThemaStrategieEnAlgemeenManagement,
        CAST(ThemaTalent AS INT) AS ThemaTalent,
        CAST(ThemaWelzijn AS INT) AS ThemaWelzijn
    FROM Persoon
)
SELECT * 
FROM ThemasPerPersoon;