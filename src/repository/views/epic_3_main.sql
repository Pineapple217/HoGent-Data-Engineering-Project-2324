CREATE VIEW epic_3_main
AS
SELECT c.ContactPersoonId, i.CampagneId, i.CampagneNaam, ca.Startdatum, a.Ondernemingsaard, a.Ondernemingstype, a.PrimaireActiviteit, f.Naam as Functie
FROM dbo.Contactfiche c
JOIN dbo.Account a ON a.AccountId = c.AccountId
JOIN dbo.Inschrijving i ON i.ContactficheId = c.ContactPersoonId
JOIN dbo.Campagne ca ON ca.CampagneId = i.CampagneId
JOIN dbo.ContactficheFunctie cf ON cf.ContactpersoonId = c.ContactPersoonId
JOIN dbo.Functie f ON f.FunctieId = cf.FunctieId
WHERE i.CampagneId IS NOT NULL;
