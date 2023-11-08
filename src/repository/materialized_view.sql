/* Gebruik de correlation.ipynb onder notebooks voor een heat map */

-- CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_view_ml AS
SELECT 
    Campagne.RedenVanStatus as campagne_statusreden,
    Campagne.Startdatum,
    Campagne.TypeCampagne,
    Campagne.SoortCampagne,

    Persoon.Marketingcommunicatie,
    Persoon.RedenVanStatus as persoon_statusreden,

    Inschrijving.AanwezigAfwezig,
    Inschrijving.Bron,
    
    Sessie.Activiteitstype,
    Sessie.Product,
    Sessie.ThemaNaam
FROM Campagne
JOIN Inschrijving ON Inschrijving.CampagneId = Campagne.CampagneId
JOIN SessieInschrijving ON SessieInschrijving.InschrijvingId = Inschrijving.InschrijvingId
JOIN Sessie ON Sessie.SessieId = SessieInschrijving.SessieId
JOIN Contactfiche on Contactfiche.ContactpersoonId = Inschrijving.ContactficheId
JOIN Persoon ON Persoon.PersoonId = Contactfiche.PersoonId