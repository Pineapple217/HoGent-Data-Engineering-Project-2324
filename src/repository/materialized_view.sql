/* Gebruik de correlation.ipynb onder notebooks voor een heat map */

-- CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_view_ml AS
SELECT 
    Campagne.CampagneId,
    Campagne.RedenVanStatus,
    Campagne.Startdatum,
    Campagne.TypeCampagne,
    Campagne.SoortCampagne,

    Inschrijving.InschrijvingId,
    Inschrijving.AanwezigAfwezig,
    Inschrijving.Bron,
    Inschrijving.DatumInschrijving,

    SessieInschrijving.SessieInschrijvingId,

    Sessie.SessieId,
    Sessie.Activiteitstype,
    Sessie.EindDatumTijd,
    Sessie.Product,
    Sessie.StartDatumTijd,
    Sessie.ThemaNaam
FROM Campagne
JOIN Inschrijving ON Inschrijving.CampagneId = Campagne.CampagneId
JOIN SessieInschrijving ON SessieInschrijving.InschrijvingId = Inschrijving.InschrijvingId
JOIN Sessie ON Sessie.SessieId = SessieInschrijving.SessieId