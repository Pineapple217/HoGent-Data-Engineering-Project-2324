/****** Script for SelectTopNRows command from SSMS  ******/
use voka; 
SELECT [SessieId]
      ,[Activiteitstype]
      ,[EindDatumTijd]
      ,[Product]
      ,[SessieNr]
      ,[StartDatumTijd]
      ,[ThemaNaam]
      ,[CampagneId]
  FROM [voka].[dbo].[Sessie]
  where CampagneId is not null;

  select distinct ThemaNaam
  from dbo.Sessie
  where CampagneId is not null;

  -- aantal inschrijvingen en themas per campagne type
  select 
  distinct c.TypeCampagne, 
  count(i.ContactficheId) as aantalInschrijvingen, 
  SUM(cast(p.ThemaDuurzaamheid as int)) as countDuurzaamheid,
  SUM(cast(p.ThemaInnovatie as int)) as ThemaInnovatie,
  SUM(cast(p.ThemaInternationaalOndernemen as int)) as ThemaInternationaalOndernemen,
  SUM(cast(p.ThemaMobiliteit as int)) as ThemaMobiliteit,
  SUM(cast(p.ThemaOmgeving as int)) as ThemaOmgeving,
  SUM(cast(p.ThemaSalesMarketingCommunicatie as int)) as ThemaSalesMarketingCommunicatie,
  SUM(cast(p.ThemaStrategieEnAlgemeenManagement as int)) as ThemaStrategieEnAlgemeenManagement,
  SUM(cast(p.ThemaTalent as int)) as ThemaTalent,
  SUM(cast(p.ThemaWelzijn as int)) as ThemaWelzijn,
  SUM(cast(p.ThemaFinancieelFiscaal as int)) as ThemaFinancieelFiscaal

  from Campagne c
  left join Inschrijving i on c.CampagneId = i.CampagneId
  left join Contactfiche cf on i.ContactficheId = cf.ContactpersoonId
  left join Persoon p on cf.PersoonId = p.PersoonId
  group by c.TypeCampagne;

  -- type campagne per sessiethema: aantal inschrijvingen en aantallen per thema persoon
  select 
  distinct s.ThemaNaam, 
  c.TypeCampagne,
  count(i.ContactficheId) as aantalInschrijvingen, 
  SUM(cast(p.ThemaDuurzaamheid as int)) as ThemaDuurzaamheid,
  SUM(cast(p.ThemaInnovatie as int)) as ThemaInnovatie,
  SUM(cast(p.ThemaInternationaalOndernemen as int)) as ThemaInternationaalOndernemen,
  SUM(cast(p.ThemaMobiliteit as int)) as ThemaMobiliteit,
  SUM(cast(p.ThemaOmgeving as int)) as ThemaOmgeving,
  SUM(cast(p.ThemaSalesMarketingCommunicatie as int)) as ThemaSalesMarketingCommunicatie,
  SUM(cast(p.ThemaStrategieEnAlgemeenManagement as int)) as ThemaStrategieEnAlgemeenManagement,
  SUM(cast(p.ThemaTalent as int)) as ThemaTalent,
  SUM(cast(p.ThemaWelzijn as int)) as ThemaWelzijn,
  SUM(cast(p.ThemaFinancieelFiscaal as int)) as ThemaFinancieelFiscaal

  from Inschrijving i 
  left join Campagne c on c.CampagneId = i.CampagneId
  left join SessieInschrijving si on i.InschrijvingId = si.InschrijvingId
  left join Sessie s on si.SessieId = s.SessieId
  left join Contactfiche cf on i.ContactficheId = cf.ContactpersoonId
  left join Persoon p on cf.PersoonId = p.PersoonId
  where s.ThemaNaam is not null and c.TypeCampagne is not null
  group by s.ThemaNaam, c.TypeCampagne
  order by 1, 3 desc


-- percentages

select 
  s.ThemaNaam, 
  c.TypeCampagne,
  count(i.ContactficheId) as aantalInschrijvingen, 
  ROUND(SUM(cast(p.ThemaDuurzaamheid as float)) / count(i.ContactficheId), 2) as PercentageThemaDuurzaamheid,
  ROUND(SUM(cast(p.ThemaInnovatie as float)) / count(i.ContactficheId), 2) as PercentageThemaInnovatie,
  ROUND(SUM(cast(p.ThemaInternationaalOndernemen as float)) / count(i.ContactficheId), 2) as PercentageThemaInternationaalOndernemen,
  ROUND(SUM(cast(p.ThemaMobiliteit as float)) / count(i.ContactficheId), 2) as PercentageThemaMobiliteit,
  ROUND(SUM(cast(p.ThemaOmgeving as float)) / count(i.ContactficheId), 2) as PercentageThemaOmgeving,
  ROUND(SUM(cast(p.ThemaSalesMarketingCommunicatie as float)) / count(i.ContactficheId), 2) as PercentageThemaSalesMarketingCommunicatie,
  ROUND(SUM(cast(p.ThemaStrategieEnAlgemeenManagement as float)) / count(i.ContactficheId), 2) as PercentageThemaStrategieEnAlgemeenManagement,
  ROUND(SUM(cast(p.ThemaTalent as float)) / count(i.ContactficheId), 2) as PercentageThemaTalent,
  ROUND(SUM(cast(p.ThemaWelzijn as float)) / count(i.ContactficheId), 2) as PercentageThemaWelzijn,
  ROUND(SUM(cast(p.ThemaFinancieelFiscaal as float)) / count(i.ContactficheId), 2) as PercentageThemaFinancieelFiscaal

from Inschrijving i 
left join Campagne c on c.CampagneId = i.CampagneId
left join SessieInschrijving si on i.InschrijvingId = si.InschrijvingId
left join Sessie s on si.SessieId = s.SessieId
left join Contactfiche cf on i.ContactficheId = cf.ContactpersoonId
left join Persoon p on cf.PersoonId = p.PersoonId
where s.ThemaNaam is not null and c.TypeCampagne is not null
group by s.ThemaNaam, c.TypeCampagne
order by s.ThemaNaam, aantalInschrijvingen desc



    -- type sessiethema per campagne: aantal inschrijvingen en aantallen per thema persoon
  select 
  distinct c.TypeCampagne,
  s.ThemaNaam,
  count(i.ContactficheId) as aantalInschrijvingen, 
  SUM(cast(p.ThemaDuurzaamheid as int)) as ThemaDuurzaamheid,
  SUM(cast(p.ThemaInnovatie as int)) as ThemaInnovatie,
  SUM(cast(p.ThemaInternationaalOndernemen as int)) as ThemaInternationaalOndernemen,
  SUM(cast(p.ThemaMobiliteit as int)) as ThemaMobiliteit,
  SUM(cast(p.ThemaOmgeving as int)) as ThemaOmgeving,
  SUM(cast(p.ThemaSalesMarketingCommunicatie as int)) as ThemaSalesMarketingCommunicatie,
  SUM(cast(p.ThemaStrategieEnAlgemeenManagement as int)) as ThemaStrategieEnAlgemeenManagement,
  SUM(cast(p.ThemaTalent as int)) as ThemaTalent,
  SUM(cast(p.ThemaWelzijn as int)) as ThemaWelzijn,
  SUM(cast(p.ThemaFinancieelFiscaal as int)) as ThemaFinancieelFiscaal

  from Inschrijving i 
  left join Campagne c on c.CampagneId = i.CampagneId
  left join SessieInschrijving si on i.InschrijvingId = si.InschrijvingId
  left join Sessie s on si.SessieId = s.SessieId
  left join Contactfiche cf on i.ContactficheId = cf.ContactpersoonId
  left join Persoon p on cf.PersoonId = p.PersoonId
  where s.ThemaNaam is not null and c.TypeCampagne is not null 
  group by s.ThemaNaam, c.TypeCampagne
  having count(i.ContactficheId) > 10
  order by 1, 3 desc;


  -- df met id
  select 
  distinct c.TypeCampagne,
  s.ThemaNaam,
  p.PersoonId,
  cast(p.ThemaDuurzaamheid as int) as ThemaDuurzaamheid,
  cast(p.ThemaInnovatie as int) as ThemaInnovatie,
  cast(p.ThemaInternationaalOndernemen as int) as ThemaInternationaalOndernemen,
  cast(p.ThemaMobiliteit as int) as ThemaMobiliteit,
  cast(p.ThemaOmgeving as int) as ThemaOmgeving,
  cast(p.ThemaSalesMarketingCommunicatie as int) as ThemaSalesMarketingCommunicatie,
  cast(p.ThemaStrategieEnAlgemeenManagement as int) as ThemaStrategieEnAlgemeenManagement,
  cast(p.ThemaTalent as int) as ThemaTalent,
  cast(p.ThemaWelzijn as int) as ThemaWelzijn,
  cast(p.ThemaFinancieelFiscaal as int) as ThemaFinancieelFiscaal

  from Inschrijving i 
  left join Campagne c on c.CampagneId = i.CampagneId
  left join SessieInschrijving si on i.InschrijvingId = si.InschrijvingId
  left join Sessie s on si.SessieId = s.SessieId
  left join Contactfiche cf on i.ContactficheId = cf.ContactpersoonId
  left join Persoon p on cf.PersoonId = p.PersoonId
  where s.ThemaNaam is not null and c.TypeCampagne is not null and p.PersoonId is not null
  order by 1, 2

  -- bovenstaande is fout, je moet sws alle personen hebben
    -- df met id correct
  select 
  p.PersoonId,
  s.ThemaNaam,
  c.TypeCampagne,
  cast(p.ThemaDuurzaamheid as int) as ThemaDuurzaamheid,
  cast(p.ThemaInnovatie as int) as ThemaInnovatie,
  cast(p.ThemaInternationaalOndernemen as int) as ThemaInternationaalOndernemen,
  cast(p.ThemaMobiliteit as int) as ThemaMobiliteit,
  cast(p.ThemaOmgeving as int) as ThemaOmgeving,
  cast(p.ThemaSalesMarketingCommunicatie as int) as ThemaSalesMarketingCommunicatie,
  cast(p.ThemaStrategieEnAlgemeenManagement as int) as ThemaStrategieEnAlgemeenManagement,
  cast(p.ThemaTalent as int) as ThemaTalent,
  cast(p.ThemaWelzijn as int) as ThemaWelzijn,
  cast(p.ThemaFinancieelFiscaal as int) as ThemaFinancieelFiscaal

  from Persoon p 
  left join Contactfiche cf on p.PersoonId = cf.PersoonId
  left join Inschrijving i on cf.ContactpersoonId = i.ContactficheId
  left join SessieInschrijving si on i.InschrijvingId = si.InschrijvingId
  left join Sessie s on si.SessieId = s.SessieId
  left join Campagne c on c.CampagneId = i.CampagneId



      -- persoonId toegevoegd, heel uitgebreid
  with totalen as (
  select 
  distinct c.TypeCampagne,
  s.ThemaNaam,
  count(i.ContactficheId) as aantalInschrijvingen, 
  SUM(cast(p.ThemaDuurzaamheid as int)) as ThemaDuurzaamheid,
  SUM(cast(p.ThemaInnovatie as int)) as ThemaInnovatie,
  SUM(cast(p.ThemaInternationaalOndernemen as int)) as ThemaInternationaalOndernemen,
  SUM(cast(p.ThemaMobiliteit as int)) as ThemaMobiliteit,
  SUM(cast(p.ThemaOmgeving as int)) as ThemaOmgeving,
  SUM(cast(p.ThemaSalesMarketingCommunicatie as int)) as ThemaSalesMarketingCommunicatie,
  SUM(cast(p.ThemaStrategieEnAlgemeenManagement as int)) as ThemaStrategieEnAlgemeenManagement,
  SUM(cast(p.ThemaTalent as int)) as ThemaTalent,
  SUM(cast(p.ThemaWelzijn as int)) as ThemaWelzijn,
  SUM(cast(p.ThemaFinancieelFiscaal as int)) as ThemaFinancieelFiscaal
  from Inschrijving i 
  left join Campagne c on c.CampagneId = i.CampagneId
  left join SessieInschrijving si on i.InschrijvingId = si.InschrijvingId
  left join Sessie s on si.SessieId = s.SessieId
  left join Contactfiche cf on i.ContactficheId = cf.ContactpersoonId
  left join Persoon p on cf.PersoonId = p.PersoonId
  where s.ThemaNaam is not null and c.TypeCampagne is not null 
  group by s.ThemaNaam, c.TypeCampagne
  having count(i.ContactficheId) > 10
  )
  select 
  distinct c.TypeCampagne,
  s.ThemaNaam,
  t.aantalInschrijvingen,
  t.ThemaDuurzaamheid as aantalThemaDuurzaamheid,
  t.ThemaInnovatie  as aantalThemaInnovatie,
  t.ThemaInternationaalOndernemen as aantalThemaInternationaalOndernemen,
  t.ThemaMobiliteit as aantalThemaMobiliteit,
  t.ThemaOmgeving as aantalThemaOmgeving,
  t.ThemaSalesMarketingCommunicatie as aantalThemaSalesMarketingCommunicatie,
  t.ThemaStrategieEnAlgemeenManagement as aantalThemaStrategieEnAlgemeenManagement,
  t.ThemaTalent as aantalThemaTalent,
  t.ThemaWelzijn as aantalThemaWelzijn,
  t.ThemaFinancieelFiscaal as aantalThemaFinancieelFiscaal,
  p.PersoonId,
  cast(p.ThemaDuurzaamheid as int) as ThemaDuurzaamheid,
  cast(p.ThemaInnovatie as int) as ThemaInnovatie,
  cast(p.ThemaInternationaalOndernemen as int) as ThemaInternationaalOndernemen,
  cast(p.ThemaMobiliteit as int) as ThemaMobiliteit,
  cast(p.ThemaOmgeving as int) as ThemaOmgeving,
  cast(p.ThemaSalesMarketingCommunicatie as int) as ThemaSalesMarketingCommunicatie,
  cast(p.ThemaStrategieEnAlgemeenManagement as int) as ThemaStrategieEnAlgemeenManagement,
  cast(p.ThemaTalent as int) as ThemaTalent,
  cast(p.ThemaWelzijn as int) as ThemaWelzijn,
  cast(p.ThemaFinancieelFiscaal as int) as ThemaFinancieelFiscaal

  from Inschrijving i 
  left join Campagne c on c.CampagneId = i.CampagneId
  left join SessieInschrijving si on i.InschrijvingId = si.InschrijvingId
  left join Sessie s on si.SessieId = s.SessieId
  left join Contactfiche cf on i.ContactficheId = cf.ContactpersoonId
  left join Persoon p on cf.PersoonId = p.PersoonId
  inner join totalen t on c.TypeCampagne = t.TypeCampagne and s.ThemaNaam = t.ThemaNaam
  where s.ThemaNaam is not null and c.TypeCampagne is not null 
  order by 1, 3 desc