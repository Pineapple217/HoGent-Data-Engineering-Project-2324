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