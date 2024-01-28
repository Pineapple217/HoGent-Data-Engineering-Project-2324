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