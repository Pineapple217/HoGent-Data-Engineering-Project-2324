/* Gebruik de correlation.ipynb onder notebooks voor een heat map */

use voka;

create materialized view epic_5 as

with 

berekende_bezoeken as (
	select 
		Persoon.PersoonId, 
		visits.Campaign as campagneid,
		count(distinct Visits.VisitId) as aantal_bezoeken
	from Persoon
		left join Contactfiche on Persoon.PersoonId = Contactfiche.PersoonId
		join Visits on Visits.ContactId = Contactfiche.ContactpersoonId
	where persoon.PersoonId is not null 
		and visits.VisitId is not null 
		and ContactId is not null 
		and ContactpersoonId is not null
	group by persoon.PersoonId, Visits.Campaign
),

berekende_sessies as (
	select 
		Inschrijving.CampagneId,
		Persoon.PersoonId,
		count(Sessie.SessieId) as aantal_sessies
	from SessieInschrijving
		join Sessie on Sessie.SessieId = SessieInschrijving.SessieId
		join Inschrijving on Inschrijving.InschrijvingId = SessieInschrijving.InschrijvingId
		join Contactfiche on Contactfiche.ContactpersoonId = Inschrijving.ContactficheId
		join Persoon on Persoon.PersoonId = Contactfiche.PersoonId
	where sessie.CampagneId is not null
	group by Inschrijving.CampagneId, Persoon.PersoonId
),

-- werkt nog niet zoals het hoort
berekende_email_klikken as (
	select
		p.PersoonId, 
		v.Campaign, 
		sum(s.Clicks) as aantal_klikken_op_mail
	from SendEmailClicks s
		join voka.dbo.Visits v on s.ContactId = v.ContactId and s.EmailVersturenId = v.EmailSendId
		join voka.dbo.Contactfiche c on c.ContactpersoonId = s.ContactId
		join voka.dbo.Persoon p on c.PersoonId = p.PersoonId
	where s.ContactId is not null 
		and s.SentEmailId is not null 
		and v.Campaign is not null 
		and p.PersoonId is not null
	group by p.PersoonId, v.Campaign
)
select 
	distinct 
	Persoon.PersoonId, 
	Campagne.CampagneId,
	coalesce(aantal_sessies, 0) as aantal_sessies,
	coalesce(aantal_bezoeken, 0) as aantal_bezoeken,
	coalesce(aantal_klikken_op_mail, 0) as aantal_klikken_op_mail,
	Campagne.SoortCampagne,
	Campagne.TypeCampagne,
	Persoon.ThemaDuurzaamheid,
	Persoon.ThemaFinancieelFiscaal,
	Persoon.ThemaInnovatie,
	Persoon.ThemaInternationaalOndernemen,
	Persoon.ThemaMobiliteit,
	Persoon.ThemaOmgeving,
	Persoon.ThemaSalesMarketingCommunicatie,
	Persoon.ThemaStrategieEnAlgemeenManagement,
	Persoon.ThemaTalent,
	Persoon.ThemaWelzijn
from Persoon
	inner join Contactfiche on Contactfiche.PersoonId = Persoon.PersoonId
	inner join Inschrijving on contactfiche.ContactpersoonId = Inschrijving.ContactficheId
	inner join Campagne on Campagne.CampagneId = Inschrijving.CampagneId
	left join berekende_sessies on Campagne.CampagneId = berekende_sessies.CampagneId and Persoon.PersoonId = berekende_sessies.PersoonId
	left join berekende_email_klikken on Campagne.CampagneId= berekende_email_klikken.Campaign and Persoon.PersoonId = berekende_email_klikken.PersoonId
	left join berekende_bezoeken on Campagne.CampagneId = berekende_bezoeken.CampagneId and Persoon.PersoonId = berekende_bezoeken.PersoonId
order by aantal_sessies DESC
-- order by aantal_bezoeken DESC

-- tests --
/* use voka;

select *
from Visits
left join Contactfiche on Visits.contactid = Contactfiche.ContactpersoonId
join Persoon on persoon.PersoonId = Contactfiche.PersoonId
where Persoon.PersoonId = 'E56CFED1-2D91-EB11-811E-001DD8B72B62' and Visits.Campaign = 'B8EF1A5F-A76C-EC11-8943-000D3A2799E1'


select 
    Inschrijving.CampagneId,
    Persoon.PersoonId,
    Sessie.SessieId
from SessieInschrijving
join Sessie on Sessie.SessieId = SessieInschrijving.SessieId
join Inschrijving on Inschrijving.InschrijvingId = SessieInschrijving.InschrijvingId
join Contactfiche on Contactfiche.ContactpersoonId = Inschrijving.ContactficheId
join Persoon on Persoon.PersoonId = Contactfiche.PersoonId
where Persoon.PersoonId = 'E56CFED1-2D91-EB11-811E-001DD8B72B62' and Inschrijving.CampagneId = 'B8EF1A5F-A76C-EC11-8943-000D3A2799E1' */ 