# Sprint 1 W2 

## Vraag 1: Moeten we een interface maken om adhv queries de kwaliteit de bekijken?
- app / frontend / cli / ...
- gebruik door klant van powerbi? 

### Antwoord vraag 1
geef opties aan de klant

## Vraag 2: Moeten we de kwaliteit van primary keys / unieke data bekijken
- checken we volgens demo van ML?

### Antwoord vraag 2
best checken

## Vraag 3: Wat moeten we doen met incomplete data?
- vragen aan klant?
- aanvullen volgens assumpties? -> logica schrijven

### Antwoord vraag 3
- klant heeft zelf CRM database, dus heeft geen oltp meer nodig
- vraag aan de klant of ze een csv met nieuwe data of een met aangevulde data geven
- vraag aan de klant of we de ML modellen moeten hertrainen of volledig opnieuw maken

## Vraag 4: DB heropbouwen bij nieuwe data of alles updaten?
- update = veel meer werk, maar toekomstgericht en klantgericht
- heropbouw = simpeler voor nieuwe data, maar moeilijk met grotere database

### Antwoord vraag 4
- soms zal er heraangevuld moeten worden, soms zal tabel opnieuw moeten gemaakt worden
- bedoeling is dat er een soort DWH komt
- aanvullen met publieke data (?)

## Vraag 5: waar zitten andere groepen vast en welke tips hebben ze
- connectieproblemen met docker
- epic 3: recommendation system met lib 'surprise' - traditionele ML - Tensorflow
- epic 3: collaborative en content based filtering, nieuwe klant moet gelinkt worden aan gelijkaardige bestaande klant (trigger)
- epic 4: content based filtering
- maak een API
- maak test & trainingsets
- toon keuze parameters
- kijk naar key phrases uit emails
- ML: Embedding = strings omzetten naar getallen (vectoren) => vector gelijkaardig als betekenis gelijkaardig is
- epic 7: is LLM, niet te veel op focussen
- 'huggingface' kan interessant zijn
- Data cleaning: lib 'Open Refine'