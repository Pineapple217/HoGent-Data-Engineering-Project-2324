CREATE VIEW dbo.epic_3_pageviews
WITH SCHEMABINDING
AS
WITH PageviewCount AS (
    SELECT p.PageTitle, COUNT(p.ContactId) AS count
    FROM dbo.Pageviews p
    GROUP BY p.PageTitle
)
SELECT pv.PageTitle, pv.ContactId
FROM dbo.Pageviews pv
JOIN PageviewCount pc ON pv.PageTitle = pc.PageTitle
WHERE pv.PageTitle IN (
    SELECT TOP (2500) pc.PageTitle
    FROM PageviewCount pc
    ORDER BY pc.count DESC
);