-- This is where you will write the SQL to create the tables needed by the bar staff to assist on restocking decisions
DROP TABLE IF EXISTS overStockLondon;

CREATE TABLE overStockLondon AS
SELECT date(a.datetime) AS date,
    b.BarName,
    d.GlassName,
    MAX(e.Stock) AS maxStock,
    COUNT(d.GlassName) AS glassCount
FROM sales a
JOIN bar_dim b ON a.BarID = b.BarID
JOIN drinks_dim c ON a.DrinkID = c.DrinkID
JOIN glasses_dim d ON c.GlassID = d.GlassID
JOIN stock e ON d.GlassID = e.GlassID AND e.BarID = b.BarID
WHERE BarName = 'london'
GROUP BY date, d.GlassName;

DROP TABLE IF EXISTS overStockBudapest;

CREATE TABLE overStockBudapest AS
SELECT date(a.datetime) AS date,
    b.BarName,
    d.GlassName,
    MAX(e.Stock) AS maxStock,
    COUNT(d.GlassName) AS glassCount
FROM sales a
JOIN bar_dim b ON a.BarID = b.BarID
JOIN drinks_dim c ON a.DrinkID = c.DrinkID
JOIN glasses_dim d ON c.GlassID = d.GlassID
JOIN stock e ON d.GlassID = e.GlassID AND e.BarID = b.BarID
Where BarName = 'budapest'
GROUP BY date, d.GlassName;

DROP TABLE IF EXISTS overStockNY;

CREATE TABLE overStockNY AS
SELECT date(a.datetime) AS date,
    b.BarName,
    d.GlassName,
    MAX(e.Stock) AS maxStock,
    COUNT(d.GlassName) AS glassCount
FROM sales a
JOIN bar_dim b ON a.BarID = b.BarID
JOIN drinks_dim c ON a.DrinkID = c.DrinkID
JOIN glasses_dim d ON c.GlassID = d.GlassID
JOIN stock e ON d.GlassID = e.GlassID AND e.BarID = b.BarID
Where BarName = 'new york'
GROUP BY date, d.GlassName;

--PoC Tables above are supposed to show when glass usage for a day passes the glass stock or the bar.