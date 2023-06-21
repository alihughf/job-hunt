-- This is where you will write the SQL to create the tables needed by the bar staff to assist on restocking decisions
SELECT date(a.datetime) date,
    MAX(b.BarName),
    d.GlassName,
    MAX(e.Stock),
    COUNT(d.GlassName)
INTO overStockLondon
FROM sales a
RIGHT JOIN bar_dim b on a.BarID = b.BarID
RIGHT JOIN drinks_dim c on a.DrinkID = c.DrinkID
RIGHT JOIN glasses_dim d on c.GlassID = d.GlassID
RIGHT JOIN stock e on d.GlassID = e.GlassID and e.BarID = b.BarID
WHERE BarName = 'london'
GROUP BY date, GlassName;

SELECT date(a.datetime) date,
    MAX(b.BarName),
    d.GlassName,
    MAX(e.Stock),
    COUNT(d.GlassName)
INTO overStockBudapest
FROM sales a
RIGHT JOIN bar_dim b on a.BarID = b.BarID
RIGHT JOIN drinks_dim c on a.DrinkID = c.DrinkID
RIGHT JOIN glasses_dim d on c.GlassID = d.GlassID
RIGHT JOIN stock e on d.GlassID = e.GlassID and e.BarID = b.BarID
Where BarName = 'budapest'
GROUP BY date, GlassName;

SELECT date(a.datetime) date,
    MAX(b.BarName),
    d.GlassName,
    MAX(e.Stock),
    COUNT(d.GlassName)
INTO overStockNY
FROM sales a 
RIGHT JOIN bar_dim b on a.BarID = b.BarID
RIGHT JOIN drinks_dim c on a.DrinkID = c.DrinkID
RIGHT JOIN glasses_dim d on c.GlassID = d.GlassID
RIGHT JOIN stock e on d.GlassID = e.GlassID and e.BarID = b.BarID
Where BarName = 'new york'
GROUP BY date, GlassName;