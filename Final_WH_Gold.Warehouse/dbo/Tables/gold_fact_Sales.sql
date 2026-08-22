CREATE TABLE [dbo].[gold_fact_Sales] (

	[sk_customer] int NULL, 
	[sk_product] int NULL, 
	[sk_store] int NULL, 
	[unit_price] int NULL, 
	[sale_id] int NULL, 
	[sale_date] date NULL, 
	[quantity] int NULL, 
	[amount] decimal(18,2) NULL
);