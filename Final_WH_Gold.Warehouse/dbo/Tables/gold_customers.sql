CREATE TABLE [dbo].[gold_customers] (

	[sk_customer] int NOT NULL, 
	[customer_id] bigint NULL, 
	[customer_name] varchar(200) NULL, 
	[email] varchar(200) NULL, 
	[city] varchar(80) NULL, 
	[birth_date] date NULL
);