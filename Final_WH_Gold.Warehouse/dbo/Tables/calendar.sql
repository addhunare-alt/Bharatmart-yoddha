CREATE TABLE [dbo].[calendar] (

	[DateKey] int NULL, 
	[Date] date NULL, 
	[Year] int NULL, 
	[MonthNo] int NULL, 
	[MonthName] varchar(3) NULL, 
	[MonthYear] varchar(8) NULL, 
	[MonthYearSort] int NULL, 
	[CalQuarter] int NULL, 
	[FiscalYearStart] int NULL, 
	[FiscalYear] varchar(10) NULL, 
	[FiscalMonthNo] int NULL, 
	[FiscalQuarterNo] int NULL, 
	[FiscalQuarter] varchar(2) NULL
);