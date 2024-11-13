--Script to load pre-ICB data into the new sickness table
--This includes CNWL and CLCH without an ics_code to not mess with the non-NCL ICS trend
INSERT INTO [Data_Lab_NCL_Dev].[JakeK].[wf_sickness] (
	[date_data], [ics_code], [ics_name], [org_code], [org_name],
    [staffgroup], [days_lost], [days_available], [date_upload])
SELECT 
	[date] AS [date_data],
	CASE
		WHEN org_code IN ('RYX', 'RV3') THEN 'XXX'
		ELSE 'QMJ'
	END AS [ics_code],
	CASE
		WHEN org_code IN ('RYX', 'RV3') THEN 'For histroic community figures'
		ELSE 'North Central London'
	END AS [ics_name],
    [org_code],
    [org_name],
    [staff_group] AS [staffgroup],
    [days_lost],
    [days_available],
	'2024-11-13 09:00:00.000' AS [date_upload]
  FROM [Data_Lab_NCL_Dev].[JakeK].[wf_sick_benchmark]
  WHERE date < '2022-08-31'
  AND org_code IN ('RRP', 'TAF', 'RP4', 'RP6', 'RAP', 'RAN', 'RAL', 'RRV', 'RKE', 'RNK', 'RYX', 'RV3')