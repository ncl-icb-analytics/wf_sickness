--ICS lookup for London Trusts
SELECT 
	org_pro.[Organisation_Code] AS [org_code],
    org_pro.[Organisation_Name] AS [org_name],
	--Set ICB Organisations as their own ICS code
	CASE
		WHEN org_pro.[SK_OrganisationID] IN (440024, 440027, 440028, 440042, 440054)
		THEN org_pro.[Organisation_Code]
		ELSE org_ics.[Organisation_Code] 
	END AS [ics_code],
	--Set ICB Organisations as their own ICS name
	CASE
		WHEN org_pro.[SK_OrganisationID] IN (440024, 440027, 440028, 440042, 440054)
		THEN org_pro.[Organisation_Name]
		ELSE org_ics.[Organisation_Name]
	END AS [ics_name]

FROM [Dictionary].[dbo].[Organisation] org_pro

LEFT JOIN [Dictionary].[dbo].[Organisation] org_ics
ON org_pro.[SK_OrganisationID_ParentOrg] = org_ics.[SK_OrganisationID]

WHERE
--Either the organisation is in a London ICS 
(
	org_ics.[SK_OrganisationID] IN (440024, 440027, 440028, 440042, 440054) AND 
	(
		org_pro.[SK_OrganisationTypeID] IN ('41', '67') OR 
		org_pro.[Organisation_Code] IN ('NQV', 'DEQ', 'AXA', 'NNV', 'G6V2S')
	)
) OR
--The organisation is a London ICB
org_pro.[SK_OrganisationID] IN (440024, 440027, 440028, 440042, 440054)