--CREATE TABLE STATEMENT--
--For the main sickness data table
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[wf_sickness] (
    --Date column
    date_data DATE NOT NULL,

    --ICS + Organisation Columns
    ics_code CHAR(3) NOT NULL,
    ics_name VARCHAR(80) NOT NULL,
    org_code CHAR(3) NOT NULL,
    org_name VARCHAR(80) NOT NULL,

    --Breakdown Columns
    staffgroup VARCHAR(60) NOT NULL,

    --Metric Numerator and Denominator
    days_lost FLOAT,
    days_available FLOAT,

    --Timestamp
    date_upload DATETIME NOT NULL

    --Primary Key Restriction
    PRIMARY KEY (date_data, org_code, staffgroup)
);