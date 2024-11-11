CREATE TABLE [dbo].[prw_encounters] (
    [id]               INT            IDENTITY (1, 1) NOT NULL,
    [prw_id]           VARCHAR (24)   NOT NULL,
    [location]         VARCHAR (256)  NOT NULL,
    [dept]             VARCHAR (256)  NOT NULL,
    [encounter_date]   DATETIME       NOT NULL,
    [encounter_time]   DATETIME       NOT NULL,
    [encounter_type]   VARCHAR (256)  NOT NULL,
    [service_provider] VARCHAR (256)  NULL,
    [billing_provider] VARCHAR (256)  NULL,
    [with_pcp]         BIT            NULL,
    [appt_status]      VARCHAR (256)  NULL,
    [diagnoses]        VARCHAR (2048) NULL,
    [level_of_service] VARCHAR (256)  NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    FOREIGN KEY ([prw_id]) REFERENCES [dbo].[prw_patients] ([prw_id])
);


GO

