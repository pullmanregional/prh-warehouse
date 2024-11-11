CREATE TABLE [dbo].[prw_patients] (
    [id]     INT           IDENTITY (1, 1) NOT NULL,
    [prw_id] VARCHAR (24)  NOT NULL,
    [sex]    VARCHAR (1)   NOT NULL,
    [age]    INT           NULL,
    [age_mo] INT           NULL,
    [city]   VARCHAR (256) NULL,
    [state]  VARCHAR (64)  NULL,
    [pcp]    VARCHAR (256) NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    UNIQUE NONCLUSTERED ([prw_id] ASC)
);


GO

