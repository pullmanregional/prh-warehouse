CREATE TABLE [dbo].[prw_sources_meta] (
    [id]       INT           IDENTITY (1, 1) NOT NULL,
    [filename] VARCHAR (512) NOT NULL,
    [modified] DATETIME      NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    UNIQUE NONCLUSTERED ([filename] ASC)
);


GO

