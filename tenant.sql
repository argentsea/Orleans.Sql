/* ===========================================================================
 * This is the setup file for a “tenant” SQL Server database.
 * This table contains the sharded grain storage and reminder data in Orleans.
 * Presumably the ShardId determins the tenant in a hosted implementation.
 * The connections to these databases should into the ArgentSea ShardCollection. 
=========================================================================== */

-- Database Creation (can be skipped if already created):
EXECUTE sp_configure 'contained database authentication', 1; -- can be skipped if already configured.
GO
RECONFIGURE;
GO

CREATE DATABASE $TenantName CONTAINMENT = Partial COLLATE Latin1_General_100_CI_AI_SC_UTF8; -- note that we are using a newer, non-default UTF8 collation (default is UTF16)
GO

ALTER DATABASE $TenantName SET READ_COMMITTED_SNAPSHOT ON; -- Substitute preferred DB name
ALTER DATABASE $TenantName SET ALLOW_SNAPSHOT_ISOLATION ON; -- Substitute preferred DB name
GO


--Database Initialization
USE $TenantName; -- Substitute preferred DB name
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rdr')
BEGIN;
	EXECUTE (N'CREATE SCHEMA rdr;')
END;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'wtr')
BEGIN;
	EXECUTE (N'CREATE SCHEMA wtr;')
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'OrleansReminder')
BEGIN;
	CREATE TABLE dbo.OrleansReminder
	(
		ReminderId int NOT NULL IDENTITY(-2147483231, 1),
		GrainId varbinary(68) NOT NULL,
		Origin nchar(1) NOT NULL,
		ReminderName nvarchar(150) NOT NULL,
		StartTime datetime2(3) NOT NULL,
		Period bigint NOT NULL,
		GrainHash int NOT NULL,
		Version int NOT NULL,
		CONSTRAINT PK_OrleansReminder PRIMARY KEY (ReminderId),
		INDEX UIX_OrleansReminder_GrainId_ReminderName UNIQUE (GrainId, ReminderName) INCLUDE (StartTime, Period),
		INDEX IX_OrleansReminder_GrainHash NONCLUSTERED COLUMNSTORE (GrainHash),
		CONSTRAINT FK_OrleansReminder_GrainType FOREIGN KEY (Origin) REFERENCES dbo.OrleansGrainType (Origin)
	);
END;

-- Authorization
IF NOT EXISTS (SELECT 1 FROM sys.sysusers WHERE name = 'orleansReader')
BEGIN;
	CREATE USER orleansReader WITH PASSWORD = '$ReaderPassword';
END;

IF NOT EXISTS (SELECT 1 FROM sys.sysusers WHERE name = 'orleansWriter')
BEGIN;
	CREATE USER orleansWriter WITH PASSWORD = '$WriterPassword';
END;


GRANT EXECUTE ON SCHEMA::rdr TO orleansReader;
GRANT EXECUTE ON SCHEMA::wtr TO orleansWriter;


-- Code
CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRangeRows1KeyV1 (
	@BeginHash int,
	@EndHash int
) AS
BEGIN
SELECT OrleansReminder.GrainId,
        OrleansReminder.Origin,
        OrleansGrainType.GrainType,
		OrleansReminder.ReminderName,
		OrleansReminder.StartTime,
		OrleansReminder.Period,
        OrleansReminder.Version
	FROM dbo.OrleansReminder
        INNER JOIN dbo.OrleansGrainType
        ON OrleansGrainType.Origin = OrleansReminder.Origin
	WHERE OrleansReminder.GrainHash > @BeginHash
		AND OrleansReminder.GrainHash <= @EndHash;
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRangeRows2KeyV1 (
	@BeginHash int,
	@EndHash int
) AS
BEGIN
SELECT OrleansReminder.GrainId,
        OrleansReminder.Origin,
        OrleansGrainType.GrainType,
		OrleansReminder.ReminderName,
		OrleansReminder.StartTime,
		OrleansReminder.Period,
        OrleansReminder.Version
	FROM dbo.OrleansReminder
        INNER JOIN dbo.OrleansGrainType
        ON OrleansGrainType.Origin = OrleansReminder.Origin
	WHERE ((OrleansReminder.GrainHash > @BeginHash AND @BeginHash IS NOT NULL)
		OR (OrleansReminder.GrainHash <= @EndHash AND @EndHash IS NOT NULL));
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRowKeyV1 (
	@GrainId varbinary(68),
	@ReminderName nvarchar(150)
) AS
BEGIN
SELECT OrleansReminder.GrainId,
        OrleansReminder.Origin,
        OrleansGrainType.GrainType,
		OrleansReminder.ReminderName,
		OrleansReminder.StartTime,
		OrleansReminder.Period,
        OrleansReminder.Version
	FROM dbo.OrleansReminder
        INNER JOIN dbo.OrleansGrainType
        ON OrleansGrainType.Origin = OrleansReminder.Origin
	WHERE OrleansReminder.GrainId = @GrainId
		AND OrleansReminder.ReminderName = @ReminderName;
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRowsKeyV1 (
	@GrainId varbinary(68)
) AS
BEGIN
SELECT OrleansReminder.GrainId,
        OrleansReminder.Origin,
        OrleansGrainType.GrainType,
		OrleansReminder.ReminderName,
		OrleansReminder.StartTime,
		OrleansReminder.Period,
        OrleansReminder.Version
	FROM dbo.OrleansReminder
        INNER JOIN dbo.OrleansGrainType
        ON OrleansGrainType.Origin = OrleansReminder.Origin
	WHERE OrleansReminder.GrainId = @GrainId;
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansReminderDeleteRowKeyV1 (
	@GrainId varbinary(68),
	@ReminderName nvarchar(150),
    @Version int,
	@IsFound bit OUTPUT
) AS
BEGIN
	DELETE FROM dbo.OrleansReminder
	WHERE OrleansReminder.GrainId = @GrainId
		AND OrleansReminder.ReminderName = @ReminderName
        AND OrleansReminder.Version = @Version;

	SET @IsFound = @@ROWCOUNT;
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansReminderUpsertRowKeyV1 (
	@GrainId varbinary(68),
    @Origin nchar(1),
    @GrainType varbinary(128),
	@ReminderName nvarchar(150),
	@StartTime datetime2,
	@Period bigint, 
	@GrainHash int,
    @OldVersion int,
    @NewVersion int OUTPUT
) AS
BEGIN
	SET XACT_ABORT, NOCOUNT ON;
	BEGIN TRANSACTION;

    DECLARE @CurrentVersion int = (
        SELECT OrleansReminder.Version
        FROM dbo.OrleansReminder WHERE GrainId = @GrainId
		AND ReminderName = @ReminderName);

    IF @CurrentVersion <> @OldVersion
    BEGIN;
        THROW 51000, 'Concurrenty error', 1
    END;
    SET @NewVersion = @OldVersion + 1;

    IF NOT EXISTS(SELECT 1 FROM dbo.OrleansGrainType WHERE OrleansGrainType.Origin = @Origin)
    BEGIN
        INSERT INTO dbo.OrleansGrainType (Origin, GrainType)
        VALUES(@Origin, @GrainType);
    END;

	UPDATE dbo.OrleansReminder
	SET
		StartTime = @StartTime,
		Period = @Period,
		GrainHash = @GrainHash,
		Version = @NewVersion
	WHERE GrainId = @GrainId
		AND ReminderName = @ReminderName;


	INSERT INTO dbo.OrleansReminder
	(
		GrainId,
		Origin,
		ReminderName,
		StartTime,
		Period,
		GrainHash,
		Version
	)
	SELECT
		@GrainId,
		@Origin,
		@ReminderName,
		@StartTime,
		@Period,
		@GrainHash,
		0
	WHERE
		@@ROWCOUNT=0;
	COMMIT TRANSACTION;
END;
