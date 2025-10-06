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
		GrainKey varbinary(1023) NOT NULL, -- corresponds to max GrainKey length in Orleans CosmosDB provider
		GrainType nvarchar(1023) NOT NULL, -- ADO.net provider’s size of 150 has created issues and forced expansion, so we are using 1023 here to avoid any bugs.
		ReminderName nvarchar(150) NOT NULL,
		StartTime datetime2(3) NOT NULL,
		Period bigint NOT NULL,
		GrainHash bigint NOT NULL, -- technically uint would fit on 32-bits, but the range searches fail with type cast type.
		Version int NOT NULL,
		CONSTRAINT PK_OrleansReminder PRIMARY KEY (ReminderId),
		INDEX UIX_OrleansReminder_GrainKey_GrainType_ReminderName UNIQUE (GrainKey, GrainType, ReminderName) INCLUDE (StartTime, Period, Version),
		INDEX UIX_OrleansReminder_GrainKey_GrainType_Version UNIQUE (GrainKey, GrainType, Version) INCLUDE (ReminderName, StartTime, Period),
		INDEX IX_OrleansReminder_GrainHash NONCLUSTERED COLUMNSTORE (GrainHash)
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
GO

-- Code
CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRowKeyV1 (
	@GrainKey varbinary(1023),
	@GrainType nvarchar(1023),
	@ReminderName nvarchar(150)
) AS
BEGIN
SELECT OrleansReminder.GrainKey,
        OrleansReminder.GrainType,
		OrleansReminder.ReminderName,
		OrleansReminder.StartTime,
		OrleansReminder.Period,
        OrleansReminder.Version
	FROM dbo.OrleansReminder
	WHERE OrleansReminder.GrainKey = @GrainKey
		AND OrleansReminder.GrainType = @GrainType
		AND OrleansReminder.ReminderName = @ReminderName;
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRowsKeyV1 (
	@GrainKey varbinary(1023),
	@GrainType nvarchar(1023)
) AS
BEGIN
SELECT OrleansReminder.GrainKey,
        OrleansReminder.GrainType,
		OrleansReminder.ReminderName,
		OrleansReminder.StartTime,
		OrleansReminder.Period,
        OrleansReminder.Version
	FROM dbo.OrleansReminder
	WHERE OrleansReminder.GrainKey = @GrainKey
		AND OrleansReminder.GrainType = @GrainType;
END;
GO




CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRangeRows1KeyV1 (
	@BeginHash bigint,
	@EndHash bigint
) AS
BEGIN
	IF @BeginHash >= @EndHash
	BEGIN;
		SELECT OrleansReminder.GrainKey,
				OrleansReminder.GrainType,
				OrleansReminder.ReminderName,
				OrleansReminder.StartTime,
				OrleansReminder.Period,
				OrleansReminder.Version
			FROM dbo.OrleansReminder
			WHERE OrleansReminder.GrainHash > @BeginHash --sic, not inclusive
				OR OrleansReminder.GrainHash <= @EndHash;
	END;
	ELSE
	BEGIN;
		SELECT OrleansReminder.GrainKey,
				OrleansReminder.GrainType,
				OrleansReminder.ReminderName,
				OrleansReminder.StartTime,
				OrleansReminder.Period,
				OrleansReminder.Version
			FROM dbo.OrleansReminder
			WHERE OrleansReminder.GrainHash > @BeginHash --sic, not inclusive
				AND OrleansReminder.GrainHash <= @EndHash;
	END;
END;
GO

--CREATE OR ALTER PROCEDURE rdr.OrleansReminderReadRangeRows2KeyV1 (
--	@BeginHash bigint,
--	@EndHash bigint
--) AS
--BEGIN
--SELECT OrleansReminder.GrainKey,
--        OrleansReminder.GrainType,
--		OrleansReminder.ReminderName,
--		OrleansReminder.StartTime,
--		OrleansReminder.Period,
--        OrleansReminder.Version
--	FROM dbo.OrleansReminder
--	WHERE ((OrleansReminder.GrainHash > @BeginHash AND @BeginHash IS NOT NULL)
--		OR (OrleansReminder.GrainHash <= @EndHash AND @EndHash IS NOT NULL));
--END;
GO



CREATE OR ALTER PROCEDURE wtr.OrleansReminderDeleteRowKeyV1 (
	@GrainKey varbinary(1023),
	@GrainType nvarchar(1023),
	@ReminderName nvarchar(150),
    @Version int,
	@IsFound bit OUTPUT
) AS
BEGIN
	DELETE FROM dbo.OrleansReminder
	WHERE OrleansReminder.GrainKey = @GrainKey
		AND OrleansReminder.GrainType = @GrainType
		AND OrleansReminder.ReminderName = @ReminderName
        AND OrleansReminder.Version = @Version;

	SET @IsFound = @@ROWCOUNT;
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansReminderUpsertRowKeyV1 (
	@GrainKey varbinary(1023),
	@GrainType nvarchar(1023),
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
        FROM dbo.OrleansReminder WHERE GrainKey = @GrainKey
		AND GrainType = @GrainType
		AND ReminderName = @ReminderName);

    IF @CurrentVersion <> @OldVersion
    BEGIN;
        THROW 51000, 'Concurrenty error', 1
    END;
    SET @NewVersion = @OldVersion + 1;

	UPDATE dbo.OrleansReminder
	SET
		StartTime = @StartTime,
		Period = @Period,
		GrainHash = @GrainHash,
		Version = @NewVersion
	WHERE GrainKey = @GrainKey
		AND GrainType = @GrainType
		AND ReminderName = @ReminderName;


	INSERT INTO dbo.OrleansReminder
	(
		GrainKey,
		GrainType,
		ReminderName,
		StartTime,
		Period,
		GrainHash,
		Version
	)
	SELECT
		@GrainKey,
		@GrainType,
		@ReminderName,
		@StartTime,
		@Period,
		@GrainHash,
		0
	WHERE
		@@ROWCOUNT=0;
	COMMIT TRANSACTION;
END;
