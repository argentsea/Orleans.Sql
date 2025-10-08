/* ===========================================================================
 * This is the setup file for a “common” SQL Server database.
 * This table contains the cluster registration table, used by Silos and Clients
 * and it is “common” in that a single set of tables are shared across all shards/tenents.
 * The connection to this should into the ArgentSea DbCollection. 
=========================================================================== */

-- Database Creation (can be skipped if already created):
EXECUTE sp_configure 'contained database authentication', 1; -- can be skipped if already configured.
GO
RECONFIGURE;
GO

CREATE DATABASE Common
CONTAINMENT = Partial
ON PRIMARY
(
    NAME = N'Common'
),
FILEGROUP imoltp CONTAINS MEMORY_OPTIMIZED_DATA
(
    NAME = N'IMOLTP'
)
COLLATE Latin1_General_100_CI_AI_SC_UTF8; -- note that we are using a newer, non-default UTF8 collation (default is UTF16)

ALTER DATABASE Common SET READ_COMMITTED_SNAPSHOT ON;
ALTER DATABASE Common SET ALLOW_SNAPSHOT_ISOLATION ON;

--Database Initialization
USE Common;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rdr')
BEGIN;
	EXECUTE (N'CREATE SCHEMA rdr;')
END;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'wtr')
BEGIN;
	EXECUTE (N'CREATE SCHEMA wtr;')
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'OrleansClusterMemberVersion')
BEGIN;
	CREATE TABLE dbo.OrleansClusterMemberVersion
	(
		ClusterId nvarchar(150) NOT NULL CONSTRAINT PK_OrleansClusterMemberVersion PRIMARY KEY NONCLUSTERED,
		Timestamp datetime2(3) NOT NULL CONSTRAINT DF_OrleansClusterMemberVersion_Timestamp DEFAULT  GETUTCDATE(),
		ETagNo int NOT NULL CONSTRAINT DF_OrleansClusterMemberVersion_ETagNo DEFAULT (0),
		Version int NOT NULL CONSTRAINT DF_OrleansClusterMemberVersion_Version DEFAULT (0),
	) WITH (MEMORY_OPTIMIZED=ON);
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'OrleansClusterMember')
BEGIN;
	CREATE TABLE dbo.OrleansClusterMember
	(
		ClusterMemberId int NOT NULL IDENTITY(1, 1),
		ClusterId nvarchar(150) NOT NULL,
		Address varbinary(16) NOT NULL,
		Port int NOT NULL,
		Generation int NOT NULL,
		SiloName nvarchar(150) NOT NULL,
		HostName nvarchar(150) NOT NULL,
		Status smallint NOT NULL,
		ProxyPort int NULL,
		StartTime datetime2(3) NOT NULL,
		IAmAliveTime datetime2(3) NOT NULL,
		MembershipETagNo int NOT NULL,
		MembershipVersion int NOT NULL,
		CONSTRAINT PK_OrleansClusterMember PRIMARY KEY NONCLUSTERED (ClusterMemberId),
		CONSTRAINT UIX_OrleansClusterMember_ClusterId_Generation UNIQUE (ClusterId, Address, Port, Generation),
		CONSTRAINT FK_OrleansClusterMember_OrleansClusterMemberVersion_ClusterId FOREIGN KEY (ClusterId) REFERENCES dbo.OrleansClusterMemberVersion (ClusterId),
		CONSTRAINT FK_OrleansClusterMember_ClusterId FOREIGN KEY (ClusterId) REFERENCES dbo.OrleansClusterMemberVersion (ClusterId)
	) WITH (MEMORY_OPTIMIZED=ON);
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'OrleansClusterMemberSuspicion')
BEGIN;
	CREATE TABLE dbo.OrleansClusterMemberSuspicion
	(
		ClusterMemberId int NOT NULL,
		Address bigint NOT NULL,
		Port int NOT NULL,
		Generation int NOT NULL,
		Timestamp datetime2(3) NOT NULL DEFAULT GETUTCDATE(),
		CONSTRAINT PK_OrleansClusterMemberSuspicion  PRIMARY KEY NONCLUSTERED (ClusterMemberId),
		CONSTRAINT UIX_OrleansClusterMemberSuspicion_Source UNIQUE (Address, Port, Generation),
		CONSTRAINT FK_OrleansClusterMemberSuspicion_Member FOREIGN KEY (ClusterMemberId) REFERENCES dbo.OrleansClusterMember (ClusterMemberId)
	) WITH (MEMORY_OPTIMIZED=ON);
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
IF NOT EXISTS (SELECT 1 FROM sys.sysusers WHERE name = 'clientUser')
BEGIN;
	CREATE USER clientUser  WITH PASSWORD = '$ClientPassword';
END;


GRANT EXECUTE ON SCHEMA::rdr TO orleansReader;
GRANT EXECUTE ON SCHEMA::wtr TO orleansWriter;


-- Code
IF NOT EXISTS (SELECT 1 FROM sys.types WHERE name = 'SuspicionType')
BEGIN;
	CREATE TYPE wtr.SuspicionType AS TABLE (
		Address varbinary(16) NOT NULL,
		Port int NOT NULL,
		Generation int NOT NULL,
		Timestamp datetime2(3) NOT NULL DEFAULT GETUTCDATE(),
		PRIMARY KEY NONCLUSTERED HASH (Address, Port, Generation) WITH (BUCKET_COUNT = 1000)
	) WITH (MEMORY_OPTIMIZED = ON);
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansClusterGatewayListV1 (
	@ClusterId nvarchar(150) NOT NULL
)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)
	SELECT OrleansClusterMember.Address,
		OrleansClusterMember.ProxyPort,
		OrleansClusterMember.Generation
	FROM dbo.OrleansClusterMember
	WHERE OrleansClusterMember.ClusterId = @ClusterId
        AND OrleansClusterMember.Status = 3; --Active
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansClusterGatewayQueryKeyV1 (
	@ClusterId nvarchar(150) NOT NULL,
	@Status int NOT NULL)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)  
	SELECT OrleansClusterMember.Address,
		OrleansClusterMember.ProxyPort,
		OrleansClusterMember.Generation
	FROM dbo.OrleansClusterMember
	WHERE OrleansClusterMember.ClusterId = @ClusterId
		AND OrleansClusterMember.Status = @Status
		AND OrleansClusterMember.ProxyPort > 0;
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansClusterMemberReadSuspicionV1 (
	@ClusterId nvarchar(150) NOT NULL,
	@Address varbinary(16) NOT NULL,
	@Port int NOT NULL,
	@Generation int NOT NULL)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)  
	SELECT OrleansClusterMemberSuspicion.ClusterMemberId,
		OrleansClusterMemberSuspicion.Address,
		OrleansClusterMemberSuspicion.Port,
		OrleansClusterMemberSuspicion.Generation,
        OrleansClusterMemberSuspicion.Timestamp
	FROM dbo.OrleansClusterMember
        INNER JOIN dbo.OrleansClusterMemberSuspicion
        ON OrleansClusterMemberSuspicion.ClusterMemberId = OrleansClusterMember.ClusterMemberId
	WHERE OrleansClusterMember.ClusterId = @ClusterId
		AND OrleansClusterMember.Address = @Address
		AND OrleansClusterMember.Port = @Port
		AND OrleansClusterMember.Generation = @Generation;
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansClusterMemberReadAllKeyV1 (
	@ClusterId nvarchar(150) NOT NULL
)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)
    SELECT TOP 1 OrleansClusterMemberVersion.Timestamp,
        OrleansClusterMemberVersion.ETagNo,
        OrleansClusterMemberVersion.Version
    FROM dbo.OrleansClusterMemberVersion
    WHERE OrleansClusterMemberVersion.ClusterId = @ClusterId;

	SELECT OrleansClusterMember.ClusterMemberId,
		OrleansClusterMember.Address,
		OrleansClusterMember.Port,
		OrleansClusterMember.Generation,
		OrleansClusterMember.SiloName,
		OrleansClusterMember.HostName,
		OrleansClusterMember.Status,
		OrleansClusterMember.ProxyPort,
		OrleansClusterMember.StartTime,
		OrleansClusterMember.IAmAliveTime,
        OrleansClusterMember.MembershipETagNo,
        OrleansClusterMember.MembershipVersion
	FROM dbo.OrleansClusterMember
	WHERE OrleansClusterMember.ClusterId = @ClusterId;

    SELECT OrleansClusterMemberSuspicion.ClusterMemberId,
        OrleansClusterMemberSuspicion.Address,
        OrleansClusterMemberSuspicion.Port,
        OrleansClusterMemberSuspicion.Generation,
        OrleansClusterMemberSuspicion.Timestamp
    FROM dbo.OrleansClusterMemberSuspicion
    INNER JOIN dbo.OrleansClusterMember
    ON OrleansClusterMember.ClusterMemberId = OrleansClusterMemberSuspicion.ClusterMemberId
	WHERE OrleansClusterMember.ClusterId = @ClusterId
    ORDER BY OrleansClusterMemberSuspicion.ClusterMemberId;
END;
GO

CREATE OR ALTER PROCEDURE rdr.OrleansClusterMemberReadRowKeyV1 (
	@ClusterId nvarchar(150) NOT NULL,
	@Address varbinary(16) NOT NULL,
	@Port int NOT NULL,
	@Generation int NOT NULL)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)  
	SELECT TOP 1 
		OrleansClusterMember.SiloName,
		OrleansClusterMember.HostName,
		OrleansClusterMember.Status,
		OrleansClusterMember.ProxyPort,
		OrleansClusterMember.StartTime,
		OrleansClusterMember.IAmAliveTime,
        OrleansClusterMemberVersion.Timestamp,
        OrleansClusterMember.MembershipETagNo,
        OrleansClusterMember.MembershipVersion,
        OrleansClusterMemberVersion.ETagNo,
        OrleansClusterMemberVersion.Version
	FROM dbo.OrleansClusterMember
        INNER JOIN dbo.OrleansClusterMemberVersion
        ON OrleansClusterMemberVersion.ClusterId = OrleansClusterMember.ClusterId
	WHERE OrleansClusterMember.ClusterId = @ClusterId
        AND OrleansClusterMember.Address = @Address
        AND OrleansClusterMember.Port = @Port
        AND OrleansClusterMember.Generation = @Generation;


    SELECT OrleansClusterMemberSuspicion.Address,
        OrleansClusterMemberSuspicion.Port,
        OrleansClusterMemberSuspicion.Generation,
        OrleansClusterMemberSuspicion.Timestamp
	FROM dbo.OrleansClusterMember
        INNER JOIN dbo.OrleansClusterMemberSuspicion
        ON OrleansClusterMember.ClusterMemberId = OrleansClusterMemberSuspicion.ClusterMemberId
	WHERE OrleansClusterMember.ClusterId = @ClusterId
        AND OrleansClusterMember.Address = @Address
        AND OrleansClusterMember.Port = @Port
        AND OrleansClusterMember.Generation = @Generation;
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansClusterDeleteDefunctMembersV1 (
	@ClusterId nvarchar(150),
	@BeforeDate datetime2(3)
) AS 
BEGIN;
    DELETE FROM dbo.OrleansClusterMemberSuspicion
    FROM dbo.OrleansClusterMemberSuspicion
        INNER JOIN dbo.OrleansClusterMember
        ON OrleansClusterMember.ClusterMemberId = OrleansClusterMemberSuspicion.ClusterMemberId
    WHERE OrleansClusterMember.ClusterId = @ClusterId
        AND OrleansClusterMember.IAmAliveTime < @BeforeDate
        AND OrleansClusterMember.Status <> 3 --SiloStatus.Active = 3


    DELETE FROM dbo.OrleansClusterMember
    WHERE OrleansClusterMember.ClusterId = @ClusterId
        AND OrleansClusterMember.IAmAliveTime < @BeforeDate
        AND OrleansClusterMember.Status <> 3 --SiloStatus.Active = 3

    IF (SELECT Count(*) FROM dbo.OrleansClusterMember WHERE OrleansClusterMember.ClusterId = @ClusterId) = 0
    BEGIN
        DELETE FROM dbo.OrleansClusterMemberVersion
        WHERE OrleansClusterMemberVersion.ClusterId = @ClusterId
    END;
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansClusterDeleteV1 (
	@ClusterId nvarchar(150)
) AS
BEGIN;

	DELETE dbo.OrleansClusterMemberSuspicion
    FROM dbo.OrleansClusterMemberSuspicion
        INNER JOIN dbo.OrleansClusterMember
        ON OrleansClusterMember.ClusterMemberId = OrleansClusterMemberSuspicion.ClusterMemberId
	WHERE OrleansClusterMember.ClusterId = @ClusterId;

	DELETE FROM dbo.OrleansClusterMember
	WHERE OrleansClusterMember.ClusterId = @ClusterId;

	DELETE FROM dbo.OrleansClusterMemberVersion
	WHERE OrleansClusterMemberVersion.ClusterId = @ClusterId;

END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansClusterInsertMemberKeyV1 (
	@ClusterId nvarchar(150) NOT NULL,
	@Address varbinary(16) NOT NULL,
	@Port int NOT NULL,
	@Generation int NOT NULL,
	@SiloName nvarchar(150) NOT NULL,
	@HostName nvarchar(150) NOT NULL,
	@Status smallint NOT NULL,
	@ProxyPort int NULL,
	@StartTime datetime2(3) NOT NULL,
	@IAmAliveTime datetime2(3) NOT NULL,
	@ETagNo int NOT NULL,
	@Version int NOT NULL
	)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)  
	UPDATE dbo.OrleansClusterMemberVersion
	SET Timestamp = GETUTCDATE(),
        Version = @Version
	WHERE ClusterId = @ClusterId
		AND ETagNo = @ETagNo;

    --IF @@ROWCOUNT = 0
    --BEGIN;
    --    THROW 51000, 'Concurrency error.', 1;
    --END;

	INSERT INTO dbo.OrleansClusterMember
	(
		ClusterId,
		Address,
		Port,
		Generation,
		SiloName,
		HostName,
		Status,
		ProxyPort,
		StartTime,
		IAmAliveTime,
        MembershipETagNo,
        MembershipVersion
	)
	SELECT @ClusterId,
		@Address,
		@Port,
		@Generation,
		@SiloName,
		@HostName,
		@Status,
		@ProxyPort,
		@StartTime,
		@IAmAliveTime,
        @ETagNo,
        @Version
	WHERE NOT EXISTS
	(
		SELECT 1
		FROM dbo.OrleansClusterMember
		WHERE OrleansClusterMember.ClusterId = @ClusterId
			AND OrleansClusterMember.Address = @Address
			AND OrleansClusterMember.Port = @Port
			AND OrleansClusterMember.Generation = @Generation
	);

	--IF @@ROWCOUNT = 0
 --   BEGIN;
	--	THROW 51000, 'Unexpectedly could not insert a new value in the cluster membership table. Possibly already exists', 1;
 --   END;
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansClusterInsertMemberVersionKeyV1 (
	@ClusterId nvarchar(150) NOT NULL,
	@ETagNo int NOT NULL,
	@Version int NOT NULL
	)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)  
	INSERT INTO dbo.OrleansClusterMemberVersion (ClusterId, ETagNo, Version)
	SELECT @ClusterId, @ETagNo, @Version
	WHERE NOT EXISTS
	(SELECT 1
	FROM dbo.OrleansClusterMemberVersion
	WHERE OrleansClusterMemberVersion.ClusterId = @ClusterId);
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansClusterUpdateIAmAliveTimeKeyV1 (
	@IAmAliveTime datetime2(3) NOT NULL, 
	@ClusterId nvarchar(150) NOT NULL,
    @Address varbinary(16),
    @Port int NOT NULL,
	@Generation int NOT NULL)
WITH NATIVE_COMPILATION, SCHEMABINDING  
AS BEGIN ATOMIC WITH  
(  
 TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english'  
)  
	-- This is expected to never fail by Orleans, so return value is not needed nor is it checked.
	UPDATE dbo.OrleansClusterMember
	SET IAmAliveTime = @IAmAliveTime
	WHERE ClusterId = @ClusterId
		AND Address = @Address
		AND Port = @Port
		AND Generation = @Generation;
END;
GO

CREATE OR ALTER PROCEDURE wtr.OrleansClusterUpdateMemberKeyV1 (
	@ClusterId nvarchar(150),
	@Status int,
	@ProxyPort int,
	@IAmAliveTime datetime2(3),
	@Address varbinary(16),
	@Port int,
	@Generation int,
    @SuspectTimes dbo.SuspicionType READONLY,
	@ETagNo int,
	@Version int
    )
AS 
BEGIN;
    DECLARE @ClusterMemberId int = (SELECT ClusterMemberId
                        FROM dbo.OrleansClusterMember
                        WHERE OrleansClusterMember.ClusterId = @ClusterId
                            AND OrleansClusterMember.Address = @Address
                            AND OrleansClusterMember.Port = @Port
                            AND OrleansClusterMember.Generation = @Generation
                            AND OrleansClusterMember.MembershipETagNo = @ETagNo);

    IF @ClusterMemberId IS NULL
    BEGIN;
        THROW 51000, 'Cluster member not found or invalid eTag.', 1;
    END;

	UPDATE dbo.OrleansClusterMemberVersion
	SET Timestamp = GETUTCDATE(),
		ETagNo = @ETagNo,
        Version = @Version
	WHERE ClusterId = @ClusterId;

    IF @@ROWCOUNT = 0
    BEGIN;
        THROW 51000, 'Concurrency error.', 1;
    END;

	UPDATE dbo.OrleansClusterMember
	SET --SiloName,
        --HostName,
        Status = @Status,
        ProxyPort = @ProxyPort,
		IAmAliveTime = @IAmAliveTime,
        MembershipETagNo = @ETagNo,
        MembershipVersion = @Version
	WHERE ClusterMemberId = @ClusterMemberId;

    INSERT INTO dbo.OrleansClusterMemberSuspicion (ClusterMemberId, Address, Port, Generation, Timestamp)
    SELECT @ClusterMemberId, Susp.Address, Susp.Port, Susp.Generation, Susp.Timestamp
    FROM @SuspectTimes As Susp
        LEFT OUTER JOIN OrleansClusterMemberSuspicion
        ON OrleansClusterMemberSuspicion.ClusterMemberId = @ClusterMemberId
            AND OrleansClusterMemberSuspicion.Address = Susp.Address
            AND OrleansClusterMemberSuspicion.Port = Susp.Port
            AND OrleansClusterMemberSuspicion.Generation = Susp.Generation
    WHERE ClusterMemberId = @ClusterMemberId
        AND Susp.Port Is Null;

    UPDATE dbo.OrleansClusterMemberSuspicion
    SET Timestamp = Susp.Timestamp
    FROM dbo.OrleansClusterMemberSuspicion
        INNER JOIN @SuspectTimes As Susp
        ON OrleansClusterMemberSuspicion.ClusterMemberId = @ClusterMemberId
            AND OrleansClusterMemberSuspicion.Address = Susp.Address
            AND OrleansClusterMemberSuspicion.Port = Susp.Port
            AND OrleansClusterMemberSuspicion.Generation = Susp.Generation;

    DELETE dbo.OrleansClusterMemberSuspicion
    FROM dbo.OrleansClusterMemberSuspicion
        LEFT OUTER JOIN @SuspectTimes As Susp
        ON OrleansClusterMemberSuspicion.ClusterMemberId = @ClusterMemberId
            AND OrleansClusterMemberSuspicion.Address = Susp.Address
            AND OrleansClusterMemberSuspicion.Port = Susp.Port
            AND OrleansClusterMemberSuspicion.Generation = Susp.Generation
    WHERE OrleansClusterMemberSuspicion.ClusterMemberId = @ClusterMemberId
        AND Susp.Port is Null;
END;


GRANT EXECUTE ON OBJECT::rdr.OrleansClusterGatewayListV1 TO clientUser;


