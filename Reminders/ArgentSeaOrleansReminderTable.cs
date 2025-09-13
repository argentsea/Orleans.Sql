using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using ArgentSea;
using ArgentSea.Sql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Runtime;

namespace ArgentSea.Orleans.Sql;

/// <summary>
/// This cannot use the ArgentSea MapTo attributes because the ReminderEntry class is sealed.
/// </summary>
public class ArgentSeaOrleansReminderTable : IReminderTable
{
    private readonly SqlShardSets.ShardSet shardSet;
    private readonly ILogger<ArgentSeaOrleansReminderTable> logger;
    private string shardSetKey;

    public ArgentSeaOrleansReminderTable(SqlShardSets shards, IOptions<OrleansShardPersistenceOptions> dbOptions, ILogger<ArgentSeaOrleansReminderTable> logger)
    {
        ArgumentNullException.ThrowIfNull(shards, nameof(shards));
        this.shardSetKey = dbOptions.Value.ShardSetKey;
        var shardSet = shards[shardSetKey];
        if (shardSet is null)
        {
            throw new KeyNotFoundException($"ShardSet “{shardSetKey}” cannot be found in the ShardSets collection.");
        }
        this.shardSet = shardSet;
        this.logger = logger;
    }

    public async Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
    {
        ArgumentNullException.ThrowIfNull(reminderName, nameof(reminderName));

        var aGrainId = StringExtensions.Decode(grainId.Key.Value.Span); //decoded ShardKey<Guid, Guid, Guid, Guid> is 69 bytes (encoded is 91 bytes); assumed to be maximum key size.
        var prms = new ParameterCollection()
            .AddSqlVarBinaryInputParameter("GrainId", aGrainId.ToArray(), 68)
            .AddSqlNVarCharInputParameter("@ReminderName", reminderName, 150);

        var shardId = grainId.ShardId();
        try
        {
            var result = await shardSet[shardId].Read.QueryAsync<GrainId, ReminderEntry?>(Queries.OrleansReminderReadRowKey, prms, ReadRowHandler, true, grainId, CancellationToken.None);
            return result ?? new ReminderEntry() { GrainId = grainId };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading reminder row.");
            throw;
        }
    }

    private static ReminderEntry? ReadRowHandler(ReminderEntry? instance, short shardId, string sprocName, GrainId grainId, DbDataReader rdr, DbParameterCollection prms, string connectionDescription, ILogger logger)
        => ReadRowHandlerObj(instance, shardId, sprocName, grainId, rdr, prms, connectionDescription, logger);

    private static ReminderEntry? ReadRowHandlerObj(ReminderEntry? instance, short shardId, string sprocName, object? grainIdObj, DbDataReader rdr, DbParameterCollection prms, string connectionDescription, ILogger logger)
    {
        if (grainIdObj is null)
        {
            return null;
        }

        if (!rdr.HasRows || rdr.IsClosed || rdr.IsDBNull(0))
        {
            if (grainIdObj is GrainId)
            {
                return new ReminderEntry() { GrainId = (GrainId)grainIdObj };
            }
        }
        var gType = new GrainType((byte[])rdr[2]);
        var buffer = new byte[68];
        var bytesRead = rdr.GetBytes(0, 0L, buffer, 0, 68);
        var sizedBuffer = new ReadOnlySpan<byte>(buffer).Slice(0, (int)bytesRead).ToArray();
        var utf8GrainId = StringExtensions.EncodeToUtf8(sizedBuffer);

        //if (grainIdObj is GrainId && utf8GrainId != ((GrainId)grainIdObj).Key.Value.ToArray())
        //{
        //    throw new TypeLoadException($"Could not load reminder for grainId {((GrainId)grainIdObj).ToString} because the record key from the database, {utf8GrainId.ToString()}, does not match.");
        //}
        return new ReminderEntry()
        {
            GrainId = new GrainId(gType, new IdSpan(utf8GrainId.ToArray())),
            Period = new TimeSpan(rdr.GetInt64(5)),
            ReminderName = rdr.GetString(3),
            StartAt = rdr.GetDateTime(4),
            ETag = rdr.GetInt32(6).ToString(CultureInfo.InvariantCulture),
        };
    }

    public async Task<ReminderTableData> ReadRows(GrainId grainId)
    {
        var aGrainId = StringExtensions.Decode(grainId.Key.Value.Span);
        var prms = new ParameterCollection()
            .AddSqlVarBinaryInputParameter("GrainId", aGrainId.ToArray(), 68);

        var shardId = grainId.ShardId();
        try
        {
            var result = await shardSet[shardId].Read.ListAsync<ReminderEntry>(Queries.OrleansReminderReadRowKey, prms, "", CancellationToken.None);
            return new ReminderTableData(result);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading reminder rows.");
            throw;
        }
    }


    public async Task<ReminderTableData> ReadRows(uint begin, uint end)
    {
        var prms = new ParameterCollection()
            .AddSqlIntInputParameter("@BeginHash", unchecked((int)begin))
            .AddSqlIntInputParameter("@EndHash", unchecked((int)end));
        try
        {
            var result = await shardSet.ReadAll.QueryAsync<ReminderEntry?>(Queries.OrleansReminderReadRangeRows1Key, prms, null, "", ReadRowHandlerObj, CancellationToken.None);
            return new ReminderTableData(result ?? []);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading reminder rows.");
            throw;
        }

    }

    public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
    {
        ArgumentNullException.ThrowIfNull(reminderName, nameof(reminderName));

        var aGrainId = StringExtensions.Decode(grainId.Key.Value.Span);
        var prms = new ParameterCollection()
            .AddSqlVarBinaryInputParameter("GrainId", aGrainId.ToArray(), 68)
            .AddSqlNVarCharInputParameter("@ReminderName", reminderName, 150)
            .AddSqlIntInputParameter("@Version", int.Parse(eTag, CultureInfo.InvariantCulture))
            .AddSqlBitOutputParameter("@IsFound");

        var shardId = grainId.ShardId();
        try
        {
            return shardSet[shardId].Write.ReturnValueAsync<bool>(Queries.OrleansReminderDeleteRowKey, "@IsFound", prms, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error deleting reminder row.");
            throw;
        }
    }

    public Task TestOnlyClearTable()
    {
        // do nothing
        return Task.CompletedTask;
    }

    public async Task<string> UpsertRow(ReminderEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry, nameof(entry));

        //var shd = ShardKey<Guid>.FromUtf8(entry.GrainId.Key.AsSpan());
        var aGrainId = StringExtensions.Decode(entry.GrainId.Key.Value.Span);
        var shardId = entry.GrainId.ShardId();
        var prms = new ParameterCollection()
            .AddSqlVarBinaryInputParameter("GrainId", aGrainId.ToArray(), 68)
            .AddSqlVarBinaryInputParameter("@GrainType", entry.GrainId.Type.AsSpan().ToArray(), 256)
            .AddSqlNVarCharInputParameter("@ReminderName", entry.ReminderName, 150)
            .AddSqlDateTime2InputParameter("@StartTime", entry.StartAt)
            .AddSqlBigIntInputParameter("@Period", entry.Period.Ticks)
            .AddSqlIntInputParameter("@GrainHash", unchecked((int)entry.GrainId.GetUniformHashCode()))
            .AddSqlIntInputParameter("@OldVersion", int.Parse(entry.ETag, CultureInfo.InvariantCulture))
            .AddSqlIntOutputParameter("@NewVersion");

        try
        {
            var intTag = await shardSet[shardId].Write.ReturnValueAsync<int>(Queries.OrleansReminderUpsertRowKey, "@NewVersion", prms, CancellationToken.None);
            return intTag.ToString(CultureInfo.InvariantCulture);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error upserting reminder row.");
            throw;
        }
    }
}