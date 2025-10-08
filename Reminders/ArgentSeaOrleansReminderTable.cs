using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Text;
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

        var prms = new ParameterCollection()
            .AddSqlVarBinaryInputParameter("GrainId", grainId.Key.Value.ToArray(), 1023)
            .AddSqlNVarCharInputParameter("GrainType", grainId.Type.ToString(), 1023)
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
            if (grainIdObj is GrainId gid)
            {
                return new ReminderEntry() { GrainId = gid };
            }
            return null;
        }
        var gType = new GrainType(UTF8Encoding.UTF8.GetBytes((string)rdr[1]));
        var buffer = new byte[1023];
        var bytesRead = rdr.GetBytes(0, 0L, buffer, 0, 1023);
        var grainKey = new IdSpan(new ReadOnlySpan<byte>(buffer).Slice(0, (int)bytesRead).ToArray());

        return new ReminderEntry()
        {
            GrainId = new GrainId(gType, grainKey),
            ReminderName = rdr.GetString(2),
            StartAt = rdr.GetDateTime(3),
            Period = new TimeSpan(rdr.GetInt64(4)),
            ETag = rdr.GetInt32(5).ToString(CultureInfo.InvariantCulture),
        };
    }

    private static object ReadRowsHandler(object instance, short shardId, string sprocName, ConcurrentBag<ReminderEntry> bagEntries, DbDataReader rdr, DbParameterCollection prms, string connectionDescription, ILogger logger)
    {
        if (bagEntries is null)
        {
            throw new ArgumentNullException(nameof(bagEntries));
        }

        if (!rdr.HasRows || rdr.IsClosed || rdr.IsDBNull(0))
        {
            return null!;
        }

        if (!rdr.Read() || rdr.IsDBNull(1))
        {
            return null!;
        }

        var gType = new GrainType(UTF8Encoding.UTF8.GetBytes((string)rdr[1]));
        var buffer = new byte[1023];
        var bytesRead = rdr.GetBytes(0, 0L, buffer, 0, 1023);
        var grainKey = new IdSpan(new ReadOnlySpan<byte>(buffer).Slice(0, (int)bytesRead).ToArray());

        bagEntries.Add(new ReminderEntry()
        {
            GrainId = new GrainId(gType, grainKey),
            ReminderName = rdr.GetString(2),
            StartAt = rdr.GetDateTime(3),
            Period = new TimeSpan(rdr.GetInt64(4)),
            ETag = rdr.GetInt32(5).ToString(CultureInfo.InvariantCulture),
        });
        return null!;
    }


    public async Task<ReminderTableData> ReadRows(GrainId grainId)
    {
        var prms = new ParameterCollection()
            .AddSqlVarBinaryInputParameter("GrainKey", grainId.Key.Value.ToArray(), 1023)
            .AddSqlNVarCharInputParameter("GrainType", grainId.Type.ToString(), 1023);

        var shardId = grainId.ShardId();
        try
        {
            var result = new ConcurrentBag<ReminderEntry>();
            await shardSet[shardId].Read.QueryAsync<ConcurrentBag<ReminderEntry>, object>(Queries.OrleansReminderReadRowsKey, prms, ReadRowsHandler, false, result, CancellationToken.None); // Using TArg to pass a shared list to which handler can append. Ignoring return value.
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
            .AddSqlBigIntInputParameter("@BeginHash", (long)begin)
            .AddSqlBigIntInputParameter("@EndHash", (long)end);
        try
        {
            var result = new ConcurrentBag<ReminderEntry>();
            await shardSet.ReadAll.QueryAsync<ConcurrentBag<ReminderEntry>, object>(Queries.OrleansReminderReadRangeRows1Key, prms, ReadRowsHandler, result, CancellationToken.None); // Using TArg to pass a shared list to which handler can append. Ignoring return value.
            return new ReminderTableData(result);
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
            .AddSqlVarBinaryInputParameter("GrainKey", grainId.Key.Value.ToArray(), 1023)
            .AddSqlNVarCharInputParameter("GrainType", grainId.Type.ToString(), 1023)
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
            .AddSqlVarBinaryInputParameter("GrainKey", entry.GrainId.Key.Value.ToArray(), 1023)
            .AddSqlNVarCharInputParameter("GrainType", entry.GrainId.Type.ToString(), 1023)
            .AddSqlNVarCharInputParameter("@ReminderName", entry.ReminderName, 150)
            .AddSqlDateTime2InputParameter("@StartTime", entry.StartAt)
            .AddSqlBigIntInputParameter("@Period", entry.Period.Ticks)
            .AddSqlBigIntInputParameter("@GrainHash", (long)entry.GrainId.GetUniformHashCode())
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