using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Storage;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using ArgentSea.Sql;

namespace ArgentSea.Orleans.Sql;

public static class ArgentSeaGrainStorageFactory
{
    public static IGrainStorage CreateDb(IServiceProvider services, string name)
    {
        var optOrleans = services.GetRequiredService<IOptions<OrleansDbPersistenceOptions>>();
        var optCluster = services.GetRequiredService<IOptions<ClusterOptions>>();
        var svcDatabases = services.GetRequiredService<SqlDatabases>();
        var svcLogger = services.GetRequiredService<ILogger<ArgentSeaDbGrainPersistence<SqlDbConnectionOptions>>>();
        return new ArgentSeaDbGrainPersistence<SqlDbConnectionOptions>(svcDatabases, optOrleans, optCluster, svcLogger);
    }

    public static IGrainStorage CreateShards(IServiceProvider services, string name)
    {
        var optOrleans = services.GetRequiredService<IOptions<OrleansShardPersistenceOptions>>();
        var optCluster = services.GetRequiredService<IOptions<ClusterOptions>>();
        var svcShards = services.GetRequiredService<SqlShardSets>();
        var svcLogger = services.GetRequiredService<ILogger<ArgentSeaShardGrainPersistence<SqlShardConnectionOptions>>>();
        return new ArgentSeaShardGrainPersistence<SqlShardConnectionOptions>(svcShards, optOrleans, optCluster, svcLogger);
    }
}