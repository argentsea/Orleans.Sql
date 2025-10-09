using ArgentSea.Sql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Storage;

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

    public static IGrainStorage CreateShards(IServiceProvider services, string providerName)
    {
        var optOrleans = services.GetOptionsByName<OrleansShardPersistenceOptions>(providerName);
        var optCluster = services.GetRequiredService<IOptions<ClusterOptions>>();
        var svcShards = services.GetRequiredService<SqlShardSets>();
        var svcLogger = services.GetRequiredService<ILogger<ArgentSeaShardGrainPersistence<SqlShardConnectionOptions>>>();
        return new ArgentSeaShardGrainPersistence<SqlShardConnectionOptions>(svcShards, optOrleans, optCluster.Value, svcLogger);
    }
}