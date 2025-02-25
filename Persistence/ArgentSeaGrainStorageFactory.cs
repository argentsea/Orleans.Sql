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
        var optOrleans = services.GetRequiredService<IOptionsMonitor<OrleansDbPersistenceOptions>>();
        var optCluster = services.GetRequiredService<IOptionsMonitor<ClusterOptions>>();
        var svcDatabases = services.GetRequiredService<SqlDatabases>();
        var svcLogger = services.GetRequiredService<ILogger<ArgentSeaDbGrainPersistence<SqlDbConnectionOptions>>>();
        return ActivatorUtilities.CreateInstance<ArgentSeaDbGrainPersistence<SqlDbConnectionOptions>>(services, svcDatabases, optOrleans.Get(name), optCluster.Get(name), svcLogger);
    }

    public static IGrainStorage CreateShards(IServiceProvider services, string name)
    {
        var optOrleans = services.GetRequiredService<IOptionsMonitor<OrleansShardPersistenceOptions>>();
        var optCluster = services.GetRequiredService<IOptionsMonitor<ClusterOptions>>();
        var svcShards = services.GetRequiredService<SqlShardSets>();
        var svcLogger = services.GetRequiredService<ILogger<ArgentSeaShardGrainPersistence<SqlShardConnectionOptions>>>();
        return ActivatorUtilities.CreateInstance<ArgentSeaShardGrainPersistence<SqlShardConnectionOptions>>(services, svcShards, Options.Create(optOrleans.Get(name)), Options.Create(optCluster.Get(name)), svcLogger);
    }
}