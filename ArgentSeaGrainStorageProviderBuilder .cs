using ArgentSea.Orleans;
using ArgentSea.Orleans.Sql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers;
using Orleans.Runtime.Hosting;

[assembly: RegisterProvider("ArgentSea", "GrainStorage", "Silo", typeof(ArgentSeaGrainStorageDbProviderBuilder))]
[assembly: RegisterProvider("ArgentSea", "GrainStorage", "Silo", typeof(ArgentSeaGrainStorageShardProviderBuilder))]

namespace ArgentSea.Orleans;

internal sealed class ArgentSeaGrainStorageDbProviderBuilder : IProviderBuilder<ISiloBuilder>
{
    public void Configure(ISiloBuilder builder, string dataProviderName, IConfigurationSection configurationSection) => builder.ConfigureServices(services =>
    {
        var section = builder.Configuration.GetRequiredSection("OrleansData");
        builder.Services.Configure<OrleansDbPersistenceOptions>(dataProviderName, opts =>
        {
            opts.DatabaseKey = section["DatabaseKey"] ?? "Default";
        });
        services.AddGrainStorage(dataProviderName, ArgentSeaGrainStorageFactory.CreateDb);
        return;
    });
}

internal sealed class ArgentSeaGrainStorageShardProviderBuilder : IProviderBuilder<ISiloBuilder>
{
    public void Configure(ISiloBuilder builder, string dataProviderName, IConfigurationSection configurationSection) => builder.ConfigureServices(services =>
    {
        var section = builder.Configuration.GetRequiredSection("OrleansData");
        builder.Services.Configure<OrleansShardPersistenceOptions>(dataProviderName, opts =>
        {
            opts.ShardSetKey = section["ShardSetKey"] ?? "Default";
        });
        services.AddGrainStorage(dataProviderName, ArgentSeaGrainStorageFactory.CreateShards);
        return;
    });
}
