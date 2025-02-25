using Microsoft.Extensions.DependencyInjection;
using ArgentSea.Orleans;

namespace ArgentSea.Orleans.Sql;

public static class ArgentSeaSiloBuilderExtensions
{
    public static ISiloBuilder AddArgentSeaDbOrleansGrainStorage(
        this ISiloBuilder builder,
        string providerName) => builder.ConfigureServices(
            services => services.AddArgentSeaDbOrleansGrainStorage(providerName, _ => { }));

    public static ISiloBuilder AddArgentSeaDbOrleansGrainStorage(
        this ISiloBuilder builder,
        string providerName,
        Action<OrleansDbPersistenceOptions> options) => builder.ConfigureServices(
            services => services.AddArgentSeaDbOrleansGrainStorage(providerName, options));

    public static IServiceCollection AddArgentSeaDbOrleansGrainStorage(
        this IServiceCollection services,
        string providerName,
        Action<OrleansDbPersistenceOptions> options)
    {
        services.AddOptions<OrleansDbPersistenceOptions>(providerName).Configure(options);
        services.AddKeyedSingleton(providerName, ArgentSeaGrainStorageFactory.CreateDb);
        return services;
    }

    public static ISiloBuilder AddArgentSeaShardOrleansGrainStorage(
        this ISiloBuilder builder,
        string providerName,
        Action<OrleansShardPersistenceOptions> options) => builder.ConfigureServices(
            services => services.AddArgentSeaShardOrleansGrainStorage(providerName, options));

    public static ISiloBuilder AddArgentSeaShardOrleansGrainStorage(
        this ISiloBuilder builder,
        string providerName) => builder.ConfigureServices(
            services => services.AddArgentSeaShardOrleansGrainStorage(providerName, _ => { }));

    public static IServiceCollection AddArgentSeaShardOrleansGrainStorage(
        this IServiceCollection services,
        string providerName,
        Action<OrleansShardPersistenceOptions> options)
    {
        services.AddOptions<OrleansShardPersistenceOptions>(providerName).Configure(options);
        services.AddKeyedSingleton(providerName, ArgentSeaGrainStorageFactory.CreateShards);
        return services;
    }

}