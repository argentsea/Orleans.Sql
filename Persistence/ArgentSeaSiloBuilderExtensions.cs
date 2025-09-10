using ArgentSea.Orleans;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime.Hosting;
using ArgentSea.Orleans.Sql;

namespace Orleans.Hosting;

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
            services => services.AddArgentSeaDbOrleansGrainStorage(providerName, ob => ob.Configure(options) ));

    public static IServiceCollection AddArgentSeaDbOrleansGrainStorage(
        this IServiceCollection services,
        string providerName,
        Action<OptionsBuilder<OrleansDbPersistenceOptions>> options)
    {
        options?.Invoke(services.AddOptions<OrleansDbPersistenceOptions>(providerName));
        services.ConfigureNamedOptionForLogging<OrleansDbPersistenceOptions>(providerName);
        services.AddGrainStorage(providerName, ArgentSeaGrainStorageFactory.CreateDb);
        return services;
    }

    public static ISiloBuilder AddArgentSeaShardOrleansGrainStorage(
        this ISiloBuilder builder,
        string providerName,
        Action<OrleansShardPersistenceOptions> options) => builder.ConfigureServices(
            services => services.AddArgentSeaShardOrleansGrainStorage(providerName, ob => ob.Configure(options)));

    public static ISiloBuilder AddArgentSeaShardOrleansGrainStorage(
        this ISiloBuilder builder,
        string providerName) => builder.ConfigureServices(
            services => services.AddArgentSeaShardOrleansGrainStorage(providerName, _ => { }));

    public static IServiceCollection AddArgentSeaShardOrleansGrainStorage(
        this IServiceCollection services,
        string providerName,
        Action<OptionsBuilder<OrleansShardPersistenceOptions>> options)
    {
        options?.Invoke(services.AddOptions<OrleansShardPersistenceOptions>(providerName));
        services.ConfigureNamedOptionForLogging<OrleansShardPersistenceOptions>(providerName);
        services.AddGrainStorage(providerName, ArgentSeaGrainStorageFactory.CreateShards);
        return services;
    }

}