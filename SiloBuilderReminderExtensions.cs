using ArgentSea.Orleans;
using Microsoft.Extensions.DependencyInjection;

namespace ArgentSea.Orleans.Sql;

public static class SiloBuilderReminderExtensions
{
    /// <summary>
    /// Adds reminder storage backed by ArgentSea.
    /// </summary>
    /// <param name="builder">
    /// The builder.
    /// </param>
    /// <param name="configure">
    /// The delegate used to configure the reminder store.
    /// </param>
    /// <returns>
    /// The provided <see cref="ISiloBuilder"/>, for chaining.
    /// </returns>
    public static ISiloBuilder UseArgentSeaReminderService(this ISiloBuilder builder, Action<ReminderOptions> configure)
    {
        builder.ConfigureServices(services => services.UseArgentSeaReminderService(configure));
        return builder;
    }

    /// <summary>
    /// Adds reminder storage backed by ArgentSea.
    /// </summary>
    /// <param name="services">
    /// The service collection.
    /// </param>
    /// <param name="configure">
    /// The delegate used to configure the reminder store.
    /// </param>
    /// <returns>
    /// The provided <see cref="IServiceCollection"/>, for chaining.
    /// </returns>
    public static IServiceCollection UseArgentSeaReminderService(this IServiceCollection services, Action<ReminderOptions> configure)
    {
        services.AddReminders();
        services.Configure(configure);
        services.AddSingleton<IReminderTable, ArgentSeaOrleansReminderTable>();
        services.AddSingleton<IConfigurationValidator, ArgentSeaReminderOptionsValidator>();
        //services.ConfigureFormatter<ArgentSeaReminderTableOptions>();
        return services;
    }
}