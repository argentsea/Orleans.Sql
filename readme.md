# ArgentSea Orleans SQL Implementation


Example appsettings.json sections in the Silos project:

```json
    "OrleansConfig": {
        "ClusterId": "default",
        "ServiceId": "default"
    },
    "SqlDbConnections": [
        {
            "DatabaseKey": "Common",
            "InitialCatalog": "Common"
        }
    ],
    "SqlShardSets": [
        {
            "ShardSetName": "Default",
            "DefaultShardId": 0,
            "UserName": "orleansWriter",
            "Read": {
                "ApplicationIntent": "ReadOnly",
                "UserName": "orleansReader"
            },
            "Shards": [
                {
                    "ShardId": 0,
                    "InitialCatalog": "Tenant1"
                },
                {
                    "ShardId": 1,
                    "InitialCatalog": "Tenant2"
                }
            ]
        }
    ],
    "OrleansData": {
        "ShardSetKey": "Default",
        "DatabaseKey": "Common",
        "ValidateGrainKeys": false
    },
```



An example startup:
```C#
builder.UseOrleans((context, siloBuilder) =>
{
    siloBuilder.Services.AddSqlServices(context.Configuration); //ArgentSea
    siloBuilder.Services.AddSingleton<IFieldCodec<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    siloBuilder.Services.AddSingleton<IDeepCopier<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    siloBuilder.Services.AddOptions();
    siloBuilder.Services.Configure<OrleansShardPersistenceOptions>(DataProviderConstants.Tenant, options =>
    {
        options.Queries.Add("Scale", ScaleGrainQueries.ScaleQueryDefinition);
    });
    siloBuilder.Services.Configure<OrleansDbPersistenceOptions>(DataProviderConstants.Common, options =>
    {
        //options.Queries.Add("Users", UserGrainQueries.UserQueryDefinition);
    });
});

```

## Configuring the Orleans Client (API)

Your Program.cs startup class should include:

```C#
builder.UseOrleansClient(client => {
    client.Configure<ClientMessagingOptions>(opts =>
    {
        opts.ResponseTimeoutWithDebugger = TimeSpan.FromSeconds(10);
    });
    client.Services.AddSingleton<IFieldCodec<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    client.Services.AddSingleton<IDeepCopier<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    client.Services.AddSingleton<IGatewayListProvider, ArgentSeaGatewayListProvider>();
});
```

You will also need to add to appsettings.json — or other configuration — information about how to connect to the Common database:
```json
    "ClusteringClientOptions": {
        "MaxRefreshInterval": "0.00:02:00",
        "ConnectionDatabase": "Common",
        "ProcedureName": "rdr.OrleansClusterGatewayListV1"
    }
```
