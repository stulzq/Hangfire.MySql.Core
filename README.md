# Hangfire.Oracle.Core Implementation

Hangfire.Oracle.Core is based on Hangfire.MySqlStorage(https://github.com/arnoldasgudas/Hangfire.MySqlStorage)

I fixed some bugs and support .net standard 2.0.

[![Build status](https://ci.appveyor.com/api/projects/status/fuhr415en9uu89h7?svg=true)](https://ci.appveyor.com/project/AhmetKoylu/hangfire-oracle-core)
[![Latest version](https://img.shields.io/nuget/v/Hangfire.Oracle.Core.svg)](https://www.nuget.org/packages/Hangfire.Oracle.Core/) 

Oracle storage implementation of [Hangfire](http://hangfire.io/) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.

**Some features of Oracle storage implementation is under development!**

## Installation
Install Oracle

Run the following command in the NuGet Package Manager console to install Hangfire.Oracle.Core:

```
Install-Package Hangfire.Oracle.Core
```

## Usage

Use one the following ways to initialize `OracleStorage`: 
- Create new instance of `OracleStorage` with connection string constructor parameter and pass it to `Configuration` with `UseStorage` method:
```
  GlobalConfiguration.Configuration.UseStorage(
    new OracleStorage(connectionString));
```
- Alternatively one or more options can be passed as a parameter to `OracleStorage`:
```
GlobalConfiguration.Configuration.UseStorage(
    new OracleStorage(
        connectionString, 
        new OracleStorageOptions
        {
            TransactionIsolationLevel = IsolationLevel.ReadCommitted,
            QueuePollInterval = TimeSpan.FromSeconds(15),
            JobExpirationCheckInterval = TimeSpan.FromHours(1),
            CountersAggregateInterval = TimeSpan.FromMinutes(5),
            PrepareSchemaIfNecessary = true,
            DashboardJobListLimit = 50000,
            TransactionTimeout = TimeSpan.FromMinutes(1),
            SchemaName = "HANGFIRE"
        }));
```
- With version 1.1 you can provide your own connection factory.
```
GlobalConfiguration.Configuration.UseStorage(
    new OracleStorage(
        () => new OracleConnection(connectionString), 
        new OracleStorageOptions
        {
            SchemaName = "HANGFIRE"
        }));
```
Description of optional parameters:
- `TransactionIsolationLevel` - transaction isolation level. Default is read committed. Didn't test with other options!
- `QueuePollInterval` - job queue polling interval. Default is 15 seconds.
- `JobExpirationCheckInterval` - job expiration check interval (manages expired records). Default is 1 hour.
- `CountersAggregateInterval` - interval to aggregate counter. Default is 5 minutes.
- `PrepareSchemaIfNecessary` - if set to `true`, it creates database tables. Default is `true`.
- `DashboardJobListLimit` - dashboard job list limit. Default is 50000.
- `TransactionTimeout` - transaction timeout. Default is 1 minute.
- `SchemaName` - schema name. Default is empty

### How to limit number of open connections

Number of opened connections depends on Hangfire worker count. You can limit worker count by setting `WorkerCount` property value in `BackgroundJobServerOptions`:
```
app.UseHangfireServer(
   new BackgroundJobServerOptions
   {
      WorkerCount = 1
   });
```
More info: http://hangfire.io/features.html#concurrency-level-control

## Dashboard
Hangfire provides a dashboard
![Dashboard](https://camo.githubusercontent.com/f263ab4060a09e4375cc4197fb5bfe2afcacfc20/687474703a2f2f68616e67666972652e696f2f696d672f75692f64617368626f6172642d736d2e706e67)
More info: [Hangfire Overview](http://hangfire.io/overview.html#integrated-monitoring-ui)

## Build
Please use Visual Studio or any other tool of your choice to build the solution.

## Known Issues
Currently Install.sql is not deployed if DB objects are not existing. As a workaround run your scripts in database and give give CRUD grants to the user that is given in connection string.
