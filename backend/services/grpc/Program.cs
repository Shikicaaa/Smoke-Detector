using Grpc.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options =>
{
    options.EnableDetailedErrors = true;
    options.MaxReceiveMessageSize = 16 * 1024 * 1024;
    options.MaxSendMessageSize = 16 * 1024 * 1024;
});

builder.Services.AddGrpcReflection();

var app = builder.Build();

app.MapGrpcService<SensorService>();
app.MapGrpcReflectionService();

app.MapGet("/", () => "gRPC SensorService is running. Use a gRPC client to connect.");

app.Run();