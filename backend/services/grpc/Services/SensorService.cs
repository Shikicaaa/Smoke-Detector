using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Npgsql;
using SensorGrpc;
 
namespace SensorGrpc.Services;
 
public class SensorService : global::SensorGrpc.SensorService.SensorServiceBase
{
    private readonly string _connectionString;
    private readonly ILogger<SensorService> _logger;

    public SensorService(IConfiguration config, ILogger<SensorService> logger)
    {
        _connectionString = config.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("DefaultConnection is not set.");
        _logger = logger;
    }

    public override async Task<IngestResponse> Ingest(
        IngestRequest request, ServerCallContext context)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var r = request.Reading;
        await using var cmd = new NpgsqlCommand(@"
            INSERT INTO sensor_data (
                time, temperature_c, humidity_percent, tvoc_ppb, eco2_ppm,
                raw_h2, raw_ethanol, pressure_hpa, pm10, pm25,
                nc05, nc10, nc25, fire_alarm
            ) VALUES (
                @time, @temperature_c, @humidity_percent, @tvoc_ppb, @eco2_ppm,
                @raw_h2, @raw_ethanol, @pressure_hpa, @pm10, @pm25,
                @nc05, @nc10, @nc25, @fire_alarm
            ) ON CONFLICT (time) DO NOTHING", conn);

        AddSensorParams(cmd, r);
        await cmd.ExecuteNonQueryAsync();

        return new IngestResponse { Success = true };
    }


    public override async Task<IngestBatchResponse> IngestBatch(
        IngestBatchRequest request, ServerCallContext context)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        await using var tx = await conn.BeginTransactionAsync();
        int inserted = 0;

        foreach (var r in request.Readings)
        {
            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO sensor_data (
                    time, temperature_c, humidity_percent, tvoc_ppb, eco2_ppm,
                    raw_h2, raw_ethanol, pressure_hpa, pm10, pm25,
                    nc05, nc10, nc25, fire_alarm
                ) VALUES (
                    @time, @temperature_c, @humidity_percent, @tvoc_ppb, @eco2_ppm,
                    @raw_h2, @raw_ethanol, @pressure_hpa, @pm10, @pm25,
                    @nc05, @nc10, @nc25, @fire_alarm
                ) ON CONFLICT (time) DO NOTHING", conn, tx);

            AddSensorParams(cmd, r);
            inserted += await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
        return new IngestBatchResponse { Success = true, Inserted = inserted };
    }

    public override async Task<SelectiveResponse> GetSelective(
        SelectiveRequest request, ServerCallContext context)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @"
            SELECT time, temperature_c, humidity_percent
            FROM sensor_data
            WHERE (@start_time::timestamptz IS NULL OR time >= @start_time)
              AND (@end_time::timestamptz IS NULL OR time <= @end_time)
            ORDER BY time DESC
            LIMIT @limit";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("start_time",
            request.StartTime != null
                ? (object)request.StartTime.ToDateTime()
                : DBNull.Value);
        cmd.Parameters.AddWithValue("end_time",
            request.EndTime != null
                ? (object)request.EndTime.ToDateTime()
                : DBNull.Value);
        cmd.Parameters.AddWithValue("limit", request.Limit > 0 ? request.Limit : 100);

        var response = new SelectiveResponse();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            response.Readings.Add(new SelectiveReading
            {
                Time = Timestamp.FromDateTime(reader.GetDateTime(0).ToUniversalTime()),
                TemperatureC = reader.IsDBNull(1) ? null : reader.GetDouble(1),
                HumidityPercent = reader.IsDBNull(2) ? null : reader.GetDouble(2),
            });
        }

        return response;
    }

    public override async Task<AggregateResponse> GetAggregate(
        AggregateRequest request, ServerCallContext context)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var interval = string.IsNullOrWhiteSpace(request.BucketInterval)
            ? "1 hour" : request.BucketInterval;
        var limit = request.Limit > 0 ? request.Limit : 100;

        var conditions = new List<string>();
        if (request.StartTime != null)
            conditions.Add("time >= @start_time");
        if (request.EndTime != null)
            conditions.Add("time <= @end_time");

        var where = conditions.Count > 0
            ? "WHERE " + string.Join(" AND ", conditions) : "";

        var sql = $@"
            SELECT
                time_bucket(CAST(@interval AS INTERVAL), time) AS bucket,
                AVG(temperature_c) AS avg_temperature_c,
                AVG(humidity_percent) AS avg_humidity_percent,
                AVG(pressure_hpa) AS avg_pressure_hpa,
                AVG(tvoc_ppb) AS avg_tvoc_ppb,
                AVG(eco2_ppm) AS avg_eco2_ppm,
                AVG(pm25) AS avg_pm25,
                AVG(pm10) AS avg_pm10,
                COUNT(*) FILTER (WHERE fire_alarm) AS fire_alarm_count
            FROM sensor_data
            {where}
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT @limit";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("interval", interval);
        cmd.Parameters.AddWithValue("limit", limit);
        if (request.StartTime != null)
            cmd.Parameters.AddWithValue("start_time", request.StartTime.ToDateTime());
        if (request.EndTime != null)
            cmd.Parameters.AddWithValue("end_time", request.EndTime.ToDateTime());

        var response = new AggregateResponse();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            response.Buckets.Add(new AggregationBucket
            {
                Bucket = Timestamp.FromDateTime(reader.GetDateTime(0).ToUniversalTime()),
                AvgTemperatureC = reader.IsDBNull(1) ? null : reader.GetDouble(1),
                AvgHumidityPercent = reader.IsDBNull(2) ? null : reader.GetDouble(2),
                AvgPressureHpa = reader.IsDBNull(3) ? null : reader.GetDouble(3),
                AvgTvocPpb = reader.IsDBNull(4) ? null : reader.GetDouble(4),
                AvgEco2Ppm = reader.IsDBNull(5) ? null : reader.GetDouble(5),
                AvgPm25 = reader.IsDBNull(6) ? null : reader.GetDouble(6),
                AvgPm10 = reader.IsDBNull(7) ? null : reader.GetDouble(7),
                FireAlarmCount = reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
            });
        }

        return response;
    }

    public override async Task<SensorReading> GetLatest(
        LatestRequest request, ServerCallContext context)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand(@"
            SELECT * FROM sensor_data ORDER BY time DESC LIMIT 1", conn);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            throw new RpcException(new Status(StatusCode.NotFound, "No sensor data found."));

        return ReadSensorReading(reader);
    }

    public override async Task<RangeResponse> GetRange(
        RangeRequest request, ServerCallContext context)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var limit = request.Limit > 0 ? request.Limit : 1000;

        await using var cmd = new NpgsqlCommand(@"
            SELECT * FROM sensor_data
            WHERE time >= @start_time AND time <= @end_time
            ORDER BY time ASC
            LIMIT @limit", conn);

        cmd.Parameters.AddWithValue("start_time", request.StartTime.ToDateTime());
        cmd.Parameters.AddWithValue("end_time", request.EndTime.ToDateTime());
        cmd.Parameters.AddWithValue("limit", limit);

        var response = new RangeResponse();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            response.Readings.Add(ReadSensorReading(reader));

        return response;
    }

    private static void AddSensorParams(NpgsqlCommand cmd, SensorReading r)
    {
        cmd.Parameters.AddWithValue("time", r.Time.ToDateTime());
        cmd.Parameters.AddWithValue("temperature_c", r.TemperatureC is not null ? (object)r.TemperatureC.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("humidity_percent", r.HumidityPercent is not null ? (object)r.HumidityPercent.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("tvoc_ppb", r.TvocPpb is not null ? (object)r.TvocPpb.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("eco2_ppm", r.Eco2Ppm is not null ? (object)r.Eco2Ppm.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("raw_h2", r.RawH2 is not null ? (object)r.RawH2.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("raw_ethanol", r.RawEthanol is not null ? (object)r.RawEthanol.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("pressure_hpa", r.PressureHpa is not null ? (object)r.PressureHpa.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("pm10", r.Pm10 is not null ? (object)r.Pm10.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("pm25", r.Pm25 is not null ? (object)r.Pm25.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("nc05", r.Nc05 is not null ? (object)r.Nc05.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("nc10", r.Nc10 is not null ? (object)r.Nc10.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("nc25", r.Nc25 is not null ? (object)r.Nc25.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("fire_alarm", r.FireAlarm is not null ? (object)r.FireAlarm.Value : DBNull.Value);
    }

    private static SensorReading ReadSensorReading(NpgsqlDataReader reader)
    {
        var reading = new SensorReading
        {
            Time = Timestamp.FromDateTime(reader.GetDateTime(reader.GetOrdinal("time")).ToUniversalTime()),
        };

        SetNullableDouble(reader, "temperature_c", v => reading.TemperatureC = v);
        SetNullableDouble(reader, "humidity_percent", v => reading.HumidityPercent = v);
        SetNullableInt (reader, "tvoc_ppb", v => reading.TvocPpb = v);
        SetNullableInt (reader, "eco2_ppm", v => reading.Eco2Ppm = v);
        SetNullableInt (reader, "raw_h2", v => reading.RawH2 = v);
        SetNullableInt (reader, "raw_ethanol", v => reading.RawEthanol = v );
        SetNullableDouble(reader, "pressure_hpa", v => reading.PressureHpa = v);
        SetNullableDouble(reader, "pm10", v => reading.Pm10 = v);
        SetNullableDouble(reader, "pm25", v => reading.Pm25 = v);
        SetNullableDouble(reader, "nc05", v => reading.Nc05 = v);
        SetNullableDouble(reader, "nc10", v => reading.Nc10 = v);
        SetNullableDouble(reader, "nc25", v => reading.Nc25 = v);
        SetNullableBool  (reader, "fire_alarm", v => reading.FireAlarm = v);

        return reading;
    }

    private static void SetNullableDouble(NpgsqlDataReader r, string col, Action<double> set)
    {
        var ord = r.GetOrdinal(col);
        if (!r.IsDBNull(ord)) set(r.GetDouble(ord));
    }

    private static void SetNullableInt(NpgsqlDataReader r, string col, Action<int> set)
    {
        var ord = r.GetOrdinal(col);
        if (!r.IsDBNull(ord)) set(r.GetInt32(ord));
    }

    private static void SetNullableBool(NpgsqlDataReader r, string col, Action<bool> set)
    {
        var ord = r.GetOrdinal(col);
        if (!r.IsDBNull(ord)) set(r.GetBoolean(ord));
    }
}