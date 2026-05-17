import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { Options } from 'k6/options';

const client = new grpc.Client();

client.load(['.'], 'sensor.proto');

export const options: Options = {
    summaryTrendStats: ['avg', 'p(95)'],
    vus: 10,
    duration: '30s',
};

const BASE_URL: string = __ENV.GRPC_URL || 'localhost:8002';
const SCENARIO: string = __ENV.SCENARIO || 'A';

export default function (): void {
    client.connect(BASE_URL, {
        plaintext: true
    });

    if (SCENARIO === 'A') {
        const randomOffsetMs = Math.floor(Math.random() * 2592000000);
        const timestampMs = Date.now() - randomOffsetMs;

        const data = {
            reading: {
                time: {
                    seconds: Math.floor(timestampMs / 1000),
                    nanos: (timestampMs % 1000) * 1000000 
                },
                temperature_c: { value: Math.random() * 40 },
                humidity_percent: { value: Math.random() * 100 },
                tvoc_ppb: { value: 150 },
                eco2_ppm: { value: 400 },
                fire_alarm: { value: false }
            }
        };

        const response = client.invoke('sensor.SensorService/Ingest', data);
        
        check(response, { 'status is OK': (r) => r && r.status === grpc.StatusOK });

    } else if (SCENARIO === 'B') {
        const data = { 
            limit: 100 
        }; 
        const response = client.invoke('sensor.SensorService/GetSelective', data);
        check(response, { 'status is OK': (r) => r && r.status === grpc.StatusOK });

    } else if (SCENARIO === 'C') {
        const data = {
            bucket_interval: "1 hour",
            limit: 100
        };
        const response = client.invoke('sensor.SensorService/Aggregate', data);
        check(response, { 'status is OK': (r) => r && r.status === grpc.StatusOK });
    }

    client.close();
    
    sleep(0.01);
}