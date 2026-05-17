import http from 'k6/http';
import { check, sleep } from 'k6';
import { Options } from 'k6/options';

export const options: Options = {
    summaryTrendStats: ['avg', 'p(95)'],
    vus: 10,
    duration: '30s', 
};

const BASE_URL: string = __ENV.GQL_URL || 'http://localhost:8001/graphql';
const SCENARIO: string = __ENV.SCENARIO || 'A';

export default function (): void {
    let query: string = '';
    let variables: Record<string, any> = {};

    if (SCENARIO === 'A') {
        const randomOffsetMs = Math.floor(Math.random() * 2592000000); 
        const uniqueTime = new Date(Date.now() - randomOffsetMs).toISOString();

        query = `
            mutation Ingest($data: SensorDataInput!) {
                ingest(data: $data) {
                    time
                    temperature_c
                }
            }`;
        
        variables = {
            data: {
                time: uniqueTime,
                temperature_c: Math.random() * 40,
                humidity_percent: Math.random() * 100,
                fire_alarm: false
            }
        };

    } else if (SCENARIO === 'B') {
        query = `
            query {
                latest {
                    temperature_c
                    humidity_percent
                }
            }`;
            
    } else if (SCENARIO === 'C') {
        query = `
            query Aggregate($bucketInterval: String!, $limit: Int!) {
                aggregate(bucketInterval: $bucketInterval, limit: $limit) {
                    bucket
                    avg_temperature_c
                    avg_humidity_percent
                    avg_eco2_ppm
                }
            }`;
        variables = { 
            bucketInterval: "1 hour", 
            limit: 100 
        };
    }

    const payload: string = JSON.stringify({ query, variables });
    const params = { headers: { 'Content-Type': 'application/json' } };
    const res = http.post(BASE_URL, payload, params);

    const body = JSON.parse(res.body as string);

    check(res, {
        'http status is 200': (r) => r.status === 200,
        'no gql errors': () => !body.errors,
        'has data': () => body.data !== null && body.data !== undefined,
    });
    
    sleep(0.01);
}