import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    summaryTrendStats: ['avg', 'p(95)'],
    vus: 10,
    duration: '30s', 
};

const BASE_URL = __ENV.REST_URL || 'http://localhost:8000/api/v1/sensor';
const SCENARIO = __ENV.SCENARIO || 'A';

export default function () {
    if (SCENARIO === 'A') {
        const payload = JSON.stringify({
            time: new Date().toISOString(),
            temperature_c: Math.random() * 40,
            humidity_percent: Math.random() * 100,
            tvoc_ppb: 150,
            eco2_ppm: 400,
            fire_alarm: false
        });

        const params = { headers: { 'Content-Type': 'application/json' } };
        const res = http.post(`${BASE_URL}/ingest`, payload, params);

        check(res, { 'status is 201': (r) => r.status === 201 });
    
    } else if (SCENARIO === 'B') {
        const res = http.get(`${BASE_URL}/selective`);
        check(res, { 'status is 200': (r) => r.status === 200 });

    } else if (SCENARIO === 'C') {
        const res = http.get(`${BASE_URL}/aggregate?bucket_interval=1%20hour&limit=100`);
        check(res, { 'status is 200': (r) => r.status === 200 });
    }

    sleep(0.01); 
}