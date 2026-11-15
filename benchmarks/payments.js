import http from 'k6/http';
import { check, sleep } from 'k6';
export const options = { vus: 10, duration: '30s', thresholds: { http_req_failed: ['rate<0.05'] } };
export default function () {
 const id = `${__VU}-${__ITER}-${Date.now()}`;
 const res = http.post(`${__ENV.API || 'http://localhost:8081'}/payments`,
   JSON.stringify({amount_cents: 1000}),
   {headers: {'Content-Type':'application/json', 'Idempotency-Key':id}});
 check(res, {'accepted': r => r.status === 202});
 sleep(0.1);
}
