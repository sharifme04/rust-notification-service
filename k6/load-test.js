import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter, Trend } from 'k6/metrics';

export const options = {
  stages: [
    { duration: '10s', target: 10 },   // ramp up
    { duration: '30s', target: 100 },  // sustain 100 notifications/s
    { duration: '10s', target: 0 },    // ramp down
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],        // < 1% errors
    http_req_duration: ['p(95)<500'],      // 95th pct under 500 ms
    notification_created: ['count>2000'],  // created enough during test
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const TOKEN = __ENV.JWT_TOKEN || 'local-dev-secret';

const notificationCreated = new Counter('notification_created');
const createDuration = new Trend('create_duration_ms');
const errorRate = new Rate('error_rate');

const USER_IDS = [
  '11111111-1111-1111-1111-111111111111',
  '22222222-2222-2222-2222-222222222222',
  '33333333-3333-3333-3333-333333333333',
  '44444444-4444-4444-4444-444444444444',
  '55555555-5555-5555-5555-555555555555',
];

const EVENT_TYPES = [
  'order.shipped',
  'order.delivered',
  'payment.confirmed',
  'account.alert',
  'promotion.offer',
];

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export default function () {
  const userId = randomItem(USER_IDS);
  const eventType = randomItem(EVENT_TYPES);

  const payload = JSON.stringify({
    user_id: userId,
    event_type: eventType,
    title: `Test notification [${eventType}]`,
    body: `Load test at ${new Date().toISOString()}`,
    metadata: { vu: __VU, iter: __ITER },
  });

  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${TOKEN}`,
  };

  const start = Date.now();
  const res = http.post(`${BASE}/api/v1/notifications`, payload, { headers });
  createDuration.add(Date.now() - start);

  const ok = check(res, {
    'status 200': (r) => r.status === 200,
    'has id': (r) => !!JSON.parse(r.body || '{}').id,
  });

  if (ok) {
    notificationCreated.add(1);
    errorRate.add(0);
  } else {
    errorRate.add(1);
  }

  sleep(0.01); // 10 ms back-off keeps rate near 100 rps per VU distribution
}
