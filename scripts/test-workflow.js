// End-to-end workflow test for Kafka microservices

const http = require('http');

const BASE_URLS = {
  userService: process.env.USER_SERVICE_URL || 'http://localhost:3001',
  orderService: process.env.ORDER_SERVICE_URL || 'http://localhost:3002',
  paymentService: process.env.PAYMENT_SERVICE_URL || 'http://localhost:3003',
  notificationService: process.env.NOTIFICATION_SERVICE_URL || 'http://localhost:3004',
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeRequest(url, method = 'GET', body = null) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      port: urlObj.port,
      path: urlObj.pathname,
      method,
      headers: {
        'Content-Type': 'application/json',
      },
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        try {
          resolve({
            status: res.statusCode,
            data: JSON.parse(data),
          });
        } catch (error) {
          resolve({
            status: res.statusCode,
            data,
          });
        }
      });
    });

    req.on('error', reject);

    if (body) {
      req.write(JSON.stringify(body));
    }

    req.end();
  });
}

async function runWorkflow() {
  try {
    console.log('========================================');
    console.log('KAFKA MICROSERVICES WORKFLOW TEST');
    console.log('========================================\n');

    console.log('Step 1: Creating user');
    const userResponse = await makeRequest(`${BASE_URLS.userService}/users`, 'POST', {
      email: 'trayee@example.com',
      name: 'Trayee Ghosh',
      password: 'password123',
    });

    if (userResponse.status !== 201) {
      throw new Error(`Failed to create user: ${JSON.stringify(userResponse.data)}`);
    }

    const userId = userResponse.data.user.id;
    console.log('[OK] User created:', userResponse.data.user);
    await sleep(2000);

    console.log('\nStep 2: Creating order');
    const orderResponse = await makeRequest(`${BASE_URLS.orderService}/orders`, 'POST', {
      userId,
      items: [
        { productId: 'PROD-003', quantity: 2, price: 29.99 },
        { productId: 'PROD-004', quantity: 1, price: 49.99 },
      ],
      totalAmount: 109.97,
    });

    if (orderResponse.status !== 201) {
      throw new Error(`Failed to create order: ${JSON.stringify(orderResponse.data)}`);
    }

    const orderId = orderResponse.data.order.id;
    console.log('[OK] Order created:', orderResponse.data.order);
    await sleep(3000);

    console.log('\nStep 3: Checking payment status');
    const paymentResponse = await makeRequest(`${BASE_URLS.paymentService}/payments/${orderId}`);

    if (paymentResponse.status === 200) {
      console.log('[OK] Payment found:', paymentResponse.data);
    } else {
      console.log('[WARN] Payment not found yet (may still be processing)');
    }

    await sleep(1000);

    console.log('\nStep 4: Checking user notifications');
    const notificationsResponse = await makeRequest(
      `${BASE_URLS.notificationService}/notifications/user/${userId}`
    );

    if (notificationsResponse.status === 200) {
      const notifications = notificationsResponse.data;
      console.log(`[OK] Notifications found: ${notifications.length}`);
      notifications.forEach((notification, index) => {
        console.log(`  ${index + 1}. [${notification.type}] ${notification.message}`);
      });
    } else {
      console.log('[WARN] Could not fetch notifications');
    }

    console.log('\nStep 5: Notification stats');
    const statsResponse = await makeRequest(`${BASE_URLS.notificationService}/notifications/stats`);

    if (statsResponse.status === 200) {
      console.log('[OK] Stats:', statsResponse.data);
    } else {
      console.log('[WARN] Could not fetch stats');
    }

    console.log('\n========================================');
    console.log('[OK] WORKFLOW TEST COMPLETED');
    console.log('========================================');
    process.exit(0);
  } catch (error) {
    console.error('[ERROR] Workflow test failed:');
    console.error('Message:', error.message);
    console.error('Stack:', error.stack);
    process.exit(1);
  }
}

runWorkflow();
