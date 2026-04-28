/**
 * RosterIQ Push Notifications Handler
 *
 * Manages Web Push API notifications:
 * - Request user permission
 * - Subscribe to push service
 * - Handle incoming push events
 * - Open relevant pages on notification click
 */

// ============================================================================
// Request Notification Permission
// ============================================================================

async function requestNotificationPermission() {
  if (!('Notification' in window)) {
    console.warn('[Push] Notifications not supported in this browser');
    return false;
  }

  if (Notification.permission === 'granted') {
    console.log('[Push] Notification permission already granted');
    return true;
  }

  if (Notification.permission === 'denied') {
    console.warn('[Push] Notification permission denied');
    return false;
  }

  try {
    const permission = await Notification.requestPermission();
    return permission === 'granted';
  } catch (err) {
    console.error('[Push] Error requesting permission:', err);
    return false;
  }
}

// ============================================================================
// Subscribe to Push Notifications
// ============================================================================

async function subscribeToPush(vapidPublicKey) {
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    console.warn('[Push] Push notifications not supported');
    return null;
  }

  try {
    // Get the service worker registration
    const registration = await navigator.serviceWorker.ready;

    // Check if already subscribed
    const existingSubscription = await registration.pushManager.getSubscription();
    if (existingSubscription) {
      console.log('[Push] Already subscribed');
      return existingSubscription;
    }

    // Subscribe to push
    const subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(vapidPublicKey),
    });

    console.log('[Push] Subscribed successfully');
    return subscription;
  } catch (err) {
    console.error('[Push] Error subscribing:', err);
    return null;
  }
}

// ============================================================================
// Send Subscription to Server
// ============================================================================

async function sendSubscriptionToServer(subscription) {
  if (!subscription) {
    return false;
  }

  try {
    const response = await fetch('/api/push/subscribe', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(subscription),
    });

    if (response.ok) {
      console.log('[Push] Subscription registered with server');
      localStorage.setItem('push_subscribed', 'true');
      localStorage.setItem('push_subscription', JSON.stringify(subscription));
      return true;
    } else {
      console.error('[Push] Server rejected subscription');
      return false;
    }
  } catch (err) {
    console.error('[Push] Error registering subscription:', err);
    return false;
  }
}

// ============================================================================
// Initialize Push Notifications
// ============================================================================

async function initializePushNotifications() {
  if (!('serviceWorker' in navigator)) {
    console.warn('[Push] Service Workers not supported');
    return;
  }

  try {
    // Request permission
    const permissionGranted = await requestNotificationPermission();
    if (!permissionGranted) {
      console.log('[Push] User denied permission');
      return;
    }

    // Get VAPID public key from server
    const vapidResponse = await fetch('/api/push/vapid-key');
    if (!vapidResponse.ok) {
      console.warn('[Push] Could not get VAPID key');
      return;
    }

    const { public_key } = await vapidResponse.json();

    // Subscribe to push
    const subscription = await subscribeToPush(public_key);
    if (!subscription) {
      console.warn('[Push] Could not subscribe to push');
      return;
    }

    // Register with server
    await sendSubscriptionToServer(subscription);
  } catch (err) {
    console.error('[Push] Error initializing push notifications:', err);
  }
}

// ============================================================================
// Unsubscribe from Push
// ============================================================================

async function unsubscribeFromPush() {
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    return false;
  }

  try {
    const registration = await navigator.serviceWorker.ready;
    const subscription = await registration.pushManager.getSubscription();

    if (subscription) {
      await subscription.unsubscribe();

      // Notify server
      await fetch('/api/push/unsubscribe', {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ endpoint: subscription.endpoint }),
      });

      localStorage.removeItem('push_subscribed');
      localStorage.removeItem('push_subscription');

      console.log('[Push] Unsubscribed from push notifications');
      return true;
    }
  } catch (err) {
    console.error('[Push] Error unsubscribing:', err);
    return false;
  }

  return false;
}

// ============================================================================
// Handle Incoming Push Events (in Service Worker)
// ============================================================================

// This runs in the service worker context
if ('self' in globalThis && 'addEventListener' in self) {
  self.addEventListener('push', event => {
    console.log('[Push Event] Received:', event.data);

    let data = {};
    if (event.data) {
      try {
        data = event.data.json();
      } catch (e) {
        console.warn('[Push Event] Could not parse JSON:', e);
        data = { title: 'RosterIQ', body: event.data.text() };
      }
    }

    const { type, title, body, venue_id, date, shift_id, url } = data;

    // Build notification title and body
    let notificationTitle = title || 'RosterIQ';
    let notificationOptions = {
      body: body || '',
      icon: '/static/manifest.json',
      badge: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 192 192"><rect fill="%231e3a5f" width="192" height="192"/><text x="50%" y="50%" font-size="80" fill="white" text-anchor="middle" dominant-baseline="middle" font-weight="bold">RQ</text></svg>',
      tag: type || 'notification',
      requireInteraction: false,
    };

    // Customize by notification type
    switch (type) {
      case 'roster.published':
        notificationTitle = 'New Roster Published';
        notificationOptions.body = `${body} ${venue_id ? `for ${venue_id}` : ''}`;
        notificationOptions.tag = 'roster-published';
        break;

      case 'shift.reminder':
        notificationTitle = 'Shift Reminder';
        notificationOptions.body = `${body}`;
        notificationOptions.requireInteraction = true;
        notificationOptions.tag = `shift-reminder-${shift_id}`;
        break;

      case 'shift.swap':
        notificationTitle = 'Shift Swap Request';
        notificationOptions.body = `${body}`;
        notificationOptions.requireInteraction = true;
        notificationOptions.tag = `swap-${shift_id}`;
        break;

      case 'alert':
        notificationTitle = 'Staffing Alert';
        notificationOptions.body = `${body}`;
        notificationOptions.tag = 'staffing-alert';
        break;

      default:
        break;
    }

    // Store notification data for click handler
    const notificationData = {
      type,
      venue_id,
      date,
      shift_id,
      url: url || '/dashboard',
    };

    notificationOptions.data = notificationData;

    event.waitUntil(
      self.registration.showNotification(notificationTitle, notificationOptions)
    );
  });

  // Handle notification clicks
  self.addEventListener('notificationclick', event => {
    console.log('[Push Click] Notification clicked:', event.notification.tag);

    event.notification.close();

    const { data } = event.notification;
    let targetUrl = data?.url || '/dashboard';

    // Navigate to relevant page
    if (data?.type === 'roster.published' && data?.venue_id) {
      targetUrl = `/dashboard?venue=${data.venue_id}`;
    } else if (data?.type === 'shift.reminder' && data?.shift_id) {
      targetUrl = `/staff?shift=${data.shift_id}`;
    } else if (data?.type === 'shift.swap' && data?.shift_id) {
      targetUrl = `/staff?swap=${data.shift_id}`;
    } else if (data?.type === 'alert') {
      targetUrl = '/dashboard';
    }

    event.waitUntil(
      clients.matchAll({ type: 'window' }).then(clientList => {
        // Check if app is already open
        for (const client of clientList) {
          if (client.url === targetUrl && 'focus' in client) {
            return client.focus();
          }
        }

        // Open new window
        if (clients.openWindow) {
          return clients.openWindow(targetUrl);
        }
      })
    );
  });

  // Handle notification close
  self.addEventListener('notificationclose', event => {
    console.log('[Push Close] Notification closed:', event.notification.tag);
  });
}

// ============================================================================
// Utility: Convert VAPID Key
// ============================================================================

function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - base64String.length % 4) % 4);
  const base64 = (base64String + padding)
    .replace(/\-/g, '+')
    .replace(/_/g, '/');

  const rawData = window.atob(base64);
  const outputArray = new Uint8Array(rawData.length);

  for (let i = 0; i < rawData.length; ++i) {
    outputArray[i] = rawData.charCodeAt(i);
  }

  return outputArray;
}

// ============================================================================
// Export Functions for Use
// ============================================================================

if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    requestNotificationPermission,
    subscribeToPush,
    sendSubscriptionToServer,
    initializePushNotifications,
    unsubscribeFromPush,
  };
}

console.log('[Push] Handler loaded and ready');
