// Long-poll wait via hibernatable WebSocket to the user's SyncDurableObject.
// The worker holds the client end and enforces the timeout, leaving the DO with
// no in-flight request and no timer, so it hibernates instead of billing time.

import type { Env } from '../types';

// Returns true if a wake arrived, false if the wait timed out with nothing new.
export async function waitForWake(
  env: Env,
  userId: string,
  timeoutMs: number,
  sinceTs: number,
): Promise<boolean> {
  const stub = env.SYNC.get(env.SYNC.idFromName(userId));

  let response: Response;
  try {
    response = await stub.fetch(new Request(`http://internal/wait-ws?since_ts=${sinceTs}`, {
      headers: { Upgrade: 'websocket' },
    }));
  } catch (error) {
    console.error('[sync-wait] wait-ws upgrade failed, using legacy wait:', error);
    return waitForWakeLegacy(stub, timeoutMs);
  }

  const ws = response.webSocket;
  if (!ws) {
    console.error('[sync-wait] wait-ws returned no WebSocket (status', response.status, '), using legacy wait');
    return waitForWakeLegacy(stub, timeoutMs);
  }

  ws.accept();
  return new Promise<boolean>((resolve) => {
    let settled = false;
    const settle = (woke: boolean) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      try {
        ws.close(1000, 'done');
      } catch {
        // Already closed by the DO after sending the wake
      }
      resolve(woke);
    };

    const timer = setTimeout(() => settle(false), timeoutMs);
    // A bare message from the DO is the wake signal; a close without one means
    // no wake (or a DO restart) and is treated as a timeout, so the client re-polls.
    ws.addEventListener('message', () => settle(true));
    ws.addEventListener('close', () => settle(false));
    ws.addEventListener('error', () => settle(false));
  });
}

// Fallback for when the WebSocket upgrade fails: parks an HTTP request on the DO.
async function waitForWakeLegacy(stub: DurableObjectStub, timeoutMs: number): Promise<boolean> {
  try {
    const response = await stub.fetch(new Request('http://internal/wait-for-events', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timeout: timeoutMs }),
    }));
    const result = await response.json() as { hasEvents: boolean };
    return result.hasEvents;
  } catch (error) {
    console.error('[sync-wait] legacy wait failed:', error);
    return false;
  }
}
