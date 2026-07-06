// Durable retry queue for outbound federation, one instance per destination server (keyed
// by idFromName(server)). PDUs that services/federation-sender.ts failed to deliver
// directly are queued here and retried on an exponential-backoff alarm until they succeed.
// PDUs and EDUs both go out over the signed X-Matrix transport; a raw fetch() gets 401.

import { DurableObject } from 'cloudflare:workers';
import type { Env } from '../types';
import { getServerSigningKey, makeFederationRequest } from '../services/federation-keys';

interface FederationTarget {
  serverName: string;
  lastContact: number;
  retryCount: number;
  nextRetry: number | null;
}

// A finalized wire PDU awaiting (re)delivery to a single destination.
interface OutboundPdu {
  destination: string;
  roomId: string;
  wirePdu: any;
  created_at: number;
  retry_count: number;
}

interface OutboundEdu {
  edu_type: string;
  destination: string;
  content: any;
  created_at: number;
}

// Spec limits: <=50 PDUs and <=100 EDUs per federation transaction.
const MAX_PDUS_PER_TXN = 50;
const MAX_EDUS_PER_TXN = 100;

// Unique-per-isolate transaction id suffix.
let txnCounter = 0;
function nextTxnId(): string {
  txnCounter = (txnCounter + 1) % 1_000_000;
  return `${Date.now()}_${txnCounter}`;
}

export class FederationDurableObject extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === '/send') {
      return this.handleSend(request);
    }

    if (path === '/receive') {
      return this.handleReceive(request);
    }

    if (path === '/status') {
      return this.handleStatus(request);
    }

    if (path === '/keys') {
      return this.handleKeys(request);
    }

    if (path === '/send-edu') {
      return this.handleSendEdu(request);
    }

    return new Response('Not found', { status: 404 });
  }

  // Enqueues a PDU whose direct send already failed; the backoff alarm performs the signed delivery.
  private async handleSend(request: Request): Promise<Response> {
    const data = (await request.json()) as {
      destination: string;
      roomId: string;
      wirePdu: any;
    };

    if (!data.destination || !data.wirePdu) {
      return new Response(
        JSON.stringify({ errcode: 'M_MISSING_PARAM', error: 'destination and wirePdu required' }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }

    const entry: OutboundPdu = {
      destination: data.destination,
      roomId: data.roomId,
      wirePdu: data.wirePdu,
      created_at: Date.now(),
      retry_count: 0,
    };

    const key = `queue:${data.destination}:${entry.created_at}:${Math.random().toString(36).slice(2)}`;
    await this.ctx.storage.put(key, entry);

    await this.scheduleRetry(data.destination);

    return new Response('Queued');
  }

  // Handle incoming federation request
  private async handleReceive(request: Request): Promise<Response> {
    const origin = request.headers.get('X-Matrix-Origin');
    if (!origin) {
      return new Response(
        JSON.stringify({
          errcode: 'M_MISSING_PARAM',
          error: 'Missing origin header',
        }),
        { status: 400 }
      );
    }

    const data = (await request.json()) as {
      pdus: any[];
      edus?: any[];
    };

    const processedPdus: string[] = [];
    for (const pdu of data.pdus || []) {
      await this.ctx.storage.put(`received:${pdu.event_id}`, {
        pdu,
        origin,
        received_at: Date.now(),
      });
      processedPdus.push(pdu.event_id);
    }

    const target: FederationTarget = {
      serverName: origin,
      lastContact: Date.now(),
      retryCount: 0,
      nextRetry: null,
    };
    await this.ctx.storage.put(`server:${origin}`, target);

    return new Response(
      JSON.stringify({
        pdus: processedPdus.reduce((acc, id) => ({ ...acc, [id]: {} }), {}),
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Get federation status for a server
  private async handleStatus(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const serverName = url.searchParams.get('server');

    if (serverName) {
      const target = (await this.ctx.storage.get(`server:${serverName}`)) as
        | FederationTarget
        | undefined;
      return new Response(JSON.stringify(target || { serverName, status: 'unknown' }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const servers: FederationTarget[] = [];
    const allKeys = await this.ctx.storage.list({ prefix: 'server:' });
    for (const [, value] of allKeys) {
      servers.push(value as FederationTarget);
    }

    return new Response(JSON.stringify({ servers }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Handle server key requests (key server endpoint - not X-Matrix authenticated).
  private async handleKeys(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const serverName = url.searchParams.get('server');

    if (!serverName) {
      return new Response(
        JSON.stringify({
          errcode: 'M_MISSING_PARAM',
          error: 'Missing server parameter',
        }),
        { status: 400 }
      );
    }

    const cachedKeys = await this.ctx.storage.get(`keys:${serverName}`);
    if (cachedKeys) {
      const keys = cachedKeys as { data: any; expires: number };
      if (keys.expires > Date.now()) {
        return new Response(JSON.stringify(keys.data), {
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    try {
      const response = await fetch(`https://${serverName}/_matrix/key/v2/server`, {
        headers: {
          Accept: 'application/json',
        },
      });

      if (response.ok) {
        const data = await response.json();

        await this.ctx.storage.put(`keys:${serverName}`, {
          data,
          expires: Date.now() + 24 * 60 * 60 * 1000,
        });

        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json' },
        });
      }
    } catch (e) {
      console.error(`Failed to fetch keys from ${serverName}:`, e);
    }

    return new Response(
      JSON.stringify({
        errcode: 'M_NOT_FOUND',
        error: 'Server keys not found',
      }),
      { status: 404 }
    );
  }

  // Queues an EDU for a destination, then flushes that destination's queue.
  private async handleSendEdu(request: Request): Promise<Response> {
    const data = (await request.json()) as {
      destination: string;
      edu_type: string;
      content: any;
    };

    const edu: OutboundEdu = {
      edu_type: data.edu_type,
      destination: data.destination,
      content: data.content,
      created_at: Date.now(),
    };

    const key = `edu:${data.destination}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    await this.ctx.storage.put(key, edu);

    // EDUs are latency-sensitive (typing/receipts/presence); attempt immediately.
    await this.processFederationQueue(data.destination);

    return new Response('Queued');
  }

  // Flushes a destination's pending PDUs and EDUs as one signed transaction.
  private async processFederationQueue(destination: string): Promise<void> {
    const pduEntries: Array<{ key: string; value: OutboundPdu }> = [];
    const pduKeys = await this.ctx.storage.list({ prefix: `queue:${destination}:` });
    for (const [key, value] of pduKeys) {
      pduEntries.push({ key, value: value as OutboundPdu });
    }

    const eduEntries: Array<{ key: string; value: OutboundEdu }> = [];
    const eduKeys = await this.ctx.storage.list({ prefix: `edu:${destination}:` });
    for (const [key, value] of eduKeys) {
      eduEntries.push({ key, value: value as OutboundEdu });
    }

    if (pduEntries.length === 0 && eduEntries.length === 0) return;

    // Oldest first, bounded to the spec transaction limits.
    pduEntries.sort((a, b) => a.value.created_at - b.value.created_at);
    eduEntries.sort((a, b) => a.value.created_at - b.value.created_at);
    const pduBatch = pduEntries.slice(0, MAX_PDUS_PER_TXN);
    const eduBatch = eduEntries.slice(0, MAX_EDUS_PER_TXN);

    const signingKey = await getServerSigningKey(this.env.DB);
    if (!signingKey) {
      console.error('[FederationDO] no signing key; deferring delivery', { destination });
      await this.scheduleRetry(destination);
      return;
    }

    const txnId = nextTxnId();
    const path = `/_matrix/federation/v1/send/${encodeURIComponent(txnId)}`;
    const body = {
      origin: this.env.SERVER_NAME,
      origin_server_ts: Date.now(),
      pdus: pduBatch.map((e) => e.value.wirePdu),
      edus: eduBatch.map((e) => ({ edu_type: e.value.edu_type, content: e.value.content })),
    };

    try {
      const response = await makeFederationRequest(
        'PUT',
        destination,
        path,
        this.env.SERVER_NAME,
        signingKey,
        this.env.CACHE,
        body
      );

      if (response.ok) {
        await response.text().catch(() => {});
        for (const { key } of pduBatch) await this.ctx.storage.delete(key);
        for (const { key } of eduBatch) await this.ctx.storage.delete(key);

        await this.ctx.storage.put(`server:${destination}`, {
          serverName: destination,
          lastContact: Date.now(),
          retryCount: 0,
          nextRetry: null,
        } satisfies FederationTarget);

        // More queued than fit in one transaction: keep draining promptly.
        if (pduEntries.length > pduBatch.length || eduEntries.length > eduBatch.length) {
          await this.ctx.storage.setAlarm(Date.now() + 1000);
        }
      } else {
        const text = await response.text().catch(() => '');
        console.warn('[FederationDO] send failed', {
          destination,
          status: response.status,
          body: text.slice(0, 200),
        });
        await this.scheduleRetry(destination);
      }
    } catch (e) {
      console.error(`[FederationDO] send to ${destination} threw:`, e);
      await this.scheduleRetry(destination);
    }
  }

  private async scheduleRetry(destination: string): Promise<void> {
    const target = (await this.ctx.storage.get(`server:${destination}`)) as
      | FederationTarget
      | undefined;
    const retryCount = (target?.retryCount || 0) + 1;

    // Exponential backoff: 1min, 2min, 4min, 8min, 16min, capped at 1 hour.
    const delay = Math.min(60000 * Math.pow(2, retryCount - 1), 3600000);
    const nextRetry = Date.now() + delay;

    await this.ctx.storage.put(`server:${destination}`, {
      serverName: destination,
      lastContact: target?.lastContact || 0,
      retryCount,
      nextRetry,
    } satisfies FederationTarget);

    await this.ctx.storage.setAlarm(nextRetry);
  }

  async alarm(): Promise<void> {
    const allKeys = await this.ctx.storage.list({ prefix: 'server:' });

    for (const [, value] of allKeys) {
      const target = value as FederationTarget;
      if (target.nextRetry && target.nextRetry <= Date.now()) {
        await this.processFederationQueue(target.serverName);
      }
    }
  }
}
