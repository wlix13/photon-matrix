// Media federation (MSC3916, spec v1.11+): federation media endpoints exchange content as
// `multipart/mixed` - a JSON metadata part, then the media bytes.

import type { Env } from '../types';
import { federationGet } from './federation-keys';

const encoder = new TextEncoder();

// Find the first index of `needle` within `haystack` at or after `from`.
function indexOfBytes(haystack: Uint8Array, needle: Uint8Array, from = 0): number {
  outer: for (let i = from; i <= haystack.length - needle.length; i++) {
    for (let j = 0; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) continue outer;
    }
    return i;
  }
  return -1;
}

export interface RemoteMedia {
  contentType: string;
  data: Uint8Array;
  filename?: string;
}

// Build a spec-compliant multipart/mixed federation media response.
export function buildMultipartMediaResponse(
  data: Uint8Array,
  contentType: string,
  filename?: string
): Response {
  const boundary = `matrixmedia${crypto.randomUUID().replace(/-/g, '')}`;

  // A control character in the filename would inject extra MIME headers or end the part early.
  const safeFilename = filename
    ?.replace(/[\x00-\x1F\x7F-\x9F]/g, '')
    .replace(/[\\/]/g, '_')
    .replace(/"/g, "'")
    .trim();
  const disposition = safeFilename
    ? `Content-Disposition: inline; filename="${safeFilename}"\r\n`
    : '';
  const preamble = encoder.encode(
    `--${boundary}\r\n` +
    `Content-Type: application/json\r\n\r\n` +
    `{}\r\n` +
    `--${boundary}\r\n` +
    `Content-Type: ${contentType}\r\n` +
    disposition +
    `\r\n`
  );
  const closing = encoder.encode(`\r\n--${boundary}--\r\n`);

  const body = new Uint8Array(preamble.length + data.length + closing.length);
  body.set(preamble, 0);
  body.set(data, preamble.length);
  body.set(closing, preamble.length + data.length);

  return new Response(body, {
    headers: {
      'Content-Type': `multipart/mixed; boundary=${boundary}`,
      'Cache-Control': 'public, max-age=31536000, immutable',
    },
  });
}

// Parse a multipart/mixed federation media response into the media bytes.
async function parseMultipartMedia(response: Response): Promise<RemoteMedia | null> {
  const contentTypeHeader = response.headers.get('Content-Type') || '';
  const match = contentTypeHeader.match(/boundary=(?:"([^"]+)"|([^;]+))/i);
  if (!match) {
    // Some servers return the media directly, without multipart framing.
    const data = new Uint8Array(await response.arrayBuffer());
    return { contentType: contentTypeHeader || 'application/octet-stream', data };
  }
  const boundary = (match[1] || match[2]).trim();
  const full = new Uint8Array(await response.arrayBuffer());

  const delim = encoder.encode(`--${boundary}`);
  // First delimiter ends the preamble; the JSON part starts after it.
  let pos = indexOfBytes(full, delim, 0);
  if (pos === -1) return null;
  pos += delim.length;
  // Skip the JSON part: find the next delimiter (start of the media part).
  const mediaPartStart = indexOfBytes(full, delim, pos);
  if (mediaPartStart === -1) return null;
  const afterDelim = mediaPartStart + delim.length;

  // The media part: headers until CRLFCRLF, then the body until the closing delimiter.
  const headerEnd = indexOfBytes(full, encoder.encode('\r\n\r\n'), afterDelim);
  if (headerEnd === -1) return null;
  const headerBytes = full.slice(afterDelim, headerEnd);
  const headerText = new TextDecoder().decode(headerBytes);

  const ctMatch = headerText.match(/Content-Type:\s*([^\r\n]+)/i);
  const contentType = ctMatch ? ctMatch[1].trim() : 'application/octet-stream';
  const fnMatch = headerText.match(/filename="?([^"\r\n]+)"?/i);
  const filename = fnMatch ? fnMatch[1] : undefined;

  const bodyStart = headerEnd + 4;
  // Body ends at the closing delimiter (preceded by CRLF).
  const closing = indexOfBytes(full, encoder.encode(`\r\n--${boundary}`), bodyStart);
  const bodyEnd = closing === -1 ? full.length : closing;
  const data = full.slice(bodyStart, bodyEnd);

  return { contentType, data, filename };
}

// Fetch a remote server's media over the signed federation transport.
export async function fetchRemoteMedia(
  env: Env,
  serverName: string,
  mediaId: string,
  opts?: { thumbnail?: boolean; width?: number; height?: number; method?: string }
): Promise<RemoteMedia | null> {
  let path: string;
  if (opts?.thumbnail) {
    const params = new URLSearchParams({
      width: String(opts.width ?? 96),
      height: String(opts.height ?? 96),
      method: opts.method ?? 'scale',
    });
    path = `/_matrix/federation/v1/media/thumbnail/${encodeURIComponent(mediaId)}?${params}`;
  } else {
    path = `/_matrix/federation/v1/media/download/${encodeURIComponent(mediaId)}`;
  }

  try {
    const resp = await federationGet(serverName, path, env.SERVER_NAME, env.DB, env.CACHE);
    if (!resp.ok) {
      console.warn('[media-fed] remote media fetch failed', { serverName, mediaId, status: resp.status });
      return null;
    }
    return await parseMultipartMedia(resp);
  } catch (err) {
    console.error('[media-fed] remote media fetch error', { serverName, mediaId, err });
    return null;
  }
}
