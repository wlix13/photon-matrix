// Matrix media endpoints (using R2 for storage)

import { Hono } from 'hono';
import type { AppEnv } from '../types';
import { Errors } from '../utils/errors';
import { requireAuth } from '../middleware/auth';
import { generateOpaqueId } from '../utils/ids';
import { validateUrlForPreview } from '../utils/url-validator';

const app = new Hono<AppEnv>();

const MAX_UPLOAD_SIZE = 50 * 1024 * 1024;

// Supported MIME types (not currently enforced on upload).
export const SUPPORTED_TYPES = [
  'image/jpeg',
  'image/png',
  'image/gif',
  'image/webp',
  'image/svg+xml',
  'video/mp4',
  'video/webm',
  'audio/mp3',
  'audio/ogg',
  'audio/wav',
  'audio/webm',
  'application/pdf',
  'application/json',
  'text/plain',
  'application/octet-stream',
];

// POST /_matrix/media/v3/upload - Upload media
app.post('/_matrix/media/v3/upload', requireAuth(), async (c) => {
  const userId = c.get('userId');

  const contentType = c.req.header('Content-Type') || 'application/octet-stream';
  const filename = c.req.query('filename');

  const contentLength = parseInt(c.req.header('Content-Length') || '0');
  if (contentLength > MAX_UPLOAD_SIZE) {
    return Errors.tooLarge('File exceeds maximum upload size').toResponse();
  }

  const mediaId = await generateOpaqueId(24);
  const mxcUri = `mxc://${c.env.SERVER_NAME}/${mediaId}`;

  const body = await c.req.arrayBuffer();

  await c.env.MEDIA.put(mediaId, body, {
    httpMetadata: {
      contentType,
    },
    customMetadata: {
      userId,
      filename: filename || '',
      uploadedAt: Date.now().toString(),
    },
  });

  await c.env.DB.prepare(
    `INSERT INTO media (media_id, user_id, content_type, content_length, filename, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).bind(mediaId, userId, contentType, body.byteLength, filename || null, Date.now()).run();

  return c.json({
    content_uri: mxcUri,
  });
});

// GET /_matrix/media/v3/download/:serverName/:mediaId - Download media
app.get('/_matrix/media/v3/download/:serverName/:mediaId', async (c) => {
  const serverName = c.req.param('serverName');
  const mediaId = c.req.param('mediaId');

  // Only serve local media for now
  if (serverName !== c.env.SERVER_NAME) {
    return Errors.notFound('Remote media not supported').toResponse();
  }

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  const metadata = await c.env.DB.prepare(
    `SELECT content_type, filename FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string; filename: string | null }>();

  const headers = new Headers();
  headers.set('Content-Type', metadata?.content_type || 'application/octet-stream');
  if (metadata?.filename) {
    headers.set('Content-Disposition', `inline; filename="${metadata.filename}"`);
  }
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  return new Response(object.body, { headers });
});

// GET /_matrix/media/v3/download/:serverName/:mediaId/:filename - Download with filename
app.get('/_matrix/media/v3/download/:serverName/:mediaId/:filename', async (c) => {
  const serverName = c.req.param('serverName');
  const mediaId = c.req.param('mediaId');
  const requestedFilename = c.req.param('filename');

  // Only serve local media for now
  if (serverName !== c.env.SERVER_NAME) {
    return Errors.notFound('Remote media not supported').toResponse();
  }

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  const metadata = await c.env.DB.prepare(
    `SELECT content_type FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string }>();

  const headers = new Headers();
  headers.set('Content-Type', metadata?.content_type || 'application/octet-stream');
  headers.set('Content-Disposition', `inline; filename="${requestedFilename}"`);
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  return new Response(object.body, { headers });
});

// GET /_matrix/media/v3/thumbnail/:serverName/:mediaId - Get thumbnail
app.get('/_matrix/media/v3/thumbnail/:serverName/:mediaId', async (c) => {
  const serverName = c.req.param('serverName');
  const mediaId = c.req.param('mediaId');
  const width = Math.min(parseInt(c.req.query('width') || '96'), 1920);
  const height = Math.min(parseInt(c.req.query('height') || '96'), 1920);
  const method = c.req.query('method') || 'scale';

  // Only serve local media for now
  if (serverName !== c.env.SERVER_NAME) {
    return Errors.notFound('Remote media not supported').toResponse();
  }

  const metadata = await c.env.DB.prepare(
    `SELECT content_type FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string }>();

  if (!metadata) {
    return Errors.notFound('Media not found').toResponse();
  }

  const isImage = metadata.content_type.startsWith('image/');

  const thumbnailKey = `thumb_${mediaId}_${width}x${height}_${method}`;
  const existingThumb = await c.env.MEDIA.get(thumbnailKey);

  if (existingThumb) {
    const headers = new Headers();
    headers.set('Content-Type', 'image/jpeg');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    return new Response(existingThumb.body, { headers });
  }

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  if (!isImage) {
    const headers = new Headers();
    headers.set('Content-Type', metadata.content_type);
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    return new Response(object.body, { headers });
  }

  try {
    const downloadUrl = `https://${c.env.WORKER_DOMAIN || c.env.SERVER_NAME}/_matrix/media/v3/download/${serverName}/${mediaId}`;
    const resized = await fetch(downloadUrl, {
      cf: {
        image: {
          width,
          height,
          fit: method === 'crop' ? 'cover' : 'contain',
          format: 'jpeg',
          quality: 85,
        },
      },
    });

    if (resized.ok) {
      const thumbBody = await resized.arrayBuffer();
      await c.env.MEDIA.put(thumbnailKey, thumbBody, {
        httpMetadata: { contentType: 'image/jpeg' },
      });

      const headers = new Headers();
      headers.set('Content-Type', 'image/jpeg');
      headers.set('Cache-Control', 'public, max-age=31536000, immutable');
      headers.set('X-Thumbnail-Generated', 'true');
      return new Response(thumbBody, { headers });
    }
  } catch (resizeErr) {
    // Image Resizing may be unavailable (Free plan); fall back to the original.
    console.warn('[media] Image resize failed (may require Pro plan):', resizeErr);
  }

  const headers = new Headers();
  headers.set('Content-Type', metadata.content_type);
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Thumbnail-Generated', 'false');

  return new Response(object.body, { headers });
});

// GET /_matrix/media/v3/preview_url - Get URL preview
app.get('/_matrix/media/v3/preview_url', requireAuth(), async (c) => {
  const url = c.req.query('url');
  // ts is an optional cache-busting timestamp; unused here.
  void c.req.query('ts');

  if (!url) {
    return Errors.missingParam('url').toResponse();
  }

  // Validate URL for SSRF protection
  const validation = validateUrlForPreview(url);
  if (!validation.valid) {
    return c.json(
      {
        errcode: 'M_UNKNOWN',
        error: validation.error || 'Invalid URL',
      },
      400
    );
  }

  try {
    const parsedUrl = new URL(validation.sanitizedUrl!);

    const cacheKey = `preview:${url}`;
    const cached = await c.env.CACHE.get(cacheKey);
    if (cached) {
      return c.json(JSON.parse(cached));
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    const response = await fetch(url, {
      headers: {
        'User-Agent': 'Tuwunel Matrix Server URL Preview Bot',
        'Accept': 'text/html,application/xhtml+xml,*/*',
      },
      signal: controller.signal,
      redirect: 'follow',
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      return c.json({});
    }

    const contentType = response.headers.get('Content-Type') || '';

    if (contentType.startsWith('image/')) {
      const result = {
        'og:image': url,
        'og:image:type': contentType,
      };

      await c.env.CACHE.put(cacheKey, JSON.stringify(result), { expirationTtl: 3600 });
      return c.json(result);
    }

    if (!contentType.includes('text/html')) {
      return c.json({});
    }

    let html: string;

    // Browser Rendering, when bound, gets the DOM of JS-rendered pages.
    if (c.env.BROWSER) {
      try {
        const browserResponse = await c.env.BROWSER.fetch(new Request(url, {
          headers: { 'Accept': 'text/html' },
        }));
        if (browserResponse.ok) {
          html = await browserResponse.text();
        } else {
          html = await response.text();
        }
      } catch (browserErr) {
        console.warn('[media] Browser Rendering failed, falling back to basic fetch:', browserErr);
        html = await response.text();
      }
    } else {
      html = await response.text();
    }

    // Extract Open Graph and meta tags
    const preview: Record<string, any> = {};

    const ogTitle = html.match(/<meta[^>]*property=["']og:title["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                   html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:title["'][^>]*>/i);
    if (ogTitle) {
      preview['og:title'] = decodeHtmlEntities(ogTitle[1]);
    } else {
      const title = html.match(/<title[^>]*>([^<]*)<\/title>/i);
      if (title) {
        preview['og:title'] = decodeHtmlEntities(title[1]);
      }
    }

    const ogDesc = html.match(/<meta[^>]*property=["']og:description["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                  html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:description["'][^>]*>/i);
    if (ogDesc) {
      preview['og:description'] = decodeHtmlEntities(ogDesc[1]);
    } else {
      const metaDesc = html.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                      html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*name=["']description["'][^>]*>/i);
      if (metaDesc) {
        preview['og:description'] = decodeHtmlEntities(metaDesc[1]);
      }
    }

    const ogImage = html.match(/<meta[^>]*property=["']og:image["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                   html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:image["'][^>]*>/i);
    if (ogImage) {
      let imageUrl = ogImage[1];
      // Resolve relative image URLs against the page.
      if (imageUrl.startsWith('/')) {
        imageUrl = `${parsedUrl.protocol}//${parsedUrl.host}${imageUrl}`;
      } else if (!imageUrl.startsWith('http')) {
        imageUrl = `${parsedUrl.protocol}//${parsedUrl.host}/${imageUrl}`;
      }
      preview['og:image'] = imageUrl;
    }

    const ogSiteName = html.match(/<meta[^>]*property=["']og:site_name["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                      html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:site_name["'][^>]*>/i);
    if (ogSiteName) {
      preview['og:site_name'] = decodeHtmlEntities(ogSiteName[1]);
    }

    const ogType = html.match(/<meta[^>]*property=["']og:type["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                  html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:type["'][^>]*>/i);
    if (ogType) {
      preview['og:type'] = ogType[1];
    }

    if (Object.keys(preview).length > 0) {
      await c.env.CACHE.put(cacheKey, JSON.stringify(preview), { expirationTtl: 3600 });
    }

    return c.json(preview);
  } catch (error) {
    console.error('URL preview error:', error);
    return c.json({});
  }
});

// Decode HTML entities exactly once; double-decoding would turn &amp;lt; back into markup.
function decodeHtmlEntities(text: string): string {
  const decoded = text
    .replace(/&amp;/g, '\x00AMP\x00')  // Temporarily replace to prevent double-decode
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, '/')
    .replace(/&nbsp;/g, ' ')
    .replace(/\x00AMP\x00/g, '&');  // Restore ampersands last
  return decoded;
}

// GET /_matrix/media/v3/config - Get media config
app.get('/_matrix/media/v3/config', async (c) => {
  return c.json({
    'm.upload.size': MAX_UPLOAD_SIZE,
  });
});

// ============================================
// Authenticated Media Endpoints (MSC3916)
// ============================================

// POST /_matrix/client/v1/media/upload - Authenticated upload
app.post('/_matrix/client/v1/media/upload', requireAuth(), async (c) => {
  const userId = c.get('userId');

  const contentType = c.req.header('Content-Type') || 'application/octet-stream';
  const filename = c.req.query('filename');

  const contentLength = parseInt(c.req.header('Content-Length') || '0');
  if (contentLength > MAX_UPLOAD_SIZE) {
    return Errors.tooLarge('File exceeds maximum upload size').toResponse();
  }

  const mediaId = await generateOpaqueId(24);
  const mxcUri = `mxc://${c.env.SERVER_NAME}/${mediaId}`;

  const body = await c.req.arrayBuffer();

  await c.env.MEDIA.put(mediaId, body, {
    httpMetadata: { contentType },
    customMetadata: {
      userId,
      filename: filename || '',
      uploadedAt: Date.now().toString(),
    },
  });

  await c.env.DB.prepare(
    `INSERT INTO media (media_id, user_id, content_type, content_length, filename, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).bind(mediaId, userId, contentType, body.byteLength, filename || null, Date.now()).run();

  return c.json({ content_uri: mxcUri });
});

// POST /_matrix/client/v1/media/create - Create media placeholder (for async uploads)
app.post('/_matrix/client/v1/media/create', requireAuth(), async (c) => {
  const userId = c.get('userId');

  const mediaId = await generateOpaqueId(24);
  const mxcUri = `mxc://${c.env.SERVER_NAME}/${mediaId}`;

  await c.env.DB.prepare(
    `INSERT INTO media (media_id, user_id, content_type, content_length, created_at)
     VALUES (?, ?, 'application/octet-stream', 0, ?)`
  ).bind(mediaId, userId, Date.now()).run();

  return c.json({
    content_uri: mxcUri,
    unused_expires_at: Date.now() + 24 * 60 * 60 * 1000, // 24 hours
  });
});

// PUT /_matrix/client/v1/media/upload/:serverName/:mediaId - Upload to placeholder
app.put('/_matrix/client/v1/media/upload/:serverName/:mediaId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const serverName = c.req.param('serverName');
  const mediaId = c.req.param('mediaId');

  if (serverName !== c.env.SERVER_NAME) {
    return Errors.forbidden('Cannot upload to remote server').toResponse();
  }

  // A placeholder is a row with content_length 0 that the same user created.
  const existing = await c.env.DB.prepare(
    `SELECT user_id, content_length FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ user_id: string; content_length: number }>();

  if (!existing) {
    return Errors.notFound('Media not found').toResponse();
  }

  if (existing.user_id !== userId) {
    return Errors.forbidden('Not authorized to upload to this media').toResponse();
  }

  if (existing.content_length > 0) {
    return c.json({
      errcode: 'M_CANNOT_OVERWRITE_MEDIA',
      error: 'Media already uploaded',
    }, 409);
  }

  const contentType = c.req.header('Content-Type') || 'application/octet-stream';
  const filename = c.req.query('filename');

  const body = await c.req.arrayBuffer();

  await c.env.MEDIA.put(mediaId, body, {
    httpMetadata: { contentType },
    customMetadata: {
      userId,
      filename: filename || '',
      uploadedAt: Date.now().toString(),
    },
  });

  await c.env.DB.prepare(
    `UPDATE media SET content_type = ?, content_length = ?, filename = ? WHERE media_id = ?`
  ).bind(contentType, body.byteLength, filename || null, mediaId).run();

  return c.json({});
});

// GET /_matrix/client/v1/media/download/:serverName/:mediaId - Authenticated download
app.get('/_matrix/client/v1/media/download/:serverName/:mediaId', requireAuth(), async (c) => {
  const serverName = c.req.param('serverName');
  const mediaId = c.req.param('mediaId');

  if (serverName !== c.env.SERVER_NAME) {
    return Errors.notFound('Remote media not supported').toResponse();
  }

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  const metadata = await c.env.DB.prepare(
    `SELECT content_type, filename FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string; filename: string | null }>();

  const headers = new Headers();
  headers.set('Content-Type', metadata?.content_type || 'application/octet-stream');
  if (metadata?.filename) {
    headers.set('Content-Disposition', `inline; filename="${metadata.filename}"`);
  }
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  return new Response(object.body, { headers });
});

// GET /_matrix/client/v1/media/download/:serverName/:mediaId/:filename - With filename
app.get('/_matrix/client/v1/media/download/:serverName/:mediaId/:filename', requireAuth(), async (c) => {
  const serverName = c.req.param('serverName');
  const mediaId = c.req.param('mediaId');
  const requestedFilename = c.req.param('filename');

  if (serverName !== c.env.SERVER_NAME) {
    return Errors.notFound('Remote media not supported').toResponse();
  }

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  const metadata = await c.env.DB.prepare(
    `SELECT content_type FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string }>();

  const headers = new Headers();
  headers.set('Content-Type', metadata?.content_type || 'application/octet-stream');
  headers.set('Content-Disposition', `inline; filename="${requestedFilename}"`);
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  return new Response(object.body, { headers });
});

// GET /_matrix/client/v1/media/thumbnail/:serverName/:mediaId - Authenticated thumbnail
app.get('/_matrix/client/v1/media/thumbnail/:serverName/:mediaId', requireAuth(), async (c) => {
  const serverName = c.req.param('serverName');
  const mediaId = c.req.param('mediaId');
  const width = Math.min(parseInt(c.req.query('width') || '96'), 1920);
  const height = Math.min(parseInt(c.req.query('height') || '96'), 1920);
  const method = c.req.query('method') || 'scale';

  if (serverName !== c.env.SERVER_NAME) {
    return Errors.notFound('Remote media not supported').toResponse();
  }

  // Get media metadata
  const metadata = await c.env.DB.prepare(
    `SELECT content_type FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string }>();

  if (!metadata) {
    return Errors.notFound('Media not found').toResponse();
  }

  const isImage = metadata.content_type.startsWith('image/');

  // Check for pre-generated thumbnail
  const thumbnailKey = `thumb_${mediaId}_${width}x${height}_${method}`;
  const existingThumb = await c.env.MEDIA.get(thumbnailKey);

  if (existingThumb) {
    const headers = new Headers();
    headers.set('Content-Type', 'image/jpeg');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    return new Response(existingThumb.body, { headers });
  }

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  // Try Image Resizing for image content
  if (isImage) {
    try {
      const downloadUrl = `https://${c.env.WORKER_DOMAIN || c.env.SERVER_NAME}/_matrix/media/v3/download/${serverName}/${mediaId}`;
      const resized = await fetch(downloadUrl, {
        cf: {
          image: {
            width,
            height,
            fit: method === 'crop' ? 'cover' : 'contain',
            format: 'jpeg',
            quality: 85,
          },
        },
      });

      if (resized.ok) {
        const thumbBody = await resized.arrayBuffer();
        await c.env.MEDIA.put(thumbnailKey, thumbBody, {
          httpMetadata: { contentType: 'image/jpeg' },
        });

        const resHeaders = new Headers();
        resHeaders.set('Content-Type', 'image/jpeg');
        resHeaders.set('Cache-Control', 'public, max-age=31536000, immutable');
        resHeaders.set('X-Thumbnail-Generated', 'true');
        return new Response(thumbBody, { headers: resHeaders });
      }
    } catch (resizeErr) {
      console.warn('[media] Image resize failed:', resizeErr);
    }
  }

  const headers = new Headers();
  headers.set('Content-Type', metadata.content_type);
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  if (isImage) {
    headers.set('X-Thumbnail-Generated', 'false');
  }

  return new Response(object.body, { headers });
});

// GET /_matrix/client/v1/media/preview_url - Authenticated URL preview
app.get('/_matrix/client/v1/media/preview_url', requireAuth(), async (c) => {
  // Same logic as the v3 preview_url handler, minus Browser Rendering.
  const url = c.req.query('url');

  if (!url) {
    return Errors.missingParam('url').toResponse();
  }

  // Validate URL for SSRF protection
  const validation = validateUrlForPreview(url);
  if (!validation.valid) {
    return c.json(
      {
        errcode: 'M_UNKNOWN',
        error: validation.error || 'Invalid URL',
      },
      400
    );
  }

  try {
    const parsedUrl = new URL(validation.sanitizedUrl!);

    const cacheKey = `preview:${url}`;
    const cached = await c.env.CACHE.get(cacheKey);
    if (cached) {
      return c.json(JSON.parse(cached));
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    const response = await fetch(url, {
      headers: {
        'User-Agent': 'Tuwunel Matrix Server URL Preview Bot',
        'Accept': 'text/html,application/xhtml+xml,*/*',
      },
      signal: controller.signal,
      redirect: 'follow',
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      return c.json({});
    }

    const contentType = response.headers.get('Content-Type') || '';

    if (contentType.startsWith('image/')) {
      const result = {
        'og:image': url,
        'og:image:type': contentType,
      };
      await c.env.CACHE.put(cacheKey, JSON.stringify(result), { expirationTtl: 3600 });
      return c.json(result);
    }

    if (!contentType.includes('text/html')) {
      return c.json({});
    }

    const html = await response.text();
    const preview: Record<string, any> = {};

    const ogTitle = html.match(/<meta[^>]*property=["']og:title["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                   html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:title["'][^>]*>/i);
    if (ogTitle) {
      preview['og:title'] = decodeHtmlEntities(ogTitle[1]);
    } else {
      const title = html.match(/<title[^>]*>([^<]*)<\/title>/i);
      if (title) {
        preview['og:title'] = decodeHtmlEntities(title[1]);
      }
    }

    const ogDesc = html.match(/<meta[^>]*property=["']og:description["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                  html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:description["'][^>]*>/i);
    if (ogDesc) {
      preview['og:description'] = decodeHtmlEntities(ogDesc[1]);
    }

    const ogImage = html.match(/<meta[^>]*property=["']og:image["'][^>]*content=["']([^"']*)["'][^>]*>/i) ||
                   html.match(/<meta[^>]*content=["']([^"']*)["'][^>]*property=["']og:image["'][^>]*>/i);
    if (ogImage) {
      let imageUrl = ogImage[1];
      if (imageUrl.startsWith('/')) {
        imageUrl = `${parsedUrl.protocol}//${parsedUrl.host}${imageUrl}`;
      } else if (!imageUrl.startsWith('http')) {
        imageUrl = `${parsedUrl.protocol}//${parsedUrl.host}/${imageUrl}`;
      }
      preview['og:image'] = imageUrl;
    }

    if (Object.keys(preview).length > 0) {
      await c.env.CACHE.put(cacheKey, JSON.stringify(preview), { expirationTtl: 3600 });
    }

    return c.json(preview);
  } catch (error) {
    console.error('URL preview error:', error);
    return c.json({});
  }
});

// GET /_matrix/client/v1/media/config - Authenticated media config
app.get('/_matrix/client/v1/media/config', requireAuth(), async (c) => {
  return c.json({
    'm.upload.size': MAX_UPLOAD_SIZE,
  });
});

export default app;
