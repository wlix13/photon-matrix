// RS256 key pair used to sign OIDC id_tokens and to populate /.well-known/jwks.json

import { base64UrlEncode } from '../utils/ids';

// The key outlives every token it signs, so the KV entry is written without a TTL
const KV_KEY = 'oidc:id_token_signing_key';

const RSA_ALGORITHM = { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' } as const;

// id_token lifetime in seconds
export const ID_TOKEN_TTL = 3600;

// JWKS entry; the Workers JsonWebKey type has no kid
export interface PublicJwk extends JsonWebKey {
  kid: string;
  alg: string;
  use: string;
}

interface StoredKeyPair {
  kid: string;
  private_jwk: JsonWebKey;
  public_jwk: PublicJwk;
}

export interface IdTokenSigningKey {
  kid: string;
  privateKey: CryptoKey;
  publicJwk: PublicJwk;
}

// Isolate-local memo: re-reading KV and re-importing the JWK on every token request is pure overhead
let memoized: IdTokenSigningKey | null = null;

export async function getIdTokenSigningKey(cache: KVNamespace): Promise<IdTokenSigningKey> {
  if (memoized) return memoized;

  const stored = await loadOrCreateKeyPair(cache);
  const privateKey = await crypto.subtle.importKey('jwk', stored.private_jwk, RSA_ALGORITHM, false, [
    'sign',
  ]);

  memoized = { kid: stored.kid, privateKey, publicJwk: stored.public_jwk };
  return memoized;
}

// Public half of the signing key, in the shape clients expect from a JWKS document
export async function getIdTokenJwks(cache: KVNamespace): Promise<{ keys: PublicJwk[] }> {
  const key = await getIdTokenSigningKey(cache);
  return { keys: [key.publicJwk] };
}

// RS256 compact JWS: base64url(header).base64url(claims).base64url(signature)
export async function signJwt(
  claims: Record<string, unknown>,
  key: IdTokenSigningKey
): Promise<string> {
  const header = { alg: 'RS256', typ: 'JWT', kid: key.kid };
  const signingInput = `${encodeSegment(header)}.${encodeSegment(claims)}`;

  const signature = await crypto.subtle.sign(
    { name: RSA_ALGORITHM.name },
    key.privateKey,
    new TextEncoder().encode(signingInput)
  );

  return `${signingInput}.${base64UrlEncode(new Uint8Array(signature))}`;
}

function encodeSegment(value: unknown): string {
  return base64UrlEncode(new TextEncoder().encode(JSON.stringify(value)));
}

async function loadOrCreateKeyPair(cache: KVNamespace): Promise<StoredKeyPair> {
  const existing = await cache.get<StoredKeyPair>(KV_KEY, 'json');
  if (existing) return existing;

  const generated = await generateKeyPair();
  await cache.put(KV_KEY, JSON.stringify(generated));

  // Two Workers can miss the read and generate at the same time; KV keeps the last write,
  // so read back and prefer whatever is stored. Every isolate then converges on one key and
  // the JWKS it serves matches the tokens it signs.
  const stored = await cache.get<StoredKeyPair>(KV_KEY, 'json');
  return stored ?? generated;
}

async function generateKeyPair(): Promise<StoredKeyPair> {
  const pair = (await crypto.subtle.generateKey(
    { ...RSA_ALGORITHM, modulusLength: 2048, publicExponent: new Uint8Array([0x01, 0x00, 0x01]) },
    true, // extractable, so the key can be persisted as JWK
    ['sign', 'verify']
  )) as CryptoKeyPair;

  const privateJwk = (await crypto.subtle.exportKey('jwk', pair.privateKey)) as JsonWebKey;
  const publicJwk = (await crypto.subtle.exportKey('jwk', pair.publicKey)) as JsonWebKey;
  const kid = await jwkThumbprint(publicJwk);

  return {
    kid,
    private_jwk: privateJwk,
    // Only the members a verifier needs; key_ops/ext from the export are noise in a JWKS
    public_jwk: { kty: 'RSA', n: publicJwk.n, e: publicJwk.e, alg: 'RS256', use: 'sig', kid },
  };
}

// RFC 7638 thumbprint: a kid derived from the key material itself, stable for the key's lifetime
async function jwkThumbprint(jwk: JsonWebKey): Promise<string> {
  const canonical = JSON.stringify({ e: jwk.e, kty: jwk.kty, n: jwk.n });
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(canonical));
  return base64UrlEncode(new Uint8Array(digest));
}
