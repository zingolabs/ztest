// ztest's read path: seed bytes out of the snapshot bucket, unauthenticated.
//
// - Exists because `r2.dev` is rate-limited and bandwidth-throttled by design, and the
//   throttle is *variable*: 1.4 MB/s at worst, 23.7 MB/s at best on one object across a
//   day, with three multi-hour pulls dropped mid-stream. Not a throughput win — measured
//   same-minute the two are within noise. The win is having no documented throttle
// - Binding, not S3 → no credential in the Worker, in wrangler.jsonc, or on the wire
// - Serves `lfs/<64 hex>` and nothing else: the bucket is not a public filesystem, and a
//   key pattern is the one check that stays true if anything else is ever stored there
//
// Never front this route with Cache Everything. Cloudflare's cache answers a Range by
// stripping the header, pulling the *whole* body from the Worker, then slicing — against a
// 245 GiB seed that is pathological, and past the cacheable size limit it cannot even store
// the result. `no-store` below makes that misconfiguration inert.

/** Object keys ztest publishes: sha256, lowercase hex, under one prefix */
const KEY = /^lfs\/[0-9a-f]{64}$/;

export default {
	async fetch(request, env) {
		if (request.method !== "GET" && request.method !== "HEAD") {
			return new Response("Method Not Allowed", {
				status: 405,
				headers: { allow: "GET, HEAD" },
			});
		}

		const key = new URL(request.url).pathname.slice(1);
		if (!KEY.test(key)) {
			return new Response("Not Found", { status: 404 });
		}

		// HEAD is the existence probe (`blob_present`, `snapshot verify`): metadata only,
		// so it never pulls a body the runtime would discard
		if (request.method === "HEAD") {
			const meta = await env.SEEDS.head(key);
			if (meta === null) {
				return new Response("Not Found", { status: 404 });
			}
			const headers = headersFor(meta);
			// Explicit: with no body the runtime has no length to infer, and the probe
			// compares this against the manifest's `size_bytes` — absent reads as absent
			headers.set("content-length", String(meta.size));
			return new Response(null, { status: 200, headers });
		}

		// `range`/`onlyIf` read straight off the request headers — R2 parses both
		const object = await env.SEEDS.get(key, {
			range: request.headers,
			onlyIf: request.headers,
		});
		if (object === null) {
			return new Response("Not Found", { status: 404 });
		}

		const headers = headersFor(object);
		// A satisfied Range must answer 206 + Content-Range, or the puller cannot tell a
		// chunk from the whole object
		const ranged = object.range && "offset" in object.range;
		if (ranged) {
			const { offset, length } = object.range;
			headers.set(
				"content-range",
				`bytes ${offset}-${offset + length - 1}/${object.size}`,
			);
		}

		// No body = the `onlyIf` preconditions failed
		const hasBody = "body" in object;
		return new Response(hasBody ? object.body : null, {
			status: hasBody ? (ranged ? 206 : 200) : 412,
			headers,
		});
	},
};

function headersFor(object) {
	const headers = new Headers();
	object.writeHttpMetadata(headers);
	headers.set("etag", object.httpEtag);
	headers.set("accept-ranges", "bytes");
	headers.set("cache-control", "no-store");
	return headers;
}
