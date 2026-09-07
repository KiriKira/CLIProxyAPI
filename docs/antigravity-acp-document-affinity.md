# ACP document-affinity protocol

This protocol is opt-in. Requests without the document scope and reuse signals
retain the normal stateless or strict turn-based stateful behavior.

## Request metadata

A document-affinity request sends:

```http
X-ACP-Session-Scope: document
X-ACP-Session-Reuse: 1
X-ACP-Client: immersive-translate
```

`X-ACP-Document-ID` is optional. When present, it is the primary document
identity. Otherwise the executor derives the identity from the title in the
request payload. `Origin`, `User-Agent`, IP address, and extension ID are not
used as document identity.

## Title marker

The first request for a document should include a title context marker in a
user prompt string:

```text
[[CLIPROXY_ACP_TITLE_PROMPT:v1]]
Title: "(132) Example video - YouTube"
[[/CLIPROXY_ACP_TITLE_PROMPT]]
```

The executor removes only the marker delimiters before sending the prompt to
the ACP daemon. The title context remains visible to the model. The optional
numeric notification prefix is normalized, so `(132) Example video - YouTube`
and `(133) Example video - YouTube` map to the same title identity.

If the marker is absent, document mode accepts the existing `Document
Metadata:` / `Title:` prompt form as a compatibility fallback. A request that
has neither an explicit document ID nor a title is rejected rather than
creating an ambiguous global binding.

## Reuse behavior

- The binding key is namespaced by auth identity, client, normalized document
  identity, model variant, source format, and a semantic fingerprint of the
  system/developer configuration.
- Requests for one key are serialized by a document lane.
- The first request sends the cleaned full payload and binds its ACP session
  only after a successful prompt.
- A later hit sends only the newest user batch, not prior user/assistant history.
- Prompt failure invalidates the document binding; it never advances the
  binding state.
- Worker death purges document bindings owned by that worker.

The document table uses the configured stateful-session TTL and maximum binding
count. Session creation remains bounded by the worker session caps introduced
in Step 2.
