# Antigravity ACP Input Attachment Limitations

This document describes the attachment behavior of the Antigravity ACP executor in
CLIProxyAPI. It covers the proxy adapter's current request-shape support; it is
not a general compatibility promise for every future Antigravity ACP release.

## Summary

Antigravity ACP can receive native image and audio prompt blocks. CLIProxyAPI
translates supported OpenAI, Gemini, and Claude-shaped inputs into those ACP
blocks. The current limitation is mostly in this translation layer:

- Base64 data URLs are supported for images and file attachments.
- Audio is supported through `input_audio` with raw base64, but not through the
  OpenAI-style `audio_url` content part.
- Remote `http(s)` URLs are not fetched by the executor.
- Unsupported content-part types are currently ignored in some paths, which can
  produce an HTTP 200 response even though the attachment was not sent to ACP.

## ACP-level model

The executor sends attachments to ACP as prompt blocks. The relevant native
shapes are:

```json
{
  "type": "image",
  "data": "<base64>",
  "mimeType": "image/png"
}
```

```json
{
  "type": "audio",
  "data": "<base64>",
  "mimeType": "audio/wav"
}
```

PDFs are handled differently: the executor stages the decoded bytes in a
short-lived local file and sends an ACP `resource_link` block. Text files are
sent as ACP resource blocks after validation.

The ACP protocol implementation is in `internal/acp/protocol.go`; attachment
conversion is in
`internal/runtime/executor/antigravity_acp_executor.go`.

## Supported request shapes

The following shapes are supported by the current executor.

| Input | Required form | Result |
|---|---|---|
| Image | `image_url.url` containing a base64 data URL | Supported |
| Image | Responses-style `input_image` containing a base64 data URL | Supported |
| Image | Claude `image` with `source.type = "base64"` | Supported |
| Image | Gemini `inlineData` with an image MIME type | Supported |
| Audio | `input_audio.input_audio.data` containing raw base64 plus `format` | Supported |
| Audio | Gemini `inlineData` with an audio MIME type | Supported |
| PDF | `file.file_data` containing a base64 data URL | Supported |
| PDF | `input_file.file_data` containing a base64 data URL | Supported |
| Text file | `file.file_data` containing a base64 data URL and a supported filename/MIME type | Supported |
| Text/PDF | Gemini `inlineData` | Supported by the Gemini contents parser |

Examples:

### Image data URL

```json
{
  "type": "image_url",
  "image_url": {
    "url": "data:image/png;base64,<base64-payload>"
  }
}
```

### Audio raw base64

```json
{
  "type": "input_audio",
  "input_audio": {
    "data": "<raw-base64-payload>",
    "format": "wav"
  }
}
```

The audio `data` value must be raw base64. It must not include the
`data:audio/wav;base64,` prefix.

### PDF data URL

```json
{
  "type": "file",
  "file": {
    "filename": "document.pdf",
    "file_data": "data:application/pdf;base64,<base64-payload>"
  }
}
```

## Unsupported or limited shapes

### `audio_url` is not implemented

The OpenAI content part below is currently not converted to an ACP audio block:

```json
{
  "type": "audio_url",
  "audio_url": {
    "url": "data:audio/wav;base64,<base64-payload>"
  }
}
```

`audio_url` is not recognized by the executor's content-part switch. Because
unknown parts are currently ignored, this may return HTTP 200 while the model
receives no audio. This is an adapter limitation, not evidence that ACP cannot
process audio.

### Remote URLs are not fetched

The executor accepts data URLs only for the image and file branches. These forms
are not supported:

```json
{
  "type": "image_url",
  "image_url": {
    "url": "https://example.com/image.png"
  }
}
```

```json
{
  "type": "file",
  "file": {
    "filename": "document.pdf",
    "file_url": "https://example.com/document.pdf"
  }
}
```

A remote URL would require the proxy to download the object before constructing
an ACP block. ACP itself does not perform this fetch for the proxy.

### Audio data URLs are not accepted through `input_audio`

This is invalid for the current implementation:

```json
{
  "type": "input_audio",
  "input_audio": {
    "data": "data:audio/wav;base64,<base64-payload>",
    "format": "wav"
  }
}
```

Use raw base64 in `input_audio.data`, or use a future `audio_url` adapter once
implemented.

### Gemini `fileData` is not readable here

The Gemini contents parser accepts inline bytes through `inlineData`. A
`fileData` URI cannot be read by the executor and is rejected with a client
error; callers must inline the file bytes instead.

## MIME and size limits

The current executor enforces these limits per turn:

- Images: up to 10 MiB per image.
- Audio: up to 20 MiB per audio attachment.
- Text files: up to 1 MiB; content must be valid UTF-8 and must not contain NUL
  bytes.
- PDFs: up to the 50 MiB total attachment limit.
- All attachments together: up to 50 MiB.

Supported image MIME types are `image/bmp`, `image/jpeg`, `image/png`, and
`image/webp`.

Supported audio MIME types are `audio/aac`, `audio/flac`, `audio/mpeg`,
`audio/mp4`, `audio/m4a`, `audio/x-m4a`, `audio/ogg`, `audio/wav`,
`audio/x-wav`, and `audio/webm`.

The data URL parser accepts base64 data URLs of the form:

```text
data:<mime-type>;base64,<payload>
```

Non-base64 data URLs are not supported. Whitespace in a base64 payload is
removed before decoding. `image/jpg` is normalized to `image/jpeg`.

## Failure behavior

The failure behavior is not yet uniform:

- Recognized image/file branches with a remote URL normally return HTTP 400 with
  a message that a base64 data URL is required.
- Invalid MIME types, malformed base64, and size-limit violations return HTTP
  400.
- Unknown content-part types, including the current `audio_url` branch, may be
  silently ignored and leave only the text portion of the request.

The last behavior is a correctness and observability limitation. A future
change should reject unsupported attachment types instead of treating them as a
successful text-only request.

## Verification snapshot

The behavior described here was verified against the deployed AGY endpoint on
2026-09-13, using the CLIProxyAPI tree at commit `f55f1fa1`:

- `input_audio` with WAV/MP3 raw base64 reached the model and produced a correct
  transcription.
- An image supplied as a PNG data URL was interpreted correctly.
- A PDF supplied through `file.file_data` and `input_file.file_data` was read
  correctly.
- A remote image URL was rejected rather than downloaded.
- `audio_url` with a WAV data URL returned HTTP 200 but the model reported no
  audio, confirming the missing adapter branch.

The relevant implementation and tests are:

- `internal/acp/protocol.go`
- `internal/runtime/executor/antigravity_acp_executor.go`
- `internal/runtime/executor/antigravity_acp_executor_test.go`

## Follow-up work

If broader compatibility is needed, the smallest safe sequence is:

1. Add an `audio_url` branch that parses base64 data URLs and reuses `addAudio`.
2. Change unknown attachment types from silent ignore to a clear HTTP 400 error.
3. Decide separately whether remote URL fetching is desirable; if so, add
   explicit SSRF protection, response-size limits, MIME validation, and download
   timeouts before constructing ACP blocks.
4. Add focused tests for `audio_url`, remote URLs, malformed data URLs, and the
   exact OpenAI Responses input shapes used by clients.
