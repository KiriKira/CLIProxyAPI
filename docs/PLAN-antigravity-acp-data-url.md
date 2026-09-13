# PLAN: Antigravity ACP data URL attachment compatibility

## Scope

Fix inline attachment compatibility in the Antigravity ACP adapter without adding remote URL downloading.

Current state:

- image data URLs are supported through `image_url` / `input_image`;
- file data URLs are supported through `file` / `input_file`;
- Gemini `inlineData` supports image/audio/file bytes;
- `input_audio` supports raw base64 plus `format`;
- `audio_url` is not implemented and can currently be silently ignored;
- `parseACPDataURL()` decodes base64 but does not strictly require a `;base64` marker.

The target invariant is:

```text
recognized attachment intent
  -> converted to an ACP attachment block
  OR
  -> rejected with HTTP 400

never
  -> HTTP 200 after silently dropping the attachment
```

## Phase 1: make data URL parsing strict and shared

Harden `parseACPDataURL()` and lock the contract with table tests.

Required behavior:

- require the `data:` prefix;
- require an explicit MIME type;
- require a comma separator and non-empty payload;
- require a `base64` parameter;
- preserve whitespace-tolerant base64 decoding;
- keep existing MIME normalization such as `image/jpg` -> `image/jpeg`;
- reject percent-encoded/non-base64 data URLs.

Add parser tests for valid image/audio data URLs, MIME parameters, whitespace folding, missing `base64`, malformed payloads, missing MIME, and invalid base64.

Exit criterion: parser behavior matches the limitation document and all existing valid image/file fixtures remain green.

## Phase 2: implement `audio_url`

Add an explicit `audio_url` branch in `acpAttachmentBuilder.handlePart()`.

Accepted form:

```json
{
  "type": "audio_url",
  "audio_url": {
    "url": "data:audio/wav;base64,<payload>"
  }
}
```

Flow:

```text
audio_url.url
  -> parseACPDataURL
  -> addAudio
  -> existing 20 MiB audio / 50 MiB total limits
  -> ACP audio prompt block
```

The same path must work inside Chat Completions message content and Responses input items because both converge on `handlePart()`.

Remote `http(s)` audio URLs remain unsupported and must return HTTP 400 rather than fall through to text-only success.

Exit criterion: WAV and MP3 `audio_url` data URLs produce ACP audio blocks on both OpenAI-shaped input paths.

## Phase 3: allow data URLs in `input_audio`

Preserve the current raw-base64 form:

```json
{
  "type": "input_audio",
  "input_audio": {
    "data": "<raw-base64>",
    "format": "wav"
  }
}
```

Also accept:

```json
{
  "type": "input_audio",
  "input_audio": {
    "data": "data:audio/wav;base64,<payload>",
    "format": "wav"
  }
}
```

Rules:

1. If `data` starts with `data:`, use the MIME from the data URL.
2. If `format` is also present, map it through `acpAudioFormatMIMEs` and verify it agrees with the data URL MIME after small alias normalization.
3. If MIME and format conflict, return HTTP 400.
4. If a supported audio data URL is present without `format`, allow the MIME in the data URL to be sufficient.
5. If `data` is raw base64, retain the existing requirement for a supported `format`.

Exit criterion: old clients remain compatible, data-URL clients work, and contradictory metadata fails deterministically.

## Phase 4: remove known silent attachment loss

Do not change the existing compatibility rule that arbitrary unknown provider-extension parts may be ignored.

Instead, explicitly reject known attachment intent that cannot be forwarded:

- malformed or remote `audio_url`;
- malformed or remote `image_url` / `input_image`;
- unusable `file` / `input_file` inline data;
- known non-inline Claude image sources;
- recognized attachments with unsupported MIME, invalid base64, or size violations.

Do not replace the default branch with blanket `unknown type -> 400`, because reasoning/tool/provider-extension blocks may legitimately pass through this adapter.

Exit criterion: every recognized attachment either reaches ACP or returns a clear client error.

## Phase 5: tests

Extend `internal/runtime/executor/antigravity_acp_executor_test.go` with a focused matrix:

- `audio_url` WAV data URL -> one ACP audio block;
- `audio_url` MP3 data URL -> one ACP audio block;
- Responses top-level `audio_url` -> ACP audio block;
- existing raw-base64 `input_audio` remains green;
- data-URL `input_audio` with matching format -> ACP audio block;
- data-URL `input_audio` without format -> ACP audio block;
- MIME/format mismatch -> HTTP 400;
- remote `audio_url` -> HTTP 400;
- malformed/non-base64 data URL -> HTTP 400;
- unsupported audio MIME -> HTTP 400;
- oversized audio -> existing size error;
- existing image/PDF/text/Claude/Gemini attachment tests remain green;
- unrelated unknown non-attachment content-part type is still ignored for compatibility.

Add one regression that proves `audio_url` can no longer yield successful text-only output after the audio is dropped.

## Phase 6: documentation and live verification

Update `docs/antigravity-acp-input-limitations.md` after implementation:

- move `audio_url` data URLs into the supported matrix;
- document that `input_audio.data` accepts both raw base64 and audio data URLs;
- document strict `;base64` parsing;
- keep remote URL fetching explicitly unsupported;
- remove the old silent-ignore warning once the regression is fixed.

Real endpoint checks:

1. WAV `audio_url` is understood/transcribed;
2. MP3 `audio_url` is understood/transcribed;
3. raw-base64 `input_audio` still works;
4. data-URL `input_audio` works;
5. PNG and PDF data URLs still work;
6. remote audio URL returns 400;
7. malformed or MIME-mismatched audio returns 400.

Record the tested commit and verification date in the limitation document.

## Expected touch points

Implementation:

- `internal/runtime/executor/antigravity_acp_executor.go`
  - `parseACPDataURL()`
  - small audio MIME canonicalization helper if required
  - `acpAttachmentBuilder.handlePart()`

Tests:

- `internal/runtime/executor/antigravity_acp_executor_test.go`

Docs:

- `docs/antigravity-acp-input-limitations.md`

No ACP protocol or translator-layer change should be necessary.

## Non-goals

Not part of this plan:

- remote `http(s)` attachment fetching;
- multipart/file-upload API work;
- percent-encoded non-base64 data URLs;
- MIME sniffing or transcoding;
- attachment size-limit changes;
- blanket rejection of every unknown content part.
