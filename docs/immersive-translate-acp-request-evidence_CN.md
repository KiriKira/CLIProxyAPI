# 沉浸式翻译请求原始证据附录（2026-09-07）

主文档：`immersive-translate-acp-resource-growth_CN.md`。本文件保存当时的原始记录样本，供后续优化分析引用。

## 1. 插件真实请求 Header（Caddy access log 原始 JSON，凭据已脱敏）

取自 `/var/log/caddy/sfo2-access.log`（当时唯一一条完整记录的插件请求，挂起 98.7s 后客户端 499 断开）。所有插件请求的 header 组合完全一致，仅 `Content-Length` 随字幕批次变化（2142-2203B）：

```json
{
  "ts": 1788783886.23,
  "status": 0,
  "duration": 98.7,
  "request": {
    "remote_ip": "<插件出口IP>",
    "proto": "HTTP/2.0",
    "method": "POST",
    "host": "sfo2.openkiri.zip",
    "uri": "/v1/chat/completions",
    "headers": {
      "Accept": ["*/*"],
      "Accept-Encoding": ["gzip, deflate, br, zstd"],
      "Accept-Language": ["en,zh-CN;q=0.9,zh;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5"],
      "Api-Key": ["<REDACTED>"],
      "Authorization": ["<REDACTED>"],
      "Content-Type": ["application/json"],
      "Dnt": ["1"],
      "Origin": ["chrome-extension://amkbmndfnliijdhojkpoglbnaaahippg"],
      "Priority": ["u=1, i"],
      "Sec-Fetch-Dest": ["empty"],
      "Sec-Fetch-Mode": ["cors"],
      "Sec-Fetch-Site": ["none"],
      "User-Agent": ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36 Edg/152.0.0.0"]
    },
    "tls": {"version": 772, "cipher_suite": 4865, "proto": "h2", "server_name": "sfo2.openkiri.zip"}
  }
}
```

要点：
- **无 Referer、无 Cookie、无任何自定义 X-\* 业务头** → header 层无视频级标记，同视频分组只能依赖请求体。
- `Api-Key` 与 `Authorization` 双凭据头并存（沉浸式翻译同时发送两种；`Api-Key` 为用户配置的端点 key）。
- TLS 1.3 (0x0304=772) + h2，直连（`Sec-Fetch-Site: none`，扩展后台 fetch）。
- `amkbmndfnliijdhojkpoglbnaaahippg` = 沉浸式翻译 Chrome 扩展固定 ID，同一浏览器所有请求相同。

## 2. 按视频标题分组（daemon conversations/ 全量 91 个 db，12:27 扫描）

| 请求数(session数) | Document Metadata Title | 说明 |
|---|---|---|
| 28 | “(132) 反日の村に日本人が行ったら、現地の反応が想像以上にやばかった！【中国】 - YouTube” | 本次压测视频 |
| 21 | “Options” | 插件设置页（非翻译请求？仍带 Title） |
| 4 | “nxtrace/NTrace-core: NextTrace, an open source visual route tracking CLI tool” | GitHub 页面 |
| 38 | 无 Title | 冒烟测试等非沉浸式翻译请求 |

注：db 数（91）> TTFT 请求数，因包含 prepared/refill 预建 session 与重启前遗留。

## 3. 翻译请求 Prompt 解剖（提取自 conversations/<uuid>.db 的 steps.step_payload）

step_payload 为 protobuf 包裹的 UTF-8 文本，剥离二进制头后的完整结构（节选，字幕正文截断）：

```text
[SYSTEM]: You are a professional Simplified Chinese Language native translator. Translate each source item accurately and fluently into Simplified Chinese Language.

## Translation Requirements
- Preserve its meaning, tone, paragraph structure, and meaningful formatting.
- Keep code, placeholders, and content that must not be translated unchanged.
- Use supplied document context and terminology consistently.

## Context Awareness
Document Metadata:
Title: "(132) 反日の村に日本人が行ったら、現地の反応が想像以上にやばかった！【中国】 - YouTube"

## Multi-item processing protocol
The input contains source items. Each item starts with its exact request id on one title line, followed by that item's source text. The source list ends with exactly one [[source_end]] line. Process every item independently according to the active task instructions, using the other items only as context. Return every supplied id exactly once in any order and preserve each id title byte-for-byte. Processed text may span multiple lines.
...（响应格式约束，略）

[USER]: Translate to Simplified Chinese Language:

[[p0]]
臭いですよ。やっぱこのゴミ、ゴミの香りがすごい。
[[p1]]
これなんか右側がいいんじゃねえか。ああ、
...（[[p2]]-[[p7]] 同构，略）
[[source_end]]
```

要点：
- 每请求 8 段（[[p0]]-[[p7]]），段号每批从 p0 重排，无跨请求进度信息。
- 唯一视频级标记 = `Title:` 行；无 URL/视频ID/时间戳。
- Title 前缀 `(132) ` 为 YouTube 标签页计数，不稳定，作分组键需剥离。

## 4. 原始数据留存位置（2026-09-07 时点）

- Caddy access log（原始 JSON，含全部请求头）：VPS `/var/log/caddy/sfo2-access.log`（20MiB 滚动，保留 1 份）
- daemon 对话存储：VPS `/opt/cliproxy/agy-home/antigravity-acp/conversations/<uuid>.db`（`steps.step_payload`，用 `CAST(step_payload AS TEXT)` 提取；`strings` 会被多字节切断）
- 中间产物：VPS `/tmp/title_map.txt`（db→标题映射）、`/tmp/prompt_full.txt`（完整 prompt 样本，重启后丢失）
- 监控/探针脚本：VPS `/root/imm_*.sh`，本地 `/opt/data/imm_*.sh`
