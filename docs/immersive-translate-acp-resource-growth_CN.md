# 沉浸式翻译压测 — ACP harness/session 无回收增长观测（2026-09-07）

## 背景
用户用沉浸式翻译（浏览器插件）翻译 YouTube 字幕，实时监控 moecloud VPS（103.136.187.119，1C/952Mi/2G swap）上的 cliproxy + agy_acp_server.par 资源占用。端点 `https://sfo2.openkiri.zip/v1`（Caddy → 127.0.0.1:8317）。

## 插件请求方式（实测日志特征）
- **模型**：客户端请求 `gemini-3.7-flash-low`（conductor_execution 行）；executor 实际执行 `gemini-3.7-flash-high`（TTFT 行 model=，见 skill 陷阱 5，-low 别名仍跑 high）。
- **节奏**：稳定 **~25-30s 一个请求**（字幕分段翻译，非爆发式）。瞬时双发也存在（11:03:30 + 11:03:32 相隔 2s）。
- **流式**：`stream=false`（全部观测均非流式）。
- **session_mode**：绝大多数 `prepared`（pool_wait≈0-6ms，命中预备 session）；偶发 `fresh`（pool_wait 2.1s + session_new 804ms，双发时预备缓存被掏空）。
- TTFT 4-9.6s，`backend_to_first_output` 占绝对大头（Google ACP 平台固有），代理侧开销可忽略。
- **无 stateful_reuse**：插件不发 `X-Session-ID`/`X-ACP-Session-Reuse`，每请求独立。
- **多段批量**：每个请求带 8 个字幕段（`[[p0]]`-`[[p7]]` + `[[source_end]]`），段号每批从 p0 重新编号——段号本身不携带跨请求的进度信息。

### 请求内可区分标记（同视频识别，实测）
- **有且仅有一个视频级标记**：prompt 的 Context Awareness 节固定携带 `Document Metadata:\nTitle: "<视频页标题> - YouTube"`。同一视频的全部请求该字符串恒定——实测 28 个请求全部命中同一标题（`(132) 反日の村に日本人が行ったら、現地の反応が想像以上にやばかった！【中国】 - YouTube`）；换页面（GitHub Options 页等）则 Title 随之变化。
- **没有** URL、视频 ID（无 `watch?v=`/`youtu.be`）、时间戳或段序列号——无法从请求体直接定位视频内进度。
- **Title 的不稳定前缀**：`(132) ` 是 YouTube 标签页的计数前缀（通知数），观看过程中可能变化。作分组键时应剥离 `^\(\d+\)\s` 前缀和 ` - YouTube` 后缀，取中间的纯视频标题。
- 其余可分层区分：来源 IP（所有插件请求同出口）、User-Agent（代理日志未记录，Caddy 未开 access log）、请求体内容哈希。
- **Header 层实测（Caddy access log，2026-09-07 部署后）**：无任何视频级标记。插件真实请求头恒定：`Origin: chrome-extension://amkbmndfnliijdhojkpoglbnaaahippg`、浏览器 UA（Edge/Chrome Windows）、无 Referer、无自定义 X-* 业务头；同时带 `Api-Key` + `Authorization` 双凭据头。**同视频分组只能依赖请求体 Document Metadata Title。**
- 佐证存储形态：daemon conversations/ 每 session 一对 `<uuid>.db+.meta`；`meta` 仅 `{"cwd": ...}`；翻译 prompt 存于 `steps.step_payload`（UTF-8 原文，可用 `CAST(step_payload AS TEXT)` 提取，`strings` 会被多字节切断）。

## localharness / session 增长方式（核心发现）
- `localharness_external` 是 **agy_acp_server.par（PID 58311）的子进程**，由 daemon 内部按 session 拉起（executor 通过 env `ANTIGRAVITY_HARNESS_PATH` 告知路径：antigravity_acp_executor.go:390-391，resolveHarness :254-258）。
- **1 请求 ≈ 1 个新 harness，时间戳滞后请求 10-45s**（请求释放 worker → 异步 refill 预建下个 session → daemon 为新 session spawn 新 harness）。
- **harness 永不回收**：最老一批存活 20+ 分钟仍在。每个 harness 稳态 RSS 7-9M + **swap 42-63M**（刚生时 RSS 45-73M，随即被换出）。
- **对话目录同步膨胀**：`/opt/cliproxy/agy-home/antigravity-acp/conversations/` 120 → 158（+38 / 20min，与请求数同量级，每 session 一目录）。

### 观测时间线
| 时刻 | harness 数 | 备注 |
|---|---|---|
| 10:57 基线 | 9 | 服务 10:49 启动后 8 min，已有数次手动冒烟 |
| 11:03（用户开始看视频） | 15 | +6/2min |
| 11:08 | 24 | +9/5min，15 请求/10min |
| 11:10 | 26 | 持续线性，无一退出 |
| 11:16+ 终态 | 37 | swap 1955M/2048M（余 93M），load 12.77（1 核） |

### 终态：翻译完全不可用（~11:16-11:18，开始观看约 25 分钟后）
- swap 几乎耗尽后 1C 全部陷入 swap thrash，load 飙到 12.77；par 进程 RSS 46M 大部被换出。
- 请求 TTFT 恶化到 10.7s+；11:16:05 的请求执行 **2m29s** 未完成，沉浸式翻译客户端超时断开（gin 日志 **499**），此后翻译完全无输出。
- 无 OOM kill（dmesg 干净），cliproxy 仍 active —— 是**功能性瘫痪**（thrash + 客户端超时），非进程死亡。
- 结论链：refill 每 25-30s 生 1 个 harness（40-63M swap 不还）→ swap 耗尽 → 1C thrash → TTFT 失控 → 客户端 499。

### 内存影响
- MemAvailable 279M → **157M**；SwapUsed 636M → **1955M**（仅剩 93M）。
- 主因不是单个 harness 大（RSS 7-9M），而是 **swap 泄漏式累积**：每个新 harness 初始 45-73M RSS，被内核换出后留 40-63M swap 不还。实测 37 个 harness × ~50M ≈ 1.9G，正好对应 swap 耗尽点（约 25 分钟内）。

## 监控工具（已部署在 VPS /root/，本会话遗留，可随时删）
- `/root/imm_monitor.sh` — 10s 采样守护：/root/imm_monitor.log（全量）/ imm_monitor_events.log（告警：avail<80M、harness>12、err>0）
- `/root/imm_probe.sh`、`/root/imm_probe2.sh`、`/root/imm_final.sh` — 一次性快照（进程年龄/swap per-pid/请求速率/TTFT/终态）
- 本地副本：/opt/data/imm_monitor.sh、imm_probe.sh、imm_probe2.sh、imm_final.sh

## 复测方法
```bash
ssh -i /opt/data/.ssh/litemoe_7515_ed25519 root@103.136.187.119 /root/imm_probe2.sh
ssh -i /opt/data/.ssh/litemoe_7515_ed25519 root@103.136.187.119 tail -30 /root/imm_monitor.log
```
