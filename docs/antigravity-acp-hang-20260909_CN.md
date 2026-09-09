# Antigravity ACP daemon 单 worker 僵死事故分析（2026-09-09）

## 摘要

moecloud 上 CLIProxyAPI 的 antigravity ACP 通道发生「上游卡死型」僵死：daemon（par）进程存活、出口链正常，但不再处理任何请求，后续请求全部在单 worker 队列上排队饿死，客户端（沉浸式翻译）100s 超时后 499。经排除法定位为 daemon 内部僵死（疑与一条异常慢的 Google 交互后状态损坏有关，par 为闭源二进制无法看到内部）。恢复手段 = `systemctl restart cliproxy`。7 天日志窗口内属首次。本文记录时间线、证据、排除项与未落地的修复方向。

## 故障时间线（UTC 2026-09-09）

| 时间 | 事件 |
|---|---|
| ~12:19:19 | par daemon 新起（进程寿命倒推），token 文件 12:19 被改写（daemon 正常工作痕迹） |
| 12:19:35 | 沉浸式翻译网页翻译流：~24 条并发请求在 9 秒内齐发（来源 IP 152.175.5.34，模型 gemini-3.8-flash-low） |
| 12:19:48 | 仅 1 条请求慢速成功：总耗时 13.5s（正常热池 2-3s），TTFT 分解 `backend_to_first_output_ms=5986`（Google 侧首响应 6s，明显偏慢）。`session_mode=fresh`，prompt 载荷仅 1.8KB |
| 12:19:48 之后 | daemon 僵死：journal 无任何 executor 日志，所有后续请求排队，无一行 TTFT |
| 12:21:15-12:21:31 | 批量 499（客户端 100s 超时断开）：15 条 1m19s-1m41s 级 |
| 12:26-12:29 | 用户新请求（gemini-3.7-flash-low，含本机诊断直连）同样挂死 499 |
| 12:38 | 执行 `/root/imm_restart.sh` 重启恢复；冒烟冷池 7.6s / 热池 2.98s 正常 |

当日 499 共 26 条（挂死签名 15 条），此前 7 天为 0。

## 排除项（附证据）

| 假设 | 结论 | 证据 |
|---|---|---|
| WARP/出口链问题 | **排除** | `curl -x 127.0.0.1:8119 https://www.cloudflare.com/cdn-cgi/trace` → `warp=on, loc=US`；经同链访问 `cloudcode-pa.googleapis.com` / `oauth2.googleapis.com` 均 404-reachable，0.05-0.19s |
| 内存耗尽型 OOM | **排除** | avail 350-480M、swap 用 244-331M / 2048M、load 0.1-0.2；harness 仅 1 个（swap 型故障为数十个 + load≥10） |
| 认证/token 失效（401 cooldown 型） | **排除** | 401（Invalid API key）秒回，说明认证层健康；agy.json 在列，token 文件新鲜 |
| Caddy/公网入口问题 | **排除** | 故障期间 127.0.0.1 直连 8317 同样挂死 |
| 批量大小（maxTextGroupLengthPerRequest=16）导致 | **排除** | 见下节实测 |

## 确认的证据

1. **真 key 请求 30s 零字节**：故障期间 `curl 127.0.0.1:8317`（Authorization 真实 key）`-m 30` 无任何输出——executor 路径整体僵死；而 401 秒回证明 gin/认证层正常。
2. **daemon 无对 Google 的连接**：`ss -tnp` 中 par/harness 仅有本地 127.0.0.1 互联（par↔harness WS），无任何对 Google 的 ESTAB——既不在传数据也不在建连，内部僵死。
3. **最后一条成功请求的 TTFT 异常**：`backend_to_first_output_ms=5986`（正常 2-3s 档），总 13.5s。僵死紧随其后。
4. **请求级无超时兜底**：daemon 卡住后无任何自动恢复/重建，只能外部重启——这是「挂死不自愈」的结构性原因。

## 负载形态实测（两条翻译流完全不同）

从 daemon `conversations/<uuid>.db` 的 `steps.step_payload`（`CAST(... AS TEXT)` 提取）还原真实请求体：

| 流 | 到达模式 | 批量（%% 分隔块数） | 载荷 |
|---|---|---|---|
| 网页翻译（新闻页「中国製エアバッグ…」） | **全批次并发齐发**：~24 条/9s | 18 块 / 1776 B | 事发会话 |
| 视频字幕翻译（YouTube） | **串行**：~1 请求/27s | 31-42 块 / 677-2615 B | 当日 10:29 成功翻译的同一形态 |

- **批量大小与成败无关**：42 块的串行请求照样成功；事发失败批次只有 18 块 1.8KB（输入长度斜率实验早已排除 prefill 瓶颈：+5K tokens 仅 +1-2s）。
- 插件当前请求格式已改为 `%%` 分隔段落（旧 reference 中的 `[[pN]]` 标记已不再出现）；批量大小 ≈ 插件「每次字幕请求最大段落数」设置 × 每条目行数，且实际受「每次请求最大文本长度」（默认 1800 字符）封顶——两个流实测都被压在 ~1.8-2.6KB。
- 网页翻译的并发洪峰规模 ≈ 页面段落数 ÷ 每请求段落数：**调大批量反而降低并发**（用户设 16 时洪峰 24 条；用常见默认 3-10 会是 40-80 条）。

## 根因分析（推断部分已标注）

**确定**：单 worker（max-workers:1，1C 铁律）+ daemon 无请求级超时 → 任一请求把 daemon 带入坏状态，整个通道即挂死且不自愈。

**推断**（par 闭源、无内部日志）：触发点 = 并发洪峰中首条请求在 Google 侧异常慢（6s 首响应），其后 daemon/harness 在该交互的收尾/中断处理中卡死（疑 gRPC 流挂起或内部锁问题）。并发洪峰是诱因不是根因——串行流下同样可能被单条慢响应触发，只是暴露概率低。

## 已做处置

- `/root/imm_restart.sh` 一键恢复（restart + is-active + 孤儿检查 + 内存 + 两轮冒烟），本次实测有效。
- 诊断脚本：`/root/imm_hang.sh`（procs/egress/token/manual-request 鉴别）、本会话新增 `agy_logcheck.sh`、`agy_netcheck.sh`、`agy_hangconfirm.sh`、`agy_hanghist.sh`、`agy_segcount.sh`（均在本地 `/opt/data/cliproxy-probe/` 有副本）。

## 未落地的改善方向（按投入排序）

1. **客户端削峰**（用户侧设置，已确认方向）：网页翻译「每秒最大请求数」设 1；两个批量设置（每请求段落数 + 每请求文本长度）成对调大。效果 = 降低触发概率与洪峰规模，**不能防住** daemon 僵死本身。
2. **探针看门狗**（VPS，低成本）：systemd timer 每 2 分钟用真 key 探 `127.0.0.1:8317`（max_tokens=3、15s 超时），连续 2 次挂死自动跑 `imm_restart.sh`。故障发现到恢复从 ~20 分钟缩到 ≤4 分钟。
3. **代理侧加固**（代码，治本）：CLIProxyAPI 对 daemon 调用加硬超时 + harness 自动重建（单请求卡死不再拖死 worker）；可顺带评估挂死探针内建。需排入 PLAN.md。
4. **daemon 侧**（改不动）：par 闭源，无内部超时/可观测性可用。

## 教训（操作层）

- 鉴别「上游卡死型」与「swap 耗尽型」先跑 `/root/imm_hang.sh`：两者处置同为重启，但内存/负载/直连表现相反，误判会浪费排障时间。
- 探针脚本里硬编码 key 会被安全过滤器掩码成 `***`，照抄掩码字面量只会测出 401 假象；新脚本在远端取真 key。
- 插件请求格式随版本漂移（`[[pN]]` → `%%`），依赖请求体形态的分析先抓最新 payload 确认格式再用旧假设。
