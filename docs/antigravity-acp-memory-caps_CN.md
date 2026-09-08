# Antigravity ACP 内存预算与 worker/session 上限建议（moecloud）

日期：2026-09-08。生产机 moecloud 103.136.187.119（1C / 952MB RAM / 2G swap），分支 `feat/antigravity-session-lifecycle-step1`（v7.2.151-kiri.11+，document 复用已上线）。数据采集于 document 模式真实流量（YouTube 字幕 12 批 / ~5 min）。

## 实测内存构成

| 组件 | RSS | swap | 备注 |
|---|---|---|---|
| 系统 + warp-svc/caddy/journald/sshd | ~150MB | — | 固定开销 |
| cli-proxy-api（代理本体） | 41–60MB | 2MB | 随负载小涨 |
| agy_acp_server.par（常驻 daemon） | 162MB | 89MB | 最大单件；自解压 + 编排 |
| localharness_external（空 / prepared） | ~58MB | 4–9MB | 刚 spawn、未携带上下文 |
| localharness_external（document 绑定） | 60→120MB | ~0 | 随字幕批次上下文增长（12 批内 58→120MB） |
| available（活跃时） | 282MB | swap 已用 405MB | |

**关键修正**（对照早期观测）：「每 harness 7–9MB」是**空 session** 的稳态值。document 复用上线后，携带真实翻译上下文的 harness 起步 ~58MB，5 分钟内涨到 ~120MB。容量规划必须按 **60–120MB / 绑定会话** 计算。

## 内存预算

```
952MB RAM − ~150MB 系统 − ~60MB 代理 − ~162MB par ≈ 580MB 给 harness 层
留 15% 余量 → 有效预算 ~450–500MB
```

swap 2G 只作冲击吸收、不作日常容量：swap 驻留会放大 1 核机器的 IO 延迟，直接反映为 TTFT 劣化。

## 推荐配置（2026-09-08 已应用到生产）

```yaml
antigravity:
  max-workers: 1                      # 1 核铁律
  max-workers-total: 1
  prepared-sessions: 1                # 1 个预备会话 ≈ 1 个 58MB 空闲 harness
  max-sessions-per-worker: 6          # 6 × ~80MB 均值 ≈ 480MB 峰值 < 500MB 预算
  max-abandoned-sessions-per-worker: 3
  max-stateful-sessions: 16
  stateful-session-ttl: "30m"
  idle-timeout: "45m"
```

推理：

- **max-workers / max-workers-total = 1**：1 核上多 worker 是 CPU 负优化（2026-09-05 实测），内存上 par 的 162MB 常驻部分也按 worker 翻倍。
- **max-sessions-per-worker = 6**：使用形态是「1–2 个活跃视频 + prepared 1 个 + 偶发网页翻译」，日常活跃 2–4 个；6 留一档缓冲，峰值仍在线内。触顶 → worker 统一回收（R1a/R1b 保证 forward progress 与物理进程让位）→ 内存归零重来。session/new 只在 bootstrap 时发生，document_reuse 批不消耗额度。
- **max-stateful-sessions = 16**：绑定表是 KB 级元数据；默认 256 无内存风险但会让 document key 无界累积，16 够日常（LRU 兜底）。
- **stateful-session-ttl = 30m**：闲置绑定过期，代价是一次重新 bootstrap（热池 ~6s TTFT）。
- **idle-timeout = 45m**：长时间不用整树回收归还 ~500MB；代价是下一请求冷启 ~15s + prepared 缓存全丢。看完视频半小时内再开不触发。

## 残余风险

1. **单会话无界增长**：max-sessions 管的是会话个数，管不住单个超长视频（2h+、数百 turn）把一个 harness 推到几百 MB。需要 PLAN §5 的 `document-session-max-turns` / `document-session-max-estimated-tokens` 滚动回收（代码尚未实现）。Step 6 soak 顺便量增长曲线再定值。
2. **触顶回收节奏**：长期连续观看会周期性把 worker 顶到 6-session 上限触发回收，每次代价 = 下一批重新 bootstrap。若 soak 显示该节奏打扰明显，优先调大 per-worker 上限前先确认峰值内存仍有余量。
3. **token 看门狗**：`/root/agy_token_watchdog.sh` + `agy-token-watchdog.timer`（2 分钟周期，防 daemon 删 `acp_token.json`）。2026-09-08 起**按 Kiri 要求停用**；重开：`systemctl enable --now agy-token-watchdog.timer`。

## 复测方法

- 内存/进程扫描：VPS `/root/mem_budget.sh`（本地源 `/opt/data/cliproxy-probe/mem_budget.sh`）
- document soak 快照（DB 增长 + 进程/swap + 模式计数）：VPS `/root/doc_soak_snapshot.py`
- R6 载荷验收：VPS `/root/r6_verify.py`
- caps 生效确认：`journalctl -u cliproxy | grep -iE "session|worker"` 结合 TTFT 行的 `session_mode`
