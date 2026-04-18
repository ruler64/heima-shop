# Windows 防火墙规则回滚说明（XXL-Job）

> 目的：这份文档用于记录“当前可用状态”以及“如何一键回滚到之前状态”，避免后续网络再次不通时无从下手。

---

## 1. 当前已验证可用的状态（先记结论）

- `trade-service` 在 Windows 上监听端口 `9999`。
- Linux 可以访问 `http://192.168.31.1:9999`（说明网络路径已通）。
- 必须保留一条放行规则：`Allow XXL-Job 9999 Inbound`。

> 注释：
> - 你之前出现的是 `connect timed out`，根因是 Windows 防火墙规则冲突。
> - 修复后 `curl` 能返回内容（哪怕是 500），就表示“端口可达 + 服务有响应”。

---

## 2. 必须保留的规则（不要删）

### 规则名
`Allow XXL-Job 9999 Inbound`

### 规则含义
- 方向：`Inbound`（入站）
- 协议：`TCP`
- 端口：`9999`
- 动作：`Allow`
- 配置文件：`Any`

> 注释：
> - 这条规则是让 Linux 调度中心能够访问 Windows 执行器的关键规则。
> - 如果删掉它，通常又会回到超时问题。

---

## 3. 排障时临时禁用过的“阻断规则”（重点）

以下规则是 `Public + Inbound + Block`，在你的环境里会干扰 Java 服务入站访问：

- `Java(TM) Platform SE binary`
- `java`
- `IntelliJ IDEA 2022.1.3`
- `Node.js JavaScript Runtime`

> 注释：
> - 你的网络类型是 `Public`，而这些规则恰好针对 `Public` 生效。
> - 即使你新增了 9999 放行规则，这些旧规则也可能造成冲突或不可预期行为。

---

## 4. 一键检查命令（PowerShell）

> 用途：快速确认“当前是否仍处于可用状态”。

```powershell
# 1) 看 9999 是否被监听（有 LISTENING 才行）
netstat -ano | findstr 9999

# 2) 看关键放行规则是否启用
Get-NetFirewallRule -DisplayName "Allow XXL-Job 9999 Inbound" |
  Select-Object DisplayName, Enabled, Direction, Action, Profile

# 3) 看是否仍有 java/IDEA/Node 的 Public 阻断规则处于启用
Get-NetFirewallRule -Enabled True |
  Where-Object {
    $_.Direction -eq 'Inbound' -and
    $_.Action -eq 'Block' -and
    $_.Profile -match 'Public' -and
    $_.DisplayName -match 'java|Java|IntelliJ|IDEA|Node'
  } |
  Select-Object DisplayName, Enabled, Action, Direction, Profile
```

---

## 5. 一键修复到“可用状态”命令（PowerShell）

> 用途：如果再次超时，直接执行这组命令恢复到当前已验证可用的状态。

```powershell
# A. 确保 9999 放行规则存在（若不存在则创建）
if (-not (Get-NetFirewallRule -DisplayName "Allow XXL-Job 9999 Inbound" -ErrorAction SilentlyContinue)) {
  New-NetFirewallRule -DisplayName "Allow XXL-Job 9999 Inbound" -Direction Inbound -Protocol TCP -LocalPort 9999 -Action Allow -Profile Any
}

# B. 强制启用该规则
Enable-NetFirewallRule -DisplayName "Allow XXL-Job 9999 Inbound"

# C. 禁用容易冲突的 Public 阻断规则（仅限 java/IDEA/Node）
Get-NetFirewallRule -Enabled True |
  Where-Object {
    $_.Direction -eq 'Inbound' -and
    $_.Action -eq 'Block' -and
    $_.Profile -match 'Public' -and
    $_.DisplayName -match 'java|Java|IntelliJ|IDEA|Node'
  } |
  Disable-NetFirewallRule
```

> 注释：
> - 这是“实用优先”的修复命令，目标是先保证 XXL-Job 可调度。
> - 若你后续想做“最小权限治理”，再单独细化规则。

---

## 6. 一键回滚到“修复前更严格状态”命令（PowerShell）

> 用途：当你不再需要 Linux 访问 9999，或者需要回退安全策略。

```powershell
# A. 重新启用之前禁用的 Public 阻断规则（java/IDEA/Node）
Get-NetFirewallRule |
  Where-Object {
    $_.Direction -eq 'Inbound' -and
    $_.Action -eq 'Block' -and
    $_.Profile -match 'Public' -and
    $_.DisplayName -match 'java|Java|IntelliJ|IDEA|Node'
  } |
  Enable-NetFirewallRule

# B. 删除 XXL-Job 9999 放行规则（可选）
# 如果你未来还要用 XXL-Job，建议不要删，只需保留。
Remove-NetFirewallRule -DisplayName "Allow XXL-Job 9999 Inbound" -ErrorAction SilentlyContinue
```

---

## 7. Linux 侧验证命令（每次改完都测）

```bash
# 端口探测（推荐先做）
nc -vz 192.168.31.1 9999

# HTTP 连通性测试
curl -i http://192.168.31.1:9999/beat
```

### 如何判断结果

- **成功场景**：
  - `nc` 显示连接成功；
  - `curl` 返回 HTTP 响应（即使 body 是 500 也表示“网络已通”）。

- **失败场景**：
  - `timed out`：通常是防火墙或网络路径不通；
  - `connection refused`：通常是目标机该端口没有进程监听。

---

## 8. 常见误区（避免重复踩坑）

1. **把命令在 cmd 里执行**
   - `New-NetFirewallRule` 等命令必须在 **PowerShell** 执行。

2. **地址填错机器**
   - 执行器地址要填 `trade-service` 实际运行机器的可达 IP。

3. **只看业务接口返回码，不看网络层**
   - 先判断“是否能连上”，再看业务接口是否 200。

---

## 9. 建议长期方案（可选）

- 保留端口放行规则：`Allow XXL-Job 9999 Inbound`。
- 清理重复/历史遗留防火墙规则，避免未来冲突。
- 若网络环境固定，可把规则作用域进一步收敛到指定远端网段，提高安全性。
