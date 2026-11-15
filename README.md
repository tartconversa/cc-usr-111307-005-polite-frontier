# 多站点目录爬取的准入与公平调度

项目保存供方接口协议、演练数据与回环地址服务。所有示例记录均使用本机演练资源，不连接实际供应平台。

运行环境为 Python 3.10 或以上，仅使用标准库。

```sh
python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m src.source --port 8765
```

前两条命令分别检查供方源协议行为及语法；第三条启动本机服务，使用 Ctrl+C 结束。

GET /robots.txt 提供规则，GET / 为种子页面，GET /busy 可返回429，GET /jump返回可配置跳转。POST /__control可设置busy、robots_status、retry_after、location。同一服务也可通过localhost访问，与127.0.0.1是两个不同origin；启动第二个服务实例也可构成独立站点。

GET /__state 可查看当前演练状态与已收到的请求。POST /__control 接受JSON，修改该服务实例的演练状态；此入口只用于本机供方模拟，不属于生产接口。服务端状态不跨进程保留。

src/source.py 是供方源实现；fixtures/source.json 保存协议示例数据；tests/test_source.py 检查供方源自身的响应。

## 爬取调度器

`src/scheduler.py` 是多站点爬取调度器，解决"一个站连续 429 占住所有工作线程"的问题：退避是调度器状态而不是线程睡眠，一个站退避只暂停该 origin，其他站继续采集。

- **准入与公平**：每个 origin 独立的最大并发、最小起始间隔与请求预算（含 robots.txt 请求）；按站点轮转派发，站点内先进先出，被门控的站点让出本轮，不会饿死小站点。
- **URL**：`src/urlnorm.py` 去片段、解析相对路径与点段、主机小写、统一默认端口；判重时仅删除配置列出的跟踪参数，其余参数值与重复参数顺序原样保留（签名 URL 不被排序重写）。规范化只用于判重，请求仍发原始 URL。
- **robots.txt**：按配置的 User-Agent 获取；支持 Allow/Disallow、`*`、结尾 `$`、最长匹配、等长 Allow 胜；404 视为无规则，5xx/网络失败暂缓该站。缓存过期且刷新未完成前不派发该站新请求；每次派发记录准入规则版本。
- **重定向**：不自动跟随，每一跳作为新任务重新过 robots/预算/判重准入；环路跳回已访问键或跳数超预算即终止；已访问标记按 origin 隔离，不同 origin 不因同一路径互相影响。
- **退避**：429/503 的 Retry-After 支持秒数与 HTTP 日期，只暂停该 origin。
- **队列与恢复**：任务、租约、结果与事件持久化在 SQLite（`src/store.py`）。认领即租约（fencing token），进程中断后重开数据库自动把未完成任务退回队列；过期租约被回收后，迟到的旧 worker 不能释放别人的槽位，也不能覆盖较新的请求结果。请求预算与 robots 缓存同样持久化，恢复后不重置。
- **可解释**：`explain(url)` 给出每个 URL 为什么被访问、等待或拒绝；事件日志保留派发时间、准入规则版本、跳转链与最终处理原因。

`src/runner.py` 是驱动调度器的工作池（worker 只执行已认领的请求），`python -m src.runner` 运行两站演示：A 站 429 退避后恢复，B 站持续采集。`src/clock.py` 提供可推进的手工时钟，测试用它复现退避场景，并从事件日志回放校验任何时刻不超出站点并发与间隔约束，不依赖休眠长短。
