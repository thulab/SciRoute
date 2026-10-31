# 不可变payload块的有界跨请求缓存与在途共享

Owner模式默认开启 `storage.version_directory.payload_read_coalescing=true`，可设为`false`禁用全部块共享。`storage.version_directory.payload_cache_bytes` 默认8 MiB，允许0至8 MiB；0只关闭跨请求驻留，保留原请求内／在途共享，适用于消融。严格模式不接受这两项配置。共享身份包含同一个Owner运行时、namespace、incarnation、对象类型、协议、content ID及精确大小；不按文件名、offset或可变head别名复用。每个Owner的总共享上限仍为64个保留条目／8 MiB块数据，包含在途、LRU驻留和已驱逐但仍被消费者／当前请求持有的条目，三者不另加额度。查找表使用weak引用，LRU仅对验证完成的成功块持有可选强引用；过期身份在下次准入时清理。

## 生命周期与多块批次

一次`StructuredBlobReadHandle::read_payload_range`及Owner backend的对应scalar入口仍建立显式请求作用域，覆盖该次原始scalar调用原有的多个8 MiB分片。嵌套adapter复用外层作用域；作用域最多持有64块／8 MiB，结束即释放自己的引用，不挂在prepared `shared_source`上。开启驻留后，独立scalar或`read_ranges`调用可按精确身份命中同一成功块；不缓存整文件、查询答案或未经验证的片段。LRU在准入和驻留压力下先撤销旧块的可选引用。活跃消费者的实际字节和pin仍计费，若无法释放足够容量则真实读取旁路。驻留只接受capacity等于精确块大小的缓冲，避免小逻辑块长期钉住更大的vector容量。

`Owner::clear_read_cache()`同时清除metadata和payload可选驻留；恢复也执行该路径。generation使清除前在途生产者不能重新入驻。清除与Owner销毁均不取消在途读取、不释放活跃消费者的pin；最终引用消失才退还预算，Owner销毁显式撤销LRU强引用以拆除生命周期环。

同一多块批次在短注册表锁下按块身份一次性取得已有条目或领取缺块生产权。调用者先用**一次原生Store batch**读取自己负责的所有缺块及容量不足的旁路块，完成整批数量、长度和digest校验后才发布成功块，再等待其他生产者的块。`AB/BC`因此可以共享B，`AB/BA`不会形成相互等待；不把原生batch拆成逐块SQL，也不引入查询内线程、凑批等待窗口或跨任务拼接SQL。结果始终按各调用者原始顺序复制到私有输出。

LRU驻留或仍被当前批次／请求持有的成功块可以供另一个合法请求复用。这允许跳过该次物理供数；不会重新观察被跳过的瞬态Store错误。force／深度完整性检查不加入也不读取共享条目，仍真实读取底层存储并完整验证。元数据保持原读取路径。

## 版本、预算、取消与错误

每条查询原有的版本／权限准入、源限定逻辑费用、payload demand、I/O预算及临时内存预约照常执行。命中不减少原逻辑需求计费。共享条目持有生产者已有的完整临时内存预约（原物理batch为4倍块字节，可能大于单个块），直到最后消费者释放；8 MiB是块数据上限，不是临时预约上限，实际预约继续计入原运行时预算。无查询预算的底层Owner读取仍受独立块容量上限约束。

运行时通过固定64槽的weak注册表关联可选存储缓存，不持有Owner或Entry。注册满时新生产者放弃可选驻留；已有请求内共享继续有效。初始loader预约、后续temporary及metadata trace预约遇到字节压力时，先在预算锁外回收当前请求和已注册缓存的可选引用，再按原规则检查／淘汰／等待／报错，不增加预算，也不重试已污染状态的失败预约。其他消费者仍使用的字节和pin继续计费；真实不足仍返回原错误。

生产者始终在原线程及其TLS下同步执行Store调用，等待和真实I/O不持有共享注册表锁。等待者每10 ms调用原有`storage_read_checkpoint()`，按自己的上下文检查取消／截止；既有GRIB解码隔离作用域保留其原有边界。取消等待者不影响生产者和其他查询；Owner返回前仍检查ready／冻结状态。无效引用、外来namespace、逻辑预算和force条件在复用前照常检查。

生产者失败或取消时不发布该次物理batch的任何部分验证结果，也不把异常传给其他查询。存活等待者对失败块组成一个自己的首次物理batch；已成功取得的自有块不重读。这个隔离回退在部分重叠的失败场景中可能让一条请求先读取自有缺块，再读取失败共享块，不能声称失败时SQL次数始终不增加。每次实际Store失败仍按当前调用者原错误路径返回；失败结果不缓存，不重试该调用者已经失败的物理读取。

共享元数据分配失败、容量不足等情况在真实I/O前退回原读取；私有读取结果直接移动，避免成功直读后再因可选共享元数据分配失败。共享条目及其预算pin不在注册表锁内销毁，防止资源释放重入死锁。

`Owner::stats()`提供原`payload_coalesce_*`和新增`payload_cache_*`命中／未命中／入驻／淘汰／压力回收计数，以及当前驻留条目和字节gauge。开启同步profile的执行线程还记录`payload_cache.hits/misses/hit_bytes/insertions/evictions/pressure_reclaims`，命中表示查找到LRU持有的条目；极窄的发布唤醒窗口仍可能包含生产者完成等待。没有该TLS的线程不伪造同步profile计数。原Owner `object_reads/hash_bytes`及`payload_map.block_reads/hash_bytes`保留每逻辑请求口径，不能用于推断实际SQL或物理哈希量。实际Store SDK查询和返回字节用`version_store_query_calls/version_store_blob_bytes`；并发profile的daemon-wide增量不能相加，驻留gauge也不能当作累计量相减。

## 物理读取时钟

在进程第一次不可变读取前设置`IOTDBFS_OWNER_READ_TRACE_JSONL=/绝对路径`可启用独立JSONL诊断；默认关闭，最多4096条，不记录SQL、对象标识、内容或凭据。日志失败不改变查询结果。该观测仅用于独立诊断挂载，性能对照关闭。

大型命名空间的启动恢复可能用完记录额度。可同时设置`IOTDBFS_OWNER_READ_TRACE_READY_FILE=/绝对标记路径`，待挂载就绪后创建该普通文件再开始查询。标记出现前不采样时钟、不消耗额度；拒绝符号链接，首次就绪后锁存起录状态。未设置此变量时仍从首次读取开始记录。

- `acquire_start/end_ns`为reader pool获取区间，包含懒连接建立；`connect_start/end_ns`是其嵌套子区间，包含SDK open及USE。
- `execute_start/end_ns`是SDK executeQueryStatement调用，`first_row_ns`为真实next返回非空行的时间。这些是客户端观察，不能当作DataNode内部执行时钟。
- `consume_end_ns`为遍历和visitor完成；`visitor_ns`、`copy_ns`、`digest_ns`、`validation_ns`存在嵌套，不能全部相加。这里记录Store层校验，Owner再次校验不在这个记录内。
- `operation_close_start/end_ns`为操作句柄关闭；`release_start/end_ns`为真实lease归还，失败时包含坏连接销毁。
- `sequence`和`reader_id`用于区分读取与连接；`payload_blocks`标记本次对象是否全为payload块，不暴露对象标识；`success/stage/close_attempts`保留失败阶段及清理证据。

共享等待者不执行Store，因此不会制造一条假SQL记录。连接／execute区间重叠只能证明客户端提交或取回重叠；要判断DataNode算子是否同时计算，还需服务端查询ID与调度证据。
