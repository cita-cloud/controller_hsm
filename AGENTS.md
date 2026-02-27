# CITA-Cloud Controller

## 基本信息
- **名称**: controller
- **版本**: 6.7.5
- **作者**: Rivtower Technologies
- **许可证**: Apache-2.0
- **Rust 版本**: Edition 2024

## 项目简介
Controller 是 CITA-Cloud 区块链架构中的核心控制微服务。它充当各个微服务模块（共识、网络、存储、执行）之间的协调者，负责维护本地区块链状态、交易池管理、区块同步以及节点间状态的广播与更新。

## 核心能力
1.  **状态机管理**
    - 使用 `statig` 库实现状态机，管理节点状态（如 Offline, Online, Syncing, ParticipateInConsensus 等）。
    - 协调节点在不同阶段的行为，确保系统状态的一致性。

2.  **交易池管理 (Mempool)**
    - 接收并验证交易，维护待打包的交易列表。
    - 支持交易配额（Quota）限制和去重。

3.  **区块同步 (Block Sync)**
    - 监控全局链高度，自动触发区块同步流程。
    - 处理 `SyncBlockReq` 和 `SyncBlockRespond` 消息，从对等节点获取缺失区块。
    - `SyncManager` 负责管理同步请求和区块数据的重组。

4.  **节点管理 (Node Manager)**
    - 维护对等节点的状态（高度、Hash 等）。
    - 识别并处理恶意节点（Misbehavior Node），维护黑名单。

5.  **微服务交互 (gRPC)**
    - **Server 端**: 提供 Consensus, Network, RPC, HealthCheck 服务接口。
    - **Client 端**: 调用 Consensus, Executor, Network, Storage 服务接口。
    - 实现了 CITA-Cloud 标准协议 (`cita_cloud_proto`)。

6.  **系统配置与治理**
    - 管理系统级配置（如管理员、验证人列表、区块间隔等）。
    - 支持通过系统交易更新链上配置。

## 运行依赖
### 编译环境
- Rust (最新稳定版，支持 Edition 2024)
- Protobuf Compiler (`protoc`)

### 运行时依赖
Controller 作为 CITA-Cloud 的一部分，通常需要与其他微服务配合运行：
- **Network Microservice**: 负责 P2P 网络通信。
- **Consensus Microservice**: 负责共识算法（如 Raft, BFT）。
- **Storage Microservice**: 负责底层数据存储。
- **Executor Microservice**: 负责交易执行（EVM, WASM 等）。

## 使用示例

### 1. 编译项目
```bash
cargo build --release
```

### 2. 查看帮助信息
```bash
./target/release/controller --help
```

### 3. 运行服务
Controller 需要配置文件和私钥文件来启动。

```bash
./target/release/controller run \
    --config config.toml \
    --private_key_path private_key
```

#### 命令行参数说明
- `run`: 启动服务的子命令。
- `-c, --config <FILE>`: 指定配置文件路径（默认为 `config.toml`）。
- `-p, --private_key_path <FILE>`: 指定节点私钥文件路径（默认为 `private_key`）。

### 4. 配置文件示例 (config.toml)
```toml
# 监听端口
network_port = 50004
# 域名
domain = "controller"

# 日志配置
[log_config]
max_level = "info"
filter = "info"
service_name = "controller"
rolling_file_path = "logs"
agent_endpoint = "127.0.0.1:6831"

# 其他微服务地址
[network_client]
port = 50000
host = "127.0.0.1"

[consensus_client]
port = 50001
host = "127.0.0.1"

[executor_client]
port = 50002
host = "127.0.0.1"

[storage_client]
port = 50003
host = "127.0.0.1"
```
