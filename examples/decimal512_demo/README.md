# Decimal512 测试 Demo

这是一个用于验证 ClickHouse Decimal512 功能的独立测试程序。

## 前置条件

1. **ClickHouse 版本**: 24.8+ （支持 Decimal512）
2. **Go 版本**: 1.21+
3. **ClickHouse 服务**: 本地或远程可访问的 ClickHouse 实例

## 快速开始

### 1. 配置连接信息

编辑 `main.go` 文件，修改连接参数：

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{"127.0.0.1:9000"}, // 修改为你的 ClickHouse 地址
    Auth: clickhouse.Auth{
        Database: "default", // 修改为你的数据库
        Username: "default", // 修改为你的用户名
        Password: "",        // 修改为你的密码
    },
    // ... 其他配置
})
```

### 2. 运行测试

```bash
cd /root/clickhouse-go/examples/decimal512_demo
go run main.go
```

## 测试内容

### 基础功能测试

1. **连接测试**: 验证与 ClickHouse 的连接
2. **版本检查**: 确认 ClickHouse 版本
3. **表创建**: 创建包含多种精度 Decimal512 列的测试表
4. **数据插入**: 插入不同精度的测试数据（77-154 位）
5. **数据查询**: 读取并验证数据完整性
6. **负数测试**: 验证负数的正确处理

### 复杂类型测试

- **Nullable(Decimal512)**: 测试 NULL 值处理
- **Array(Decimal512)**: 测试数组类型
- **Map(String, Decimal512)**: 测试 Map 类型

## 预期输出

```
✅ 连接成功！
📊 ClickHouse 版本: 24.8.x

🔨 创建测试表: test_decimal512_demo
✅ 表创建成功

📝 插入测试数据...
✅ 成功插入 3 条数据

🔍 查询数据并验证...

======================================================================================================================================================
ID    | 名称                   | 小精度(80位)                          | 中精度(120位)                                                      | 大精度(154位)
======================================================================================================================================================
1     | 小精度测试              | 12345678901234567890.1234567890      | 11111111111111111111111111111111111111111111111111.11111...      | 12345678901234567890123456789012345678901234567890123456...
2     | 中精度测试              | 98765432109876543210.9876543210      | 22222222222222222222222222222222222222222222222222.22222...      | 99999999999999999999999999999999999999999999999999999999...
3     | 负数测试               | -55555555555555555555.5555555555     | -33333333333333333333333333333333333333333333333333.3333...      | -77777777777777777777777777777777777777777777777777777777...
======================================================================================================================================================

✅ 成功读取 3 条数据

🧪 测试复杂类型支持...

  复杂类型测试结果:
  - ID 1: Nullable=有值, Array长度=2, Map键数=2
  - ID 2: Nullable=NULL, Array长度=1, Map键数=1
  ✅ 复杂类型测试通过

🧹 清理测试表...
✅ 清理完成

🎉 所有测试完成！Decimal512 功能正常工作！
```

## 测试的精度范围

| 列名 | 精度 | Scale | 示例值长度 |
|------|------|-------|-----------|
| amount_small | Decimal(80, 10) | 10 | 70 位整数 + 10 位小数 |
| amount_medium | Decimal(120, 25) | 25 | 95 位整数 + 25 位小数 |
| amount_large | Decimal(154, 50) | 50 | 104 位整数 + 50 位小数 |

## 常见问题

### 1. 连接失败

**错误**: `连接失败: dial tcp 127.0.0.1:9000: connect: connection refused`

**解决**:
- 检查 ClickHouse 服务是否运行
- 确认端口号正确（Native 协议默认 9000）
- 检查防火墙设置

### 2. 版本不支持

**错误**: 表创建失败或精度超出范围

**解决**:
- 确保 ClickHouse 版本 >= 24.8
- 运行 `clickhouse-client --version` 检查版本

### 3. 权限错误

**错误**: `Code: 497. DB::Exception: default: Authentication failed`

**解决**:
- 检查用户名和密码
- 确保用户有创建表的权限
- 如果使用非 default 数据库，确保数据库存在

### 4. 调试模式

如果遇到问题，开启调试模式查看详细日志：

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    // ...
    Debug: true,  // 开启调试
    Debugf: func(format string, v ...any) {
        fmt.Printf("[DEBUG] "+format+"\n", v...)
    },
})
```

## 自定义测试

### 修改测试数据

编辑 `testData` 变量，添加你自己的测试数据：

```go
testData := []struct {
    id          uint32
    name        string
    amountSmall string
    amountMed   string
    amountLarge string
}{
    {
        id:          1,
        name:        "你的测试名称",
        amountSmall: "你的数值...",
        amountMed:   "你的数值...",
        amountLarge: "你的数值...",
    },
    // ... 更多测试数据
}
```

### 修改表结构

根据你的需求修改 DDL：

```go
createSQL := fmt.Sprintf(`
    CREATE TABLE %s (
        id UInt32,
        name String,
        your_column Decimal(100, 30),  // 自定义列
        // ... 更多列
    ) ENGINE = MergeTree()
    ORDER BY id
`, tableName)
```

## 性能测试

如果需要测试大批量数据性能，可以修改代码：

```go
// 插入 10000 条数据
for i := 0; i < 10000; i++ {
    batch.Append(
        uint32(i),
        fmt.Sprintf("test-%d", i),
        decimal.RequireFromString("123456789.123456789"),
        // ...
    )
}
```

## 技术支持

如果遇到问题：

1. 检查 ClickHouse 日志: `/var/log/clickhouse-server/clickhouse-server.log`
2. 查看项目文档: `/root/clickhouse-go/docs/`
3. 参考验证报告: `/root/clickhouse-go/docs/2025_1026_03_decimal512_verification.md`

## 相关链接

- [ClickHouse Decimal512 文档](https://clickhouse.com/docs/en/sql-reference/data-types/decimal)
- [shopspring/decimal 文档](https://github.com/shopspring/decimal)
- [clickhouse-go 文档](https://github.com/ClickHouse/clickhouse-go)
