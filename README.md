# SeaOtter(海獭)-企业级多源异构数据集成引擎

## 介绍
一个支持多数据源、使用api集成、依赖spark及flink大数据计算平台完成数据流的批量及实时同步的数据引擎，目前支持的功能有：数据库预览、
数据库同步及同步任务回调的功能，支持的数据源包括：Mysql、达梦、oracle、starrocks、ftp/sftp等。

## 环境要求
| 依赖    | 版本号   | 说明                     |
|-------|-------|------------------------|
| JAVA  | 1.8   | 初个版本为了兼容之前老项目，后续会逐步升级  |
| spark | 3.4.3 | 用于历史数据的批量处理            |
| flink | 1.9.1 | 利用其flink-cdc实现数据实时更新同步 |

## 使用示例

### 导入核心依赖
``` xml
<parent>
    <groupId>com.ybt.seaotter</groupId>
    <artifactId>core</artifactId>
    <version>1.0</version>
</parent>
```

### 初始化sdk
```java
SeaOtterConfig seaOtterConfig = SeaOtterConfig.builder()
    .sparkOptions(new SparkOptions("<host>", 6066))
    .flinkOptions(new FlinkOptions("<host>", 8081))
    .callback("<callbackUrl>")
    .build();
    seaOtter = SeaOtter.config(seaOtterConfig);
```

### 定义数据源
```java
private final SourceConnector source = new MysqlConnector()
        .setHost("<host>")
        .setPort(3306)
        .setUsername("<username>")
        .setPassword("<pwd>")
        .setDatabase("<db>")
        .setTable("<table>");

private final SourceConnector sink = new StarrocksConnector()
        .setHost("<host>")
        .setHttpPort(8080)
        .setRpcPort(9030)
        .setUsername("<username>")
        .setPassword("<pwd>")
        .setDatabase("<db>")
        .setTable("<table>");
```

### 执行任务
```java
// 批处理全量同步
seaOtter.job()
    .from(source).to(sink)
    .batchMode(TransmissionMode.OVERWRITE)
    .submit()

// 实时同步
seaOtter.job().from(source).to(sink).CDCMode().submit()
```

### 数据源操作
```java
// 查询所有数据库
List<String> databases = seaOtter.db(source).databases();

// 查询指定数据库下的所有表
List<String> databases = seaOtter.db(source).database("<db>").tables();

...

// 文件系统操作
List<FileObject> files = seaOtter.file(source).list("/", Lists.newArrayList("txt, csv"));

...
```

## 计划支持的数据库
1. [MySQL](https://github.com/antlr/grammars-v4/tree/master/sql/mysql)
2. [PrestoSQL](https://github.com/prestosql/presto/tree/master/presto-parser/src/main/antlr4/io/prestosql/sql/parser)
3. [PostgreSQL](https://github.com/pgcodekeeper/pgcodekeeper/tree/master/apgdiff/antlr-src)
5. [Sql Server](https://github.com/antlr/grammars-v4/tree/master/sql/tsql)
6. [StarRocks](https://github.com/StarRocks/starrocks/tree/main/fe/fe-core/src/main/java/com/starrocks/sql/parser)
7. [Oracle](https://github.com/antlr/grammars-v4/tree/master/sql/plsql)
8. [OceanBase](https://github.com/oceanbase/odc/tree/main/libs/ob-sql-parser)
9. [Flink SQL / Flink CDC SQL](https://github.com/DTStack/dt-sql-parser/tree/main/src/grammar/flinksql)


