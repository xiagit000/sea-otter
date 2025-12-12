<div align="center">
         <a href="https://e.gitee.com/vibot/repos/vibot/sea-otter" target="_blank" rel="noopener noreferrer">
           <img src="logo.svg" width="20%" height="20%" alt="sea-otter Logo" />
        </a>
 <h1>SeaOtter</h1>
 <h3>海獭--企业级多源异构数据集成引擎</h3>
</div>

<p align="center">
   <img src="version.svg">
   <img src="license.svg">
  </a>
</p>

## 介绍

一个支持多数据源、使用api集成、依赖spark及flink大数据计算平台完成数据流的批量及实时同步的数据引擎，目前支持的功能有：数据库预览、
数据库同步及同步任务回调的功能，支持的数据源包括：Mysql、达梦、oracle、starrocks、ftp/sftp等。

## 核心功能

1. 多数据源支持：支持Mysql、达梦、oracle、starrocks、ftp/sftp等多种数据源的同步。
2. 依赖spark及flink大数据计算平台：SeaOtter基于spark及flink大数据计算平台实现数据同步，支持大数据平台的高并发、高性能。
3. 同步任务回调：SeaOtter支持同步任务的回调，用户可以实时获取同步任务的状态及结果。
4. 数据库预览：SeaOtter支持对数据库的预览，用户可以查看数据库的表结构、字段信息、数据量等。
5. 数据库同步：SeaOtter支持对数据库的同步，用户可以对数据库的表结构、字段信息、数据量等进行同步。
6. 实时同步：SeaOtter支持实时同步，用户可以实时获取数据库的实时更新。
7. 自定义映射：SeaOtter支持自定义映射，用户可以自定义数据源到数据集成的映射关系。
8. 数据质量监控：SeaOtter支持数据质量监控，用户可以对数据源的同步质量进行监控。

## 环境要求

| 依赖                        | 版本号       | 说明                     |
|---------------------------|-----------|------------------------|
| java                      | 1.8       | 初个版本为了兼容之前老项目，后续会逐步升级  |
| spark                     | 3.4.3     | 用于历史数据的批量处理            |
| starrocks-spark-connector | 3.4._2.12 | spark同步starrocks依赖     |
| flink                     | 1.9.1     | 实时数据流处理基础依赖            |
| flink-cdc                 | 3.3.0     | 利用其flink-cdc实现数据实时更新同步 |
| flink-connector-starrocks | 1.2.10    | flink同步starrocks的依赖    |
| jsch                      | 0.1.55    | 处理SFTP文件协议             |
| antlr                     | 4.9.3     | 数据库sql解析与自定义映射         |
| common-nets               | 3.8.0     | 处理FTP文件协议              |
| mysql-connector-java      | 8.0.28    | mysql连接驱动              |
| ojdbc                     | 21.1.0.0  | oracle连接驱动             |
| DmJdbcDeriver             | 8.1.3.140 | 达梦数据库连接驱动              |

## 快速开始

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
    .from(source)
    .to(sink)
    .batchMode(TransmissionMode.OVERWRITE)
    .submit()

// 实时同步
seaOtter.job()
    .from(source)
    .to(sink)
    .CDCMode()
    .submit()
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

## 发布
在本地构建打包spark和flink的任务jar包，然后进行上传操作。
- spark: 可根据实际情况上传到共享目录中，各work会自动拉取
- flink: 则可以通过web界面手动上传jar包

## 计划支持的数据库

1. [MySQL](https://github.com/antlr/grammars-v4/tree/master/sql/mysql)
2. [PrestoSQL](https://github.com/prestosql/presto/tree/master/presto-parser/src/main/antlr4/io/prestosql/sql/parser)
3. [PostgreSQL](https://github.com/pgcodekeeper/pgcodekeeper/tree/master/apgdiff/antlr-src)
5. [Sql Server](https://github.com/antlr/grammars-v4/tree/master/sql/tsql)
6. [StarRocks](https://github.com/StarRocks/starrocks/tree/main/fe/fe-core/src/main/java/com/starrocks/sql/parser)
7. [Oracle](https://github.com/antlr/grammars-v4/tree/master/sql/plsql)
8. [OceanBase](https://github.com/oceanbase/odc/tree/main/libs/ob-sql-parser)
9. [Flink SQL / Flink CDC SQL](https://github.com/DTStack/dt-sql-parser/tree/main/src/grammar/flinksql)

## 问题反馈
在使用上有遇到 bug 或者优化点，强烈建议你提 issue 我们将及时修复


