package com.ybt.seaotter;

import com.alibaba.fastjson.JSON;
import com.github.melin.superior.sql.parser.mysql.MySqlHelper;
import io.github.melin.superior.common.relational.create.CreateTable;
import io.github.melin.superior.parser.mysql.antlr4.MySqlParser;
import io.github.melin.superior.parser.starrocks.StarRocksHelper;

public class SqlTest {
    public static void main(String[] args) {
        String mysql_sql = "CREATE TABLE `sync_task` (\n" +
                "  `id` int NOT NULL AUTO_INCREMENT,\n" +
                "  `task_no` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" +
                "  `state` int NOT NULL COMMENT '0：未执行；1: 执行中；2：成功；-1：失败',\n" +
                "  `type` int DEFAULT NULL COMMENT '任务类型：1：全量；2：增量；',\n" +
                "  `start_time` datetime DEFAULT NULL COMMENT '增量同步的范围起始时间',\n" +
                "  `create_time` datetime DEFAULT NULL,\n" +
                "  `update_time` datetime DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=60 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;";
        CreateTable statement = (CreateTable) MySqlHelper.parseStatement(mysql_sql);
        System.out.println(JSON.toJSONString(statement.getColumnRels()));
    }
}
