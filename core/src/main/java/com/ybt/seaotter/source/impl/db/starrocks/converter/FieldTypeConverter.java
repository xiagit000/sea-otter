package com.ybt.seaotter.source.impl.db.starrocks.converter;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ybt.seaotter.common.enums.DataSourceType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class FieldTypeConverter {

    private final Function<String, String> converter;
    private static final Map<String, Function<String, String>> CONVERTERS;

    static {
        Map<String, Function<String, String>> map = new HashMap<>();

        // MySQL 映射表
        map.put(DataSourceType.MYSQL.name(), type -> {
            String mysqlTypeLower = type.toLowerCase();
            if (Lists.newArrayList("date", "datetime", "timestamp").contains(mysqlTypeLower)) {
                return "datetime";
            } else if (mysqlTypeLower.startsWith("enum")) {
                return "varchar(100)";
            } else if (mysqlTypeLower.startsWith("double")) {
                return "DOUBLE";
            } else if (mysqlTypeLower.startsWith("float")) {
                return "FLOAT";
            } else if (mysqlTypeLower.startsWith("decimal")) {
                return "DECIMAL";
            } else {
                return type;
            }
        });

        map.put(DataSourceType.ORACLE.name(), type -> {
            switch (type.toLowerCase()) {
                case "timestamp":
                    return "datetime";
                case "number":
                    return "int";
                default:
                    return type.replace("VARCHAR2", "varchar");
            }
        });

        map.put(DataSourceType.DM.name(), type -> {
            type = type.toLowerCase();
            if (type.startsWith("datetime") || type.startsWith("timestamp")) {
                return "datetime";
            }
            return type;
        });

        CONVERTERS = ImmutableMap.copyOf(map);  // 不可变
    }

    public FieldTypeConverter(String dataSourceName) {
        if (!Strings.isNullOrEmpty(dataSourceName) && CONVERTERS.containsKey(dataSourceName)) {
            this.converter = CONVERTERS.get(dataSourceName);
        } else {
            // 默认策略：原样返回（相当于 Function.identity()）
            this.converter = type -> type;
        }
    }

    public String convert(String fieldType) {
        return converter.apply(fieldType);
    }
}
