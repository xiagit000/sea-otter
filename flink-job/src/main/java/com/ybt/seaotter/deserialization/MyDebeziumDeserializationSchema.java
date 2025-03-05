package com.ybt.seaotter.deserialization;

import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.utils.TemporalConversions;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;

import java.util.List;

public class MyDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    public MyDebeziumDeserializationSchema() {
    }

    public MyDebeziumDeserializationSchema(String database) {
        this.database = database;
    }

    private String database;
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        String topic = sourceRecord.topic();
        String[] topicString = topic.split("\\.");
        String db = topic.split("\\.")[1];
        String tableName = topic.split("\\.")[topicString.length - 1];
        Struct value = (Struct)sourceRecord.value();
        Struct current = null;
        String operator = value.getString("op");
        if (Envelope.Operation.READ.code().equals(operator) || Envelope.Operation.CREATE.code().equals(operator) || Envelope.Operation.UPDATE
                .code().equals(operator)) {
            current = value.getStruct("after");
        } else if (Envelope.Operation.DELETE.code().equals(operator)) {
            current = value.getStruct("before");
        }
        JSONObject data = new JSONObject();
        Struct keyStruck = (Struct)sourceRecord.key();
        StringBuilder pk = new StringBuilder();
        List<Field> fields = keyStruck.schema().fields();
        for (Field field : fields) {
            pk.append(keyStruck.get(field));
        }
        data.put("pk", pk.toString());
        if (!StringUtils.isEmpty(this.database)) {
            data.put("database", this.database);
        } else {
            data.put("database", db);
        }
        data.put("table_name", tableName);
        JSONObject record = new JSONObject();
        record.put("__op", Integer.valueOf(Envelope.Operation.DELETE.code().equals(operator) ? 1 : 0));
        for (Field field : current.schema().fields()) {
            long micro, nano;
            String name = (null == field.schema().name()) ? "" : field.schema().name();
            switch (name) {
                case "io.debezium.time.Timestamp":
                    record.put(field.name(), (current.getInt64(field.name()) == null) ? "" : TimestampData.fromEpochMillis(current.getInt64(field.name()).longValue()).toString());
                    continue;
                case "io.debezium.time.MicroTimestamp":
                    micro = current.getInt64("field").longValue();
                    record.put(field.name(), TimestampData.fromEpochMillis(micro / 1000L, (int)(micro % 1000L * 1000L)).toString());
                    continue;
                case "io.debezium.time.NanoTimestamp":
                    nano = current.getInt64("field").longValue();
                    record.put(field.name(), TimestampData.fromEpochMillis(nano / 1000000L, (int)(nano % 1000000L)).toString());
                    continue;
                case "io.debezium.time.Date":
                    record.put(field.name(), (current.getInt32(field.name()) == null) ? "" : TemporalConversions.toLocalDate(current.getInt32(field.name())).toString());
                    continue;
            }
            Object column = current.get(field);
            record.put(field.name(), column);
        }
        String companyId = record.getString("company_id");
        data.put("record", record);
        data.put("company_id", companyId);
        collector.collect(JSONObject.toJSONString(data));
    }

}