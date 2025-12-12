package com.ybt.seaotter.hologres;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCWriter;
import com.alibaba.hologres.connector.flink.jdbc.copy.HologresJDBCCopyWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.hologres.connector.flink.sink.v1.multi.HoloRecordConverter;
import com.alibaba.hologres.connector.flink.sink.v1.multi.HologresMapOutputFormat;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHologresMultiTableSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction, AutoCloseable {
    private static final transient Logger LOG = LoggerFactory.getLogger(CustomHologresMultiTableSinkFunction.class);
    private static final String TABLE_NAME_KEY;
    private static final String TABLE_OPERATOR_KEY = "__op";
    private final Configuration commonConfig;
    private final Map<TableName, Configuration> tableToConfig;
    private final Map<Tuple2<TableName, Set<String>>, CustomHologresMapOutputFormat> tableToOutputFormat;
    private final Map<TableName, WriteMode> tableToWriteMode;
    private final HoloRecordConverter<T> convert;

    public CustomHologresMultiTableSinkFunction(Configuration commonConfig, HoloRecordConverter<T> convert) {
        this(commonConfig, new HashMap(), convert);
    }

    public CustomHologresMultiTableSinkFunction(Configuration commonConfig, Map<TableName, Configuration> tableToConfig, HoloRecordConverter<T> convert) {
        this.commonConfig = commonConfig;
        this.tableToConfig = tableToConfig;
        this.tableToWriteMode = new HashMap();
        this.tableToOutputFormat = new ConcurrentHashMap();
        this.convert = convert;
    }

    public void initOutputFormat(CustomHologresMapOutputFormat format) throws IOException {
        format.setRuntimeContext(this.getRuntimeContext());
        final RuntimeContext context = this.getRuntimeContext();
        final int indexInSubtaskGroup = context.getTaskInfo().getIndexOfThisSubtask();
        final int currentNumberOfSubtasks = context.getTaskInfo().getNumberOfParallelSubtasks();
        format.open(new OutputFormat.InitializationContext() {
            public int getNumTasks() {
                return currentNumberOfSubtasks;
            }

            public int getTaskNumber() {
                return indexInSubtaskGroup;
            }

            public int getAttemptNumber() {
                return context.getTaskInfo().getAttemptNumber();
            }
        });
    }

    public void invoke(T record) throws Exception {
        Map<String, Object> holoRecord = this.convert.convert(record);
        if (!holoRecord.containsKey(TABLE_OPERATOR_KEY))
            throw new RuntimeException("Operator is not found in record: " + holoRecord);

//        if (!holoRecord.containsKey(TABLE_NAME_KEY)) {
//            throw new RuntimeException("Table name is not found in record: " + holoRecord);
//        }

        TableName tableName = TableName.valueOf(commonConfig.get(HologresConfigs.TABLE));
        Integer operator = Integer.valueOf(holoRecord.get(TABLE_OPERATOR_KEY).toString());
        holoRecord.remove(TABLE_OPERATOR_KEY);
        this.tableToWriteMode.computeIfAbsent(tableName, (name) -> {
            Configuration tableConf = this.commonConfig.clone();
            if (this.tableToConfig.containsKey(name)) {
                tableConf.addAll((Configuration)this.tableToConfig.get(name));
            }

            return (WriteMode)tableConf.get(HologresConfigs.WRITE_MODE);
        });
        WriteMode writeMode = (WriteMode)this.tableToWriteMode.get(tableName);
        Set<String> columnNames;
        if (writeMode.equals(WriteMode.INSERT)) {
            columnNames = Collections.emptySet();
        } else {
            columnNames = holoRecord.keySet();
        }

        CustomHologresMapOutputFormat format = (CustomHologresMapOutputFormat) this.tableToOutputFormat.computeIfAbsent(new Tuple2(tableName, columnNames), (nameColumnsTuple) -> {
            try {
                TableName name = (TableName)nameColumnsTuple.f0;
                Configuration tableConf = this.commonConfig.clone();
                if (this.tableToConfig.containsKey(name)) {
                    tableConf.addAll((Configuration)this.tableToConfig.get(name));
                }

                tableConf.setString(HologresConfigs.TABLE.key(), name.getFullName());
                HologresConnectionParam param = new HologresConnectionParam(tableConf);
                HologresWriter<Map<String, Object>> hologresWriter;
                if (param.getWriteMode().equals(WriteMode.INSERT)) {
                    hologresWriter = HologresJDBCWriter.createMapRecordWriter(param, HologresTableSchema.get(param.getJdbcOptions()));
                } else {
                    hologresWriter = HologresJDBCCopyWriter.createMapRecordWriter(param, HologresTableSchema.get(param.getJdbcOptions()));
                }

                CustomHologresMapOutputFormat outputFormat = new CustomHologresMapOutputFormat(param, hologresWriter);
                this.initOutputFormat(outputFormat);
                return outputFormat;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create output format for table: " + tableName, e);
            }
        });
        if (operator.equals(1)) {
            format.deleteData(holoRecord);
        } else {
            format.writeData(holoRecord);
        }
    }

    public void setRuntimeContext(RuntimeContext context) {
        super.setRuntimeContext(context);
    }

    public void close() throws IOException {
        LOG.info("HologresMultiTableSinkFunction start to closing");
        IOException lastException = null;

        for(Map.Entry<Tuple2<TableName, Set<String>>, CustomHologresMapOutputFormat> entry : this.tableToOutputFormat.entrySet()) {
            try {
                ((CustomHologresMapOutputFormat) entry.getValue()).close();
            } catch (IOException e) {
                LOG.error("Error closing output format for table: " + entry.getKey(), e);
                lastException = e;
            }
        }

        this.tableToOutputFormat.clear();
        if (lastException != null) {
            throw lastException;
        } else {
            LOG.info("HologresMultiTableSinkFunction closed");
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info("HologresMulitTableSinkFunction start to taking snapshotState for checkpoint " + context.getCheckpointId());
        IOException lastException = null;

        for(CustomHologresMapOutputFormat format : this.tableToOutputFormat.values()) {
            try {
                format.flush();
            } catch (IOException e) {
                LOG.error("Error flushing output format", e);
                lastException = e;
            }
        }

        if (lastException != null) {
            throw lastException;
        } else {
            LOG.info("HologresMulitTableSinkFunction finished taking snapshotState for checkpoint " + context.getCheckpointId());
        }
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    static {
        TABLE_NAME_KEY = HologresConfigs.TABLE.key();
    }
}
