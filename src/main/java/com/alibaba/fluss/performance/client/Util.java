package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Util {
    public static Logger LOG = LoggerFactory.getLogger(Util.class);

    public static final String BENCHMARK_DB = "benchmark_db";

    public static List<String> getWriteColumnsName(PutTestConf conf, Schema schema) {
        List<String> columns = new ArrayList<>();
        if (conf.writeColumnCount > 0) {
            int columnCount = 0;
            for (int i = 0; i < schema.getColumnNames().size() && columnCount < conf.writeColumnCount; i++) {
                String columnName = schema.getColumnNames().get(i);
                columns.add(columnName);
                if (!schema.getPrimaryKeyColumnNames().contains(columnName) && columnName != "ts") {
                    columnCount++;
                }
            }
            if (conf.additionTsColumn) {
                columns.add("ts");
            }
        } else {
            columns.addAll(schema.getColumnNames());
        }
        return columns;
    }

    public static void createTable(Admin admin, boolean partition, String tableName,
                                   int columnCount, int bucketCount,
                                   boolean additionTsColumn, boolean hasPk, String dataColumnType) throws ExecutionException, InterruptedException {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.INT());
        for (int i = 0; i < columnCount; ++i) {
            if (dataColumnType == "text") {
                schemaBuilder.column("name" + i, DataTypes.STRING());
            } else if (dataColumnType == "int") {
                schemaBuilder.column("name" + i, DataTypes.INT());
            }
        }
        if (additionTsColumn) {
            schemaBuilder.column("ts", DataTypes.TIMESTAMP());
        }
        if (partition) {
            schemaBuilder.column("ds", DataTypes.INT());
        }
        List<String> primaryKeys = new ArrayList<>();
        primaryKeys.add("id");
        if (partition) {
            primaryKeys.add("ds");
        }
        schemaBuilder.primaryKey(primaryKeys);
        TableDescriptor.Builder tableBuilder = TableDescriptor.builder();
        if (partition) {
            tableBuilder.partitionedBy("ds");
        }
        if (bucketCount > 0) {
            tableBuilder.distributedBy(bucketCount, "id");
        } else {
            tableBuilder.distributedBy(null, Collections.singletonList("id"));
        }
        TableDescriptor descriptor = tableBuilder
                        .schema(schemaBuilder.build())
                        .build();
        admin.createDatabase(BENCHMARK_DB, DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(TablePath.of(BENCHMARK_DB, tableName), descriptor, true).get();
    }

    public static void dropTable(Admin admin, String tableName) throws ExecutionException, InterruptedException {
        admin.dropTable(new TablePath(BENCHMARK_DB, tableName), true).get();
        admin.dropDatabase(BENCHMARK_DB, false, true);
    }

    public static String alignWithColumnSize(long value, int columnSize) {
        String val = String.valueOf(value);
        int len = val.length();
        if (len < columnSize) {
            int deltaLen = columnSize - len;
            StringBuilder sb = new StringBuilder();
            while (deltaLen-- > 0) {
                sb.append('0');
            }
            sb.append(val);
            return sb.toString();
        } else if (len > columnSize) {
            return val.substring(0, columnSize);
        } else {
            return val;
        }
    }
}
