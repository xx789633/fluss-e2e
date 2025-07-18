package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.client.admin.Admin;
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
        for (int i = 0; i < columnCount; ++i) {
            if (dataColumnType == "text") {
                schemaBuilder.column("name" + i, DataTypes.STRING());
            } else if (dataColumnType == "int") {
                schemaBuilder.column("name" + i, DataTypes.INT());
            }
        }
        if (additionTsColumn) {
            schemaBuilder.column("ts", DataTypes.TIMESTAMP_LTZ());
        }
        if (partition) {
            schemaBuilder.column("ds", DataTypes.INT());
        }
        if (hasPk) {
            List<String> primaryKeys = new ArrayList<>();
            primaryKeys.add("id");
            if (partition) {
                primaryKeys.add("ds");
            }
            schemaBuilder.primaryKey(primaryKeys);
        }
        TableDescriptor.Builder tableBuilder = TableDescriptor.builder();
        if (partition) {
            tableBuilder.partitionedBy("ds");
        }
        TableDescriptor descriptor = tableBuilder
                        .schema(schemaBuilder.build())
                        .distributedBy(bucketCount)
                        .build();
        admin.createTable(TablePath.of("benchmark_db", tableName), descriptor, false).get();
    }

    public static void dropTable(Admin admin, String tableName) throws ExecutionException, InterruptedException {
        admin.dropTable(new TablePath("benchmark_db", tableName), true).get();
    }
}
