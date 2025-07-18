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
import com.alibaba.fluss.config.Configuration;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.alibaba.fluss.client.Connection;

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

    public static void createTable(Connection conn, boolean partition, String tableName,
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
        Admin admin = conn.getAdmin();
        admin.createTable(TablePath.of("benchmark_db", tableName), descriptor, false).get();
    }

    public static Configuration loadConfiguration(final String confName) {
        File yamlConfigFile = new File(confName);
        return loadYAMLResource(yamlConfigFile);
    }

    private static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": Line is not a key-value pair (missing space after ':'?)");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": Key or value was empty");
                        continue;
                    }

                    config.setString(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }
}
