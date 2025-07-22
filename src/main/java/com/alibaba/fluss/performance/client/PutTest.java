package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampNtz;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fluss.metadata.Schema;

import com.alibaba.fluss.client.ConnectionFactory;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PutTest {
    public static final Logger LOG = LoggerFactory.getLogger(PutTest.class);
    protected PutTestConf conf = new PutTestConf();
    protected long targetTime;
    protected String confName;
    protected AtomicLong totalCount = new AtomicLong(0);

    public void run(String confName) throws Exception {
        LOG.info("confName:{}", confName);
        this.confName = confName;
        ConfLoader.load(confName, "put.", conf);

        FlussConfig config = new FlussConfig();
        ConfLoader.load(confName, "flussClient.", config);
        this.init();
        Configuration flussConfig = new Configuration();
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), config.getBootstrapServers());
        Admin admin = ConnectionFactory.createConnection(flussConfig).getAdmin();
        if (conf.createTableBeforeRun) {
            Util.createTable(admin, conf.partition, conf.tableName, conf.columnCount,
                    conf.bucketCount, conf.additionTsColumn,
                    conf.hasPk, conf.dataColumnType);
        }

        targetTime = System.currentTimeMillis() + conf.testTime;
        Thread[] threads = new Thread[conf.threadSize];
        // start
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(buildJob(i));
            threads[i].start();
        }
        for (int i = 0; i < threads.length; ++i) {
            threads[i].join();
        }
        LOG.info("finished, {} rows has written", totalCount.get());

        if (conf.deleteTableAfterDone) {
            Util.dropTable(admin, conf.tableName);
        }
    }
    abstract Runnable buildJob(int id);

    abstract void init() throws Exception;

    protected void fillRecord(GenericRow record, long pk, TableInfo tableInfo, Random random,
                              List<String> writeColumns) {
        fillRecord(record, pk, tableInfo, random, writeColumns, false);
    }

    protected void fillRecord(GenericRow record, long pk, TableInfo tableInfo, Random random,
                              List<String> writeColumns, boolean enableRandomPartialCol) {
        Schema schema = tableInfo.getSchema();
        for (String columnName : writeColumns) {
            List<String> columns = schema.getColumnNames();
            int columnIndex = columns.indexOf(columnName);
            Schema.Column column = schema.getColumns().get(columnIndex);
            long value = pk;
            if (!schema.getPrimaryKeyColumnNames().contains(columnName) && enableRandomPartialCol) {
                int randNum = random.nextInt(3);
                if (randNum == 0) {
                    continue;
                }
            }

            if (tableInfo.getPartitionKeys().size() == 1 && tableInfo.getPartitionKeys().get(0) == columnName) {
                if (conf.partition) {
                    value = random.nextInt(conf.partitionCount + conf.partitionRatio);
                    if (value > conf.partitionCount) {
                        value = 0;
                    }
                } else {
                    value = 0;
                }
            }
            switch (column.getDataType().getTypeRoot()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    record.setField(columnIndex, (int) value);
                    break;
                case BIGINT:
                    record.setField(columnIndex, value);
                    break;
                case STRING:
                    record.setField(columnIndex, BinaryString.fromString(Util.alignWithColumnSize(value, conf.columnSize)));
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    record.setField(columnIndex, TimestampNtz.fromMillis(System.currentTimeMillis()));
                    break;
                case BINARY:
                    byte[] bytes = new byte[conf.columnSize];
                    random.nextBytes(bytes);
                    record.setField(columnIndex, bytes);
                    break;
                default:
                    throw new RuntimeException("unknown type " + column.getDataType());
            }
        }
    }
}

class PutTestConf {
    public int threadSize = 10;
    public long testTime = 600000;
    public long rowNumber = 1000000;
    public boolean partition = false;
    public int bucketCount = -1;
    public int partitionRatio = 10;
    public int partitionCount = 30;

    public boolean testByTime = true;
    public int writeColumnCount = -1;
    public boolean deleteTableAfterDone = true;
    public boolean additionTsColumn = true;

    public String tableName = "fluss_perf";
    public int columnCount = 100;
    public int columnSize = 10;
    public boolean hasPk = true;
    public String dataColumnType = "text";
    public boolean createTableBeforeRun = true;
}