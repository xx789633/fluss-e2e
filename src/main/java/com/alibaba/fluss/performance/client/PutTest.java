package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.row.GenericRow;
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

        Configuration flussConfig = new Configuration();
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), config.getBootstrapServers());

        Connection conn = ConnectionFactory.createConnection(flussConfig);
        Admin admin = conn.getAdmin();
        Util.createTable(admin, conf.partition, conf.tableName, conf.columnCount,
                conf.bucketCount, conf.additionTsColumn,
                conf.hasPk, conf.dataColumnType);

        targetTime = System.currentTimeMillis() + conf.testTime;
        Thread[] threads = new Thread[conf.threadSize];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(buildJob(i));
            threads[i].start();
        }
        for (int i = 0; i < threads.length; ++i) {
            threads[i].join();
        }
        LOG.info("finished, {} rows has written", totalCount.get());

        Util.dropTable(admin, conf.tableName);
    }
    abstract Runnable buildJob(int id);

    protected void fillRecord(GenericRow record, long pk, Schema schema, Random random,
                              List<String> writeColumns, boolean enableRandomPartialCol) {
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
            switch (column.getDataType().getTypeRoot()) {
                case DOUBLE:
                    record.setField(columnIndex, value);
                case INTEGER:
                case SMALLINT:
                case BIGINT:
                    record.setField(columnIndex, value);
                case STRING:
                    record.setField(columnIndex, String.valueOf(value));
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
    public int bucketCount = 3;

    public boolean testByTime = true;
    public int writeColumnCount = -1;
    public boolean additionTsColumn = true;

    public String tableName = "fluss_perf";
    public int columnCount = 100;
    public int columnSize = 10;
    public boolean hasPk = true;
    public String dataColumnType = "text";
}