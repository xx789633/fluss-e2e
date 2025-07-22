package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.admin.Admin;

public class InsertTest extends PutTest {
    public static final Logger LOG = LoggerFactory.getLogger(InsertTest.class);

    private AtomicLong tic = new AtomicLong(0);
    @Override
    Runnable buildJob(int id) {
        return new InsertJob(id);
    }

    @Override
    void init() throws Exception {
        ConfLoader.load(confName, "insert.", insertTestConf);
    }

    private InsertTestConf insertTestConf = new InsertTestConf();

    class InsertJob implements Runnable {
        final int id;

        public InsertJob(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            Connection conn = null;
            try {
                FlussConfig config = new FlussConfig();
                ConfLoader.load(confName, "flussClient.", config);
                Configuration flussConfig = new Configuration();
                flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), config.getBootstrapServers());
                conn = ConnectionFactory.createConnection(flussConfig);

                Random rand = new Random();
                Table table = conn.getTable(new TablePath("benchmark_db", conf.tableName));
                TableInfo tableInfo = table.getTableInfo();
                UpsertWriter upsertWriter = table.newUpsert().createWriter();
                int i = 0;
                List<String> writeColumns = Util.getWriteColumnsName(conf, tableInfo.getSchema());
                while (true) {
                    long pk = tic.incrementAndGet();
                    ++i;
                    if(conf.testByTime) {
                        if (i % 1000 == 0) {
                            if (System.currentTimeMillis() > targetTime) {
                                LOG.info("test time reached");
                                totalCount.addAndGet(i-1);
                                break;
                            }
                        }
                    } else {
                        if (pk > conf.rowNumber) {
                            LOG.info("insert write : {}", i - 1);
                            totalCount.addAndGet(i-1);
                            break;
                        }
                    }
                    GenericRow put = newPut(pk, tableInfo, rand, writeColumns, insertTestConf.enableRandomPartialColumn);
                    upsertWriter.upsert(put);
                }
                upsertWriter.flush();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    private GenericRow newPut(long id, TableInfo tableInfo, Random random, List<String> writeColumns, boolean enableRandomPartialColumn) {
        GenericRow put = new GenericRow(tableInfo.getSchema().getColumnNames().size());
        fillRecord(put, id, tableInfo, random, writeColumns, enableRandomPartialColumn);
        return put;
    }
}

class InsertTestConf {
    public boolean enableRandomPartialColumn = false;
}