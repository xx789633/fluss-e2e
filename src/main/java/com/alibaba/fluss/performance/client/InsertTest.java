package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;
import com.alibaba.fluss.client.table.writer.AppendWriter;
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

    private InsertTestConf insertTestConf = new InsertTestConf();

    class InsertJob implements Runnable {
        final int id;

        public InsertJob(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            Configuration config = new Configuration();
            config.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");
            Connection conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();
            try {
                Random rand = new Random();
                TablePath path = new TablePath("benchmark_db", conf.tableName);
                Table table = conn.getTable(path);
                Schema schema = admin.getTableSchema(path).get().getSchema();
                UpsertWriter upsertWriter = null;
                AppendWriter appendWriter = null;
                if (conf.hasPk) {
                    upsertWriter = table.newUpsert().createWriter();
                } else {
                    appendWriter = table.newAppend().createWriter();
                }
                int i = 0;
                List<String> writeColumns = Util.getWriteColumnsName(conf, schema);
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
                    GenericRow row = new GenericRow(schema.getColumnNames().size());
                    fillRecord(row, id, schema, rand, writeColumns, insertTestConf.enableRandomPartialColumn);
                    if (conf.hasPk) {
                        upsertWriter.upsert(row);
                    } else {
                        appendWriter.append(row);
                    }
                }
                if (conf.hasPk) {
                    upsertWriter.flush();
                } else {
                    appendWriter.flush();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
            }
        }
    }
}

class InsertTestConf {
    public boolean enableRandomPartialColumn = false;
}