package com.alibaba.fluss.performance.client;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.performace.params.ParamsProvider;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;

public class GetTest {
    public static final Logger LOG = LoggerFactory.getLogger(PutTest.class);
    public static final String METRICS_GET_PERF_RPS = "get_perf_rps";
    public static final String METRICS_GET_PERF_LATENCY = "get_perf_latency";

    private String confName;
    private long targetTime;
    GetTestConf conf = new GetTestConf();
    ParamsProvider provider;

    public void run(String confName) throws Exception {
        LOG.info("confName:{}", confName);
        this.confName = confName;
        ConfLoader.load(confName, "get.", conf);
        provider = new ParamsProvider(conf.keyRangeParams);
        FlussConfig config = new FlussConfig();
        ConfLoader.load(confName, "flussClient.", config);
        Configuration flussConfig = new Configuration();
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), config.getBootstrapServers());
        Admin admin = ConnectionFactory.createConnection(flussConfig).getAdmin();

        Schema schema =  admin.getTableSchema(new TablePath("benchmark_db", conf.tableName)).get().getSchema();
        if (schema.getPrimaryKeyColumnNames().size() != provider.size()) {
            throw new Exception(
                    "table has " + schema.getPrimaryKeyColumnNames().size() + " pk columns, but test.params only has "
                            + provider.size() + " columns");
        }
        targetTime = System.currentTimeMillis() + conf.testTime;
        Thread[] threads = new Thread[conf.threadSize];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new Job(i));
            threads[i].start();
        }

        for (int i = 0; i < threads.length; ++i) {
            threads[i].join();
        }

        if (conf.deleteTableAfterDone) {
            Util.dropTable(admin, conf.tableName);
        }
    }

    class Job implements Runnable {
        int id;

        public Job(int id) {
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

                Table table = conn.getTable(new TablePath("benchmark_db", conf.tableName));
                Schema schema = table.getTableInfo().getSchema();
                Lookuper lookuper = table.newLookup().createLookuper();

                int i = 0;
                CompletableFuture<LookupResult> future = null;
                while (true) {
                    if (++i % 1000 == 0) {
                        if (System.currentTimeMillis() > targetTime) {
                            break;
                        }
                    }
                    GenericRow get = new GenericRow(schema.getPrimaryKeyIndexes().length);
                    for (int j = 0; j < schema.getPrimaryKeyIndexes().length; ++j) {
                        get.setField(j, provider.get(j));
                    }
                    future = lookuper.lookup(get);
                    if (conf.async) {
                        future = future.thenApply(r -> {
                            return r;
                        });
                    } else {
                        future.get();
                    }
                }
                if (conf.async && future != null) {
                    future.get();
                }
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
}

class GetTestConf {
    public int threadSize = 10;
    public long testTime = 600000;
    public String tableName = "fluss_perf";
    public String keyRangeParams;
    public boolean async = true;
    public boolean deleteTableAfterDone = false;
}