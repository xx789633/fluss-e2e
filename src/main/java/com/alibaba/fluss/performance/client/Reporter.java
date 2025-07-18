package com.alibaba.fluss.performance.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class Reporter {
    public static Logger LOG = LoggerFactory.getLogger(Reporter.class);
    private String confName;
    private File dir;

    public Reporter(String confName) {
        this.confName = confName;
        this.dir = new File(confName).getParentFile();
    }

    long start = 0L;

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void report(long count, double qps1, double qps5, double qps15, double latencyMean, double latencyP99,
                       double latencyP999, long memoryUsage) {
        File file = new File(dir, "result.csv");
        try (BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file)))) {
            bw.write("start,end,count,qps1,qps5,qps15,latencyMean,latencyP99,latencyP999,memoryUsage,version\n");
            bw.write(String.valueOf(start));
            bw.write(',');
            bw.write(String.valueOf(System.currentTimeMillis()));
            bw.write(',');
            bw.write(String.valueOf(count));
            bw.write(',');
            bw.write(String.valueOf(qps1));
            bw.write(',');
            bw.write(String.valueOf(qps5));
            bw.write(',');
            bw.write(String.valueOf(qps15));
            bw.write(',');
            bw.write(String.valueOf(latencyMean));
            bw.write(',');
            bw.write(String.valueOf(latencyP99));
            bw.write(',');
            bw.write(String.valueOf(latencyP999));
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}