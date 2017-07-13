package com.latticeengines.perf.exposed.metric.sink;

import org.apache.commons.configuration.SubsetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

public class SocketSink implements MetricsSink {
    private static final Logger log = LoggerFactory.getLogger(SocketSink.class);
    private static final String SERVER_KEY = "server";

    private SinkServerClient client = null;

    @Override
    public void init(SubsetConfiguration conf) {
        String hostPort = conf.getString(SERVER_KEY);
        String[] hostPortTokens = parseServer(hostPort);
        client = new SinkServerClient(hostPortTokens[0], Integer.parseInt(hostPortTokens[1]));
    }

    public static String[] parseServer(String hostPort) {
        if (hostPort == null) {
            throw new MetricsException("Missing server property");
        }

        String[] hostPortTokens = hostPort.split(":");

        if (hostPortTokens.length != 2) {
            throw new MetricsException("Format must be host:port");
        }

        return hostPortTokens;
    }

    @Override
    public void putMetrics(MetricsRecord record) {
        if (writeToFile()) {
            StringBuilder sb = writeMetrics(record);
            client.sendMetric(sb.toString());
        }
    }

    public StringBuilder writeMetrics(MetricsRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append(record.timestamp());
        sb.append(" ");
        sb.append(record.context());
        sb.append(".");
        sb.append(record.name());
        String separator = ":";
        for (MetricsTag tag : record.tags()) {
            sb.append(separator);
            separator = ",";
            sb.append(tag.name());
            sb.append("=");
            sb.append(tag.value());
        }
        for (AbstractMetric metric : record.metrics()) {
            sb.append(separator);
            separator = ",";
            sb.append(metric.name());
            sb.append("=");
            sb.append(metric.value());
        }
        sb.append("\n");
        return sb;
    }

    public boolean writeToFile() {
        boolean writeToFile = Boolean.parseBoolean(client.canWrite());
        if (writeToFile) {
            log.info("Can write to file.");
        }
        return writeToFile;
    }

    @Override
    public void flush() {
        // NOOP
    }

}
