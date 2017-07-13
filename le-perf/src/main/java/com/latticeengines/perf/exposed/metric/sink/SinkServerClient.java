package com.latticeengines.perf.exposed.metric.sink;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.metrics2.MetricsException;

class SinkServerClient implements SinkOperations {
    private static final Logger log = LoggerFactory.getLogger(SinkServerClient.class);

    private String host;
    private int port;

    public SinkServerClient(String host, int port) {
        try {
            this.host = host;
            this.port = port;
        } catch (Exception e) {
            throw new MetricsException(e);
        }
    }

    private String writeThenRead(String output) {
        try (Socket clientSocket = new Socket(host, port)) {
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            outToServer.writeBytes(output + "\n");
            outToServer.flush();
            String retval = inFromServer.readLine();
            log.info("FROM SERVER: " + retval);

            return retval;
        } catch (IOException e) {
            throw new MetricsException(e);
        }
    }

    @Override
    public String canWrite() {
        return writeThenRead("CANWRITE");
    }

    @Override
    public void sendMetric(String metric) {
        writeThenRead("SENDMETRIC " + metric);
    }

}
