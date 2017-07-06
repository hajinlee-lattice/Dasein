package com.latticeengines.perf.exposed.metric.sink;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsException;

public class SinkCollectionServer implements Runnable, SinkOperations {
    private static final Log log = LogFactory.getLog(SinkCollectionServer.class);
    private int port;
    private boolean canWrite;
    private Map<String, Method> methodMap = new HashMap<String, Method>();
    private BufferedOutputStream metricsOutputStream;
    private int numMetrics = 1;

    public SinkCollectionServer(String metricFileName, int port) {
        this.port = port;
        try {
            File metricFile = new File(metricFileName);
            if (!metricFile.exists()) {
                metricFile.createNewFile();
            }
            metricsOutputStream = new BufferedOutputStream(new FileOutputStream(metricFile));
        } catch (Exception e) {
            throw new MetricsException(e);
        }
        init();
    }

    private void init() {
        Class<SinkOperations> c = SinkOperations.class;

        Method[] methods = c.getMethods();

        for (Method method : methods) {
            methodMap.put(method.getName().toUpperCase(), method);
        }
    }

    @Override
    public void run() {
        try (ServerSocket listener = new ServerSocket(port)) {
            while (true) {
                Socket connectionSocket = listener.accept();
                BufferedReader inFromClient = new BufferedReader(new InputStreamReader(
                        connectionSocket.getInputStream()));
                DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
                String command = inFromClient.readLine();
                log.info("Invoking command " + command);
                Object retval = null;
                try {
                    retval = invoke(command);
                    if (retval == null) {
                        retval = "Acknowledged";
                    }
                } catch (Exception e) {
                    retval = "Error";
                }

                log.info("Writing " + retval);
                outToClient.writeBytes(retval.toString());
                connectionSocket.close();
            }
        } catch (Exception e) {
            throw new MetricsException(e);
        }
    }

    Object invoke(String command) throws Exception {
        if (command == null) {
            return null;
        }
        String[] tokens = command.split(" ");

        String cmd = tokens[0];
        Method method = methodMap.get(cmd);

        if (method == null) {
            log.info("Method " + cmd + " does not exist in SinkOperations interface.");
            return null;
        }
        int tokenLen = tokens.length;
        if (tokenLen > 1) {
            String[] tokensWithoutCmd = new String[tokenLen - 1];

            System.arraycopy(tokens, 1, tokensWithoutCmd, 0, tokenLen - 1);
            String param = StringUtils.join(tokensWithoutCmd, ",");
            return method.invoke(this, new Object[] { param });
        } else {
            return method.invoke(this, new Object[] {});
        }
    }

    @Override
    public String canWrite() {
        return Boolean.toString(canWrite);
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    @Override
    public void sendMetric(String metric) {
        log.info(metric);
        try {
            metricsOutputStream.write((metric + "\n").getBytes());

            if (numMetrics % 100 == 0) {
                metricsOutputStream.flush();
            }
        } catch (IOException e) {
            throw new MetricsException(e);
        }
    }

    public void flushToFile() {
        try {
            metricsOutputStream.flush();
        } catch (IOException e) {
            throw new MetricsException(e);
        }
    }
}
