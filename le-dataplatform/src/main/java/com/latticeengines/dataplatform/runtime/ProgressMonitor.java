package com.latticeengines.dataplatform.runtime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.am.allocate.ContainerAllocator;

public class ProgressMonitor {

    private final static Log log = LogFactory.getLog(ProgressMonitor.class);

    private ServerSocket listener;

    private float progress = 0;

    private ContainerAllocator allocator;

    private ExecutorService executor;

    public ProgressMonitor(ContainerAllocator allocator) {
        if (allocator == null) {
            log.error("Container Allocator is null");
        }
        this.allocator = allocator;

        while (true) {
            try {
                listener = new ServerSocket(0);
                break;
            } catch (IOException e) {
                log.error("Can't find open port due to: " + e.getStackTrace());
            }
        }

        executor = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                log.info("Listening application progress at : " + listener.getInetAddress().getHostAddress() + " "
                        + listener.getLocalPort());
                // executor.shutdownNow() will interrupt this thread
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket connectionSocket = listener.accept();
                        BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket
                                .getInputStream()));
                        String update = inFromClient.readLine();

                        float progress = Float.parseFloat(update);
                        setProgress(progress);
                        log.info("Setting application progress to: " + progress);

                        connectionSocket.close();
                    } catch (IOException e) {
                        log.error("Can't recieve progress status due to: " + e.getStackTrace().toString());
                    }
                }
            }
        });
    }

    public void stop() {
        log.info("Shutting down progress monitor");
        executor.shutdownNow();
        try {
            listener.close();
        } catch (IOException e) {
            log.error("Can't close progress monitor socket due to: " + ExceptionUtils.getStackTrace(e));
        }
    }

    private void setProgress(float progress) {
        this.progress = progress;
        allocator.setProgress(progress);
    }

    public float getProgress() {
        return progress;
    }

    public int getPort() {
        return listener.getLocalPort();
    }

    public String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return null;
    }
}
