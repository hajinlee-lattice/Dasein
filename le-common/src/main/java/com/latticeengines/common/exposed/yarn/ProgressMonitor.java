package com.latticeengines.common.exposed.yarn;

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

    private static final int MAX_ATTEMPTS = 5;

    private ServerSocket listener;

    private float progress = 0;

    private ContainerAllocator allocator;

    private ExecutorService executor;

    public ProgressMonitor(ContainerAllocator allocator) {
        this.allocator = allocator;
        int attempt = 1;
        while (attempt <= MAX_ATTEMPTS) {
            try {
                listener = new ServerSocket(0);
                break;
            } catch (IOException e) {
                log.warn("Couldn't find open port at " + attempt + " attempt due to: "
                        + ExceptionUtils.getStackTrace(e));
                attempt++;
            }
        }
        if (listener == null) {
            log.error("Couldn't find open port after " + MAX_ATTEMPTS + " attempts; aborting progress monitor");
        }

        executor = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        if (listener == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                log.info("Listening application progress at : " + listener.getInetAddress().getHostAddress() + " "
                        + listener.getLocalPort());
                // executor.shutdownNow() will interrupt this thread
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket connectionSocket = listener.accept();

                        try (BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket
                                .getInputStream()))) {
                            String update = inFromClient.readLine();
                            float progress = Float.parseFloat(update);
                            setProgress(progress);
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("Setting application progress to: " + progress);
                        }
                        connectionSocket.close();
                    } catch (Exception e) {
                        log.error("Can't receive progress status due to: " + ExceptionUtils.getStackTrace(e));
                    }
                }
                log.info("Listening thread terminated");
            }
        });
    }

    public void stop() {
        if (listener == null) {
            return;
        }

        log.info("Shutting down progress monitor");
        executor.shutdownNow();

        // Shut down socket
        try {
            listener.close();
        } catch (IOException e) {
            log.error("Couldn't close progress monitor socket due to: " + ExceptionUtils.getStackTrace(e));
        }
    }

    private void setProgress(float progress) {
        this.progress = progress;
        // Allocator reports progress asynchronously to RM through heart beat
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
