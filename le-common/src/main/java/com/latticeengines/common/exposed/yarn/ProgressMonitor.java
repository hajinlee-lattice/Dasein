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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.am.allocate.ContainerAllocator;

public class ProgressMonitor {

    private static final Logger log = LoggerFactory.getLogger(ProgressMonitor.class);

    private static final int MAX_ATTEMPTS = 5;

    private ServerSocket listener;

    private float progress = 0;

    private ContainerAllocator allocator;
    private Progressable progressable;

    private ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean(false);


    public ProgressMonitor(ContainerAllocator allocator) {
        this.allocator = allocator;
        init();
    }

    public ProgressMonitor(Progressable progressable) {
        this.progressable = progressable;
        init();
    }

    private void init() {
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

    public synchronized void start() {
        if (listener == null) {
            return;
        }

        if (!started.get()) {
            executor.execute(() -> {
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

                        if (log.isInfoEnabled()) {
                            log.info("Setting application progress to: " + progress);
                        }
                        connectionSocket.close();
                    } catch (Exception e) {
                        log.error("Can't receive progress status due to: " + ExceptionUtils.getStackTrace(e));
                    }
                }
                log.info("Listening thread terminated");
            });
            started.set(true);
        }

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
        if (allocator != null) {
            allocator.setProgress(progress);
        } else {
            progressable.progress();
        }
    }

    public float getProgress() {
        return progress;
    }

    public int getPort() {
        return listener.getLocalPort();
    }

    public String getHost() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            if (host.startsWith("ip-")) {
                // use ip instead of hostname
                host = InetAddress.getLocalHost().getHostAddress();
            }
            return host;
        } catch (UnknownHostException e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return null;
    }
}
