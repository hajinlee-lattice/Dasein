package com.latticeengines.dataplatform.runtime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.am.allocate.ContainerAllocator;

public class ProgressMonitor {

    private final static Log log = LogFactory.getLog(ProgressMonitor.class);

    private ServerSocket listener;

    private float progress = 0;

    private ContainerAllocator allocator;

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
    }

    public void start() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Listening application progress at : " + listener.getInetAddress().getHostAddress() + " "
                        + listener.getLocalPort());
                while (true) {
                    try {
                        Socket connectionSocket = listener.accept();
                        BufferedReader inFromClient = new BufferedReader(new InputStreamReader(
                                connectionSocket.getInputStream()));
                        String update = inFromClient.readLine();

                        float progress = Float.parseFloat(update);
                        setProgress(progress);
                        log.info("Setting application progress to: " + progress);

                        connectionSocket.close();
                    } catch (IOException e) {
                        log.error("Can't recieve progress status due to: " + e.getStackTrace());
                    }
                }
            }
        }).start();
    }

    /**
     * Sets the progress of the application through Container Allocator
     * inherited from AbstractServiceAppMaster
     * */
    private void setProgress(float progress) {
        this.progress = progress;
        allocator.setProgress(progress);
    }

    public int getPort() {
        return listener.getLocalPort();
    }

    public String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error(e);
        }
        return null;
    }

    public float getProgress() {
        return progress;
    }
}
