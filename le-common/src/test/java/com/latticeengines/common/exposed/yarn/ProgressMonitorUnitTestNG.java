package com.latticeengines.common.exposed.yarn;

import static org.testng.Assert.assertTrue;

import java.io.DataOutputStream;
import java.net.Socket;
import java.util.Random;

import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProgressMonitorUnitTestNG {

    private ProgressMonitor monitor;

    @BeforeClass(groups = "unit")
    public void setup() {
        ContainerAllocator allocator = new DefaultContainerAllocator();
        monitor = new ProgressMonitor(allocator);
        monitor.start();
    }

    @Test(groups = "unit")
    public void test() {
        int testSize = 5;
        Random rm = new Random();
        for (int i = 0; i < testSize; i++) {
            try {
                Thread.sleep(1000);
                float progress = rm.nextFloat();
                System.out.println("Sending to: " + monitor.getHost() + " " + monitor.getPort()
                        + " with progress value: " + progress);

                Socket clientSocket = new Socket(monitor.getHost(), monitor.getPort());
                DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
                outToServer.writeBytes(Float.toString(progress) + "\n");
                outToServer.flush();
                clientSocket.close();
                Thread.sleep(1000);
                assertTrue(progress == monitor.getProgress());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
