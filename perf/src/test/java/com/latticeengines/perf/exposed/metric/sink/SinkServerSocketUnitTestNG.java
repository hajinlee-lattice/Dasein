package com.latticeengines.perf.exposed.metric.sink;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SinkServerSocketUnitTestNG {
    
    private SinkCollectionServer sinkServerSocket;

    @BeforeClass(groups = "unit")
    public void setup() {

        sinkServerSocket = spy(new SinkCollectionServer("/tmp/metricfile.txt", 123));
    }
    
    @Test(groups = "unit")
    public void invokeCanWrite() throws Exception {
        sinkServerSocket.invoke("CANWRITE");
        verify(sinkServerSocket, times(1)).canWrite();
    }

    @Test(groups = "unit")
    public void invokeSendMetric() throws Exception {
        sinkServerSocket.invoke("SENDMETRIC xyz");
        verify(sinkServerSocket, times(1)).sendMetric("xyz");
    }
}
