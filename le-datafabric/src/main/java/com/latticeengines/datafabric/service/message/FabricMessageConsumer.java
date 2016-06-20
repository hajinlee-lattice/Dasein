package com.latticeengines.datafabric.service.message;

public interface FabricMessageConsumer {

    void start();

    void stop(int waitTime);

}
