package com.latticeengines.common.exposed.rest;


public interface HttpStopWatch {

    void start();

    long splitAndGetTimeSinceLastSplit();

    void stop();

    long getTime();

}
