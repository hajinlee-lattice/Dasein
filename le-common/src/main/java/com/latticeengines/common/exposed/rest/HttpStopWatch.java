package com.latticeengines.common.exposed.rest;

import java.util.Map;


public interface HttpStopWatch {

    void start();

    void stop();

    long split(String key);

    Map<String, String> getSplits();

    long getTime();

}
