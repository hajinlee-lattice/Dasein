package com.latticeengines.microservice.service;

import java.util.Map;

public interface StatusService {

    Map<String, String> moduleStatus();

    Map<String, String> appStatus();

    void unhookApp(String app);

}
