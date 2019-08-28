package com.latticeengines.proxy.exposed.pls;

import java.util.Map;

import com.latticeengines.domain.exposed.StatusDocument;

public interface PlsHealthCheckProxy {

    StatusDocument systemCheck();

    Map<String, String> getActiveStack();

}
