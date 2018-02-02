package com.latticeengines.admin.service;

import java.util.Map;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public interface ServiceConfigService {
    SerializableDocumentDirectory setDefaultInvokeTime(String serviceName, SerializableDocumentDirectory rawDir);

    void verifyInvokeTime(String serviceName, boolean allowAutoSchedule, String nodePath, String data);

    void verifyInvokeTime(boolean allowAutoSchedule, Map<String, Map<String, String>> props);
}
