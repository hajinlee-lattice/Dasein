package com.latticeengines.admin.service;

import java.util.Map;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public interface ServiceConfigService {
    SerializableDocumentDirectory setDefaultInvokeTime(String serviceName, SerializableDocumentDirectory rawDir);

    void verifyInvokeTime(String serviceName, String nodePath, String data);

    void verifyInvokeTime(Map<String, Map<String, String>> props);
}
