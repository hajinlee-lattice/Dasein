package com.latticeengines.eai.service;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.eai.config.HttpClientConfig;

public interface EaiZKService {

    HttpClientConfig getHttpClientConfig(String customerSpace);

    DocumentDirectory getSalesforceEndpointConfigDocumentDirectory(String customerSpace);

    DocumentDirectory getHttpClientConfigDocumentDirectory(String customerSpace);

}
