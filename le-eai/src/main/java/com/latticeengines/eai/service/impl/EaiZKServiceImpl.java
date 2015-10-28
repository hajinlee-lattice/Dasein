package com.latticeengines.eai.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.eai.config.HttpClientConfig;
import com.latticeengines.eai.service.EaiZKService;

@Component("eaiZKService")
public class EaiZKServiceImpl implements EaiZKService {

    private final static String SERVICE_NAME = "Eai";

    private final static String SFSC_ENDPOINT_CONFIG = "SalesforceEndpointConfig";

    private final static String HTTP_Client = "HttpClient";

    private final static String CONNECT_TIMEOUT = "ConnectTimeout";

    private final static String IMPORT_TIMEOUT = "ImportTimeout";

    @Override
    public HttpClientConfig getHttpClientConfig(String customerSpace) {
        try{
            DocumentDirectory documentDirectory = getHttpClientConfigDocumentDirectory(customerSpace);
            int connectTimeout = Integer.valueOf(documentDirectory.getChild(CONNECT_TIMEOUT).getDocument().getData());
            int importTimeout = Integer.valueOf(documentDirectory.getChild(IMPORT_TIMEOUT).getDocument().getData());
            return new HttpClientConfig(connectTimeout, importTimeout);
        }catch(Exception e){
            throw new LedpException(LedpCode.LEDP_17005, e, new String[]{customerSpace});
        }
    }

    @Override
    public DocumentDirectory getSalesforceEndpointConfigDocumentDirectory(String customerSpace) {
        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customerSpace), SERVICE_NAME);
        docPath = docPath.append(SFSC_ENDPOINT_CONFIG);
        return camille.getDirectory(docPath);
    }

    @Override
    public DocumentDirectory getHttpClientConfigDocumentDirectory(String customerSpace) {
        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customerSpace), SERVICE_NAME);
        docPath = docPath.append(SFSC_ENDPOINT_CONFIG).append(HTTP_Client);
        return camille.getDirectory(docPath);
    }
}
