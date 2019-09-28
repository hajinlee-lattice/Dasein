package com.latticeengines.testframework.exposed.proxy.pls;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.CrossSellModelingParameters;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;

@Service("plsModelProxy")
public class PlsModelProxy extends PlsRestApiProxyBase {

    public PlsModelProxy() {
        super("pls/models");
    }

    public ApplicationId kickOffCrossSellModel(CrossSellModelingParameters parameters) {
        String url = constructUrl("/rating/{modelName}", parameters.getName());
        ResponseDocument<?> responseDoc = post("create rating model", url, parameters, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult().toString());
    }

}
