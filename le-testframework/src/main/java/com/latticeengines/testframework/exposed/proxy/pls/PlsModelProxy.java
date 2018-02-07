package com.latticeengines.testframework.exposed.proxy.pls;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.RatingEngineModelingParameters;

@Service("plsModelProxy")
public class PlsModelProxy extends PlsRestApiProxyBase {

    public PlsModelProxy() {
        super("pls/models");
    }

    public ApplicationId createRatingModel(RatingEngineModelingParameters parameters) {
        String url = constructUrl("/rating/{modelName}", parameters.getName());
        ResponseDocument responseDoc = post("create rating model", url, parameters, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = String.valueOf(responseDoc.getResult());
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

}
