package com.latticeengines.proxy.exposed.cdl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CrossSellModelingParameters;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("cdlModelProxy")
public class CDLModelProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/models";

    protected CDLModelProxy() {
        super("cdl");
    }

    public String model(String customerSpace, String modelName, ModelingParameters parameters) {
        String url = constructUrl(URL_PREFIX + "/{modelName}", customerSpace, modelName);
        return post("custom event modeling", url, parameters, String.class);
    }

    public String model(String customerSpace, String modelName,
            CrossSellModelingParameters crossSellModelingParameters) {
        String url = constructUrl(URL_PREFIX + "/rating/{modelName}", customerSpace, modelName);
        return post("modelRatingEngine", url, crossSellModelingParameters, String.class);
    }

    public String clone(String customerSpace, String modelName, CloneModelingParameters cloneModelingParameters) {
        String url = constructUrl(URL_PREFIX + "/rating/{modelName}/clone", customerSpace, modelName);
        return post("cloneAndRemodel", url, cloneModelingParameters, String.class);
    }
}
