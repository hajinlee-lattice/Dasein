package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelFeatureImportanceProxy;

@Component("modelFeatureImportanceProxy")
public class ModelFeatureImportanceProxyImpl extends MicroserviceRestApiProxy implements ModelFeatureImportanceProxy {

    protected ModelFeatureImportanceProxyImpl() {
        super("lp");
    }

    @Override
    public void upsertModelFeatureImportances(String customerSpace, String modelGuId) {
        String url = constructUrl("/customerspaces/{customerSpace}/featureimportances/model/{modelGuId}",
                shortenCustomerSpace(customerSpace), modelGuId);
        post("upsert feature importances", url, null, SimpleBooleanResponse.class);
    }

    @Override
    public List<ModelFeatureImportance> getFeatureImportanceByModelGuid(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/featureimportances/model/{modelGuId}",
                shortenCustomerSpace(customerSpace), modelGuid);
        return getList("get feature importances for model", url, ModelFeatureImportance.class);

    }

}
