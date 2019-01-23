package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceapps.lp.CopyModelRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ModelCopyProxy;

@Component
public class ModelCopyProxyImpl extends MicroserviceRestApiProxy implements ModelCopyProxy {

    protected ModelCopyProxyImpl() {
        super("lp");
    }

    @Override
    public String copyModel(String sourceTenant, String targetTenant, String modelGuid, String async) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelcopy", shortenCustomerSpace(sourceTenant));
        CopyModelRequest request = new CopyModelRequest();
        request.setModelGuid(modelGuid);
        request.setAsync(async);
        request.setTargetTenant(shortenCustomerSpace(targetTenant));
        return post("copy model", url, request, String.class);
    }

    @Override
    public Table cloneTrainingTable(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/modelcopy/modelid/{modelGuid}/training-table",
                shortenCustomerSpace(customerSpace), modelGuid);
        return post("clone training table", url, null, Table.class);
    }

}
