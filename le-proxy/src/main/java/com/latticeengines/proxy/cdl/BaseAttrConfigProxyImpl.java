package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

public abstract class BaseAttrConfigProxyImpl extends MicroserviceRestApiProxy {

    protected BaseAttrConfigProxyImpl(String rootpath, Object... urlVariables) {
        super(rootpath, urlVariables);
    }

    public AttrConfigRequest getAttrConfigByEntity(String customerSpace, BusinessEntity entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/entities/{entity}", //
                shortenCustomerSpace(customerSpace), entity);
        return getKryo("get attr config by entity", url, AttrConfigRequest.class);
    }

    public AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/categories/{categoryName}", //
                shortenCustomerSpace(customerSpace), categoryName);
        return getKryo("get attr config by category", url, AttrConfigRequest.class);
    }

    public AttrConfigRequest saveAttrConfig(String customerSpace, AttrConfigRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/", //
                shortenCustomerSpace(customerSpace));
        return post("save attr config", url, request, AttrConfigRequest.class);
    }

    public AttrConfigRequest validateAttrConfig(String customerSpace, AttrConfigRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/validate", //
                shortenCustomerSpace(customerSpace));
        return post("validate attr config request", url, request, AttrConfigRequest.class);
    }

}
