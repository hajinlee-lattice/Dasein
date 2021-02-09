package com.latticeengines.proxy.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.metadata.DataTemplateProxy;


@Component("dataTemplateProxy")
public class DataTemplateProxyImpl extends MicroserviceRestApiProxy implements DataTemplateProxy {

    protected DataTemplateProxyImpl() {
        super("metadata");
    }

    @Override
    public String create(String customerSpace, DataTemplate dataTemplate) {
        String url = constructUrl("/customerspaces/{customerSpace}/datatemplate", shortenCustomerSpace(customerSpace));
        return post("create data template", url, dataTemplate, String.class);
    }

}
