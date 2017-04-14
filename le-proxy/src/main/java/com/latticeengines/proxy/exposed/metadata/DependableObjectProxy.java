package com.latticeengines.proxy.exposed.metadata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.network.exposed.metadata.DependableObjectInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class DependableObjectProxy extends MicroserviceRestApiProxy implements DependableObjectInterface {
    protected DependableObjectProxy() {
        super("metadata");
    }

    @Override
    public DependableObject find(String customerSpace, String type, String name) {
        String url = constructUrl("/customerspaces/{customerspace}/dependencies/?type={type}&name={name}",
                customerSpace, type, name);
        return get("find", url, DependableObject.class);
    }

    @Override
    public boolean createOrUpdate(String customerSpace, DependableObject dependableObject) {
        String url = constructUrl("/customerspaces/{customerSpace}/dependencies/", customerSpace);
        return post("createOrUpdate", url, dependableObject, Boolean.class);
    }

    @Override
    public boolean delete(String customerSpace, String type, String name) {
        String url = constructUrl("/customerspaces/{customerSpace}/dependencies/?type={type}&name={name}",
                customerSpace, type, name);
        super.delete("delete", url);
        return true;
    }
}
