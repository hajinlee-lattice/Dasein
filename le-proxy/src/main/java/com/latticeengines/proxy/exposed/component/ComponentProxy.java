package com.latticeengines.proxy.exposed.component;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.ComponentStatus;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

@Component("componentProxy")
public class ComponentProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    public ComponentProxy() {
        super("");
    }

    public boolean install(String customerSpace, String serviceName, InstallDocument installDocument) {
        String path = "{serviceName}/component/customerSpace/{customerSpace}/install";
        path = constructUrl(path, getRootpath(serviceName), shortenCustomerSpace(customerSpace));
        return post("Install component", path, installDocument, Boolean.class);
    }

    public boolean destroy(String customerSpace, String serviceName) {
        String path = "{serviceName}/component/customerSpace/{customerSpace}/destroy";
        path = constructUrl(path, getRootpath(serviceName), shortenCustomerSpace(customerSpace));
        return post("Uninstall component", path, null, Boolean.class);
    }

    public ComponentStatus getComponentStatus(String customerSpace, String serviceName) {
        String path = "{serviceName}/component/customerSpace/{customerSpace}/status";
        path = constructUrl(path, getRootpath(serviceName), shortenCustomerSpace(customerSpace));

        return get("Get component status", path, ComponentStatus.class);
    }

    public void setComponentStatus(String customerSpace, String serviceName, ComponentStatus status) {
        String path = "{serviceName}/component/customerSpace/{customerSpace}/status/{status}";
        path = constructUrl(path, getRootpath(serviceName), shortenCustomerSpace(customerSpace), status.name());
        put("Set component status", path);
    }

    public boolean reset(String customerSpace, String serviceName) {
        String path = "{serviceName}/component/customerSpace/{customerSpace}/reset";
        path = constructUrl(path, getRootpath(serviceName), shortenCustomerSpace(customerSpace));
        return post("Reset component", path, null, Boolean.class);
    }

    private String getRootpath(String serivceName) {
        if (StringUtils.isEmpty(serivceName)) {
            throw new IllegalArgumentException("Service name cannot be empty!");
        }
        serivceName = serivceName.toUpperCase();
        switch (serivceName) {
            case ComponentConstants.CDL:
                return "cdl";
            case ComponentConstants.METADATA:
                return "metadata";
            case ComponentConstants.LP:
            case ComponentConstants.PLS:
                return "lp";
            default:
                throw new IllegalArgumentException("Cannot recognize service : " + serivceName);
        }
    }

}
