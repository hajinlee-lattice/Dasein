package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.CreateCatalogRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("activityStoreProxy")
public class ActivityStoreProxy extends MicroserviceRestApiProxy implements ProxyInterface {
    private static final String ROOT_PATH = "cdl";

    protected ActivityStoreProxy() {
        super(ROOT_PATH);
    }

    public ActivityStoreProxy(String hostPort) {
        super(hostPort, ROOT_PATH);
    }

    public Catalog createCatalog(@NotNull String customerSpace, @NotNull String catalogName, String taskUniqueId) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/catalogs",
                shortenCustomerSpace(customerSpace));
        CreateCatalogRequest request = new CreateCatalogRequest(catalogName, taskUniqueId);
        return post("create_catalog", url, request, Catalog.class);
    }

    public Catalog findCatalogByName(@NotNull String customerSpace, @NotNull String catalogName) {
        String url = constructUrl("/customerspaces/{customerSpace}/activities/catalogs/{name}",
                shortenCustomerSpace(customerSpace), catalogName);
        return get("find_catalog_by_name", url, Catalog.class);
    }
}
