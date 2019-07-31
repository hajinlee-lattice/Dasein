package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("migrateTrackingProxy")
public class MigrateTrackingProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/migratetracking";

    protected MigrateTrackingProxy() {
        super("cdl");
    }

    public ImportMigrateTracking creatMigrateTracking(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/create", shortenCustomerSpace(customerSpace));
        return post("Create MigrateTracking record", url, null, ImportMigrateTracking.class);
    }

    public ImportMigrateTracking getMigrateTracking(String customerSpace, Long pid) {
        String url = constructUrl(URL_PREFIX + "/get/{pid}", shortenCustomerSpace(customerSpace), pid);
        return get("Get MigrateTracking record by pid", url, ImportMigrateTracking.class);
    }

    public void updateStatus(String customerSpace, Long pid, ImportMigrateTracking.Status status) {
        String url = constructUrl(URL_PREFIX + "/update/{pid}/status/{status}",
                shortenCustomerSpace(customerSpace), pid, status);
        put("Update MigrateTracking record status", url, null, Void.class);
    }
    public void updateReport(String customerSpace, Long pid, ImportMigrateReport report) {
        String url = constructUrl(URL_PREFIX + "/update/{pid}/report",
                shortenCustomerSpace(customerSpace), pid);
        put("Update MigrateTracking record report", url, report, Void.class);
    }

}
