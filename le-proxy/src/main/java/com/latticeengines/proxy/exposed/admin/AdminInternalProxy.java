package com.latticeengines.proxy.exposed.admin;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("adminInternalProxy")
public class AdminInternalProxy extends BaseRestApiProxy {

    public AdminInternalProxy() {
        super(PropertyUtils.getProperty("common.admin.url"), "admin/internal");
    }

    // only works in QA
    public void deletePermStore(String tenantName) {
        String url = constructUrl("/BODCDEVVINT207/BODCDEVVINT187/" + tenantName);
        delete("delete permstore", url);
    }

}
