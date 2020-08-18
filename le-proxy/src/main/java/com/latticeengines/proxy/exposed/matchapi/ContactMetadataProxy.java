package com.latticeengines.proxy.exposed.matchapi;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("ContactMetadataProxy")
public class ContactMetadataProxy extends BaseRestApiProxy {

    public ContactMetadataProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/contact-metadata");
    }

    @SuppressWarnings("unchecked")
    public List<String> getTpsJobFunctions() {
        String url = constructUrl("/tps-job-functions");
        return get("getTpsJobFunctions", url, List.class);
    }

    @SuppressWarnings("unchecked")
    public List<String> getTpsJobLevels() {
        String url = constructUrl("/tps-job-levels");
        return get("getTpsJobLevels", url, List.class);
    }
}
