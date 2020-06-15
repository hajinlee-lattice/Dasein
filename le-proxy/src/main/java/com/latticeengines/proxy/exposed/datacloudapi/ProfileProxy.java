package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.statistics.ProfileArgument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("profileProxy")
public class ProfileProxy extends MicroserviceRestApiProxy {

    public ProfileProxy() {
        super("datacloudapi/profile");
    }

    @SuppressWarnings("unchecked")
    public Map<String, ProfileArgument> getAMAttrsConfig(String dataCloudVersion) {
        String url = constructUrl("/arguments");
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url += "?dataCloudVersion=" + dataCloudVersion;
        }
        return getKryo("getAMAttrsConfig", url, Map.class);
    }

}
