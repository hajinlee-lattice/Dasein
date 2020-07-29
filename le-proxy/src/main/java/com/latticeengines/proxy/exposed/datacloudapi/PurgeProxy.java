package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("purgeProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class PurgeProxy extends MicroserviceRestApiProxy {
    public PurgeProxy() {
        super("datacloudapi/purge");
    }

    public List<PurgeSource> getPurgeSources(String hdfsPod) {
        String url = constructUrl("/sources?HdfsPod={hdfsPod}", hdfsPod);
        List<?> list = get("purge_source", url, List.class);
        List<PurgeSource> purgeSrcs = new ArrayList<>();
        if (list == null) {
            return purgeSrcs;
        }
        for (Object obj : list) {
            String json = JsonUtils.serialize(obj);
            PurgeSource purgeSrc = JsonUtils.deserialize(json, PurgeSource.class);
            purgeSrcs.add(purgeSrc);
        }
        return purgeSrcs;
    }

    public List<String> getUnknownSources(String hdfsPod) {
        String url = constructUrl("/sources/unknown?HdfsPod={hdfsPod}", hdfsPod);
        return getList("unknown_source", url, String.class);
    }
}
