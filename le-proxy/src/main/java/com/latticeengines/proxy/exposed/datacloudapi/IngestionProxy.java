package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("ingestionProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class IngestionProxy extends MicroserviceRestApiProxy {
    public IngestionProxy() {
        super("datacloudapi/ingestions");
    }

    public List<IngestionProgress> scan(String hdfsPod) {
        String url = constructUrl("/?HdfsPod={hdfsPod}", hdfsPod);
        List<?> list = post("scan_ingestion", url, "", List.class);
        return JsonUtils.convertList(list, IngestionProgress.class);
    }

    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod) {
        hdfsPod = StringUtils.isEmpty(hdfsPod) ? "" : hdfsPod;
        String url = constructUrl("/internal/{ingestionName}?HdfsPod={hdfsPod}", ingestionName,
                hdfsPod);
        return post("ingestInternal", url, ingestionRequest, IngestionProgress.class);
    }
}
