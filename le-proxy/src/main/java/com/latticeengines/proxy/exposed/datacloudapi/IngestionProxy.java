package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.network.exposed.propdata.IngestionInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("ingestionProxy")
public class IngestionProxy extends MicroserviceRestApiProxy implements IngestionInterface {
    public IngestionProxy() {
        super("datacloudapi/ingestions");
    }

    @Override
    public List<IngestionProgress> scan(String hdfsPod) {
        String url = constructUrl("/?HdfsPod={hdfsPod}", hdfsPod);
        List<?> list = post("scan_ingestion", url, "", List.class);
        List<IngestionProgress> progresses = new ArrayList<>();
        if (list == null) {
            return progresses;
        }
        for (Object obj : list) {
            String json = JsonUtils.serialize(obj);
            IngestionProgress progress = JsonUtils.deserialize(json, IngestionProgress.class);
            progresses.add(progress);
        }
        return progresses;
    }

    @Override
    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod) {
        hdfsPod = StringUtils.isEmpty(hdfsPod) ? "" : hdfsPod;
        String url = constructUrl("/internal/{ingestionName}?HdfsPod={hdfsPod}", ingestionName,
                hdfsPod);
        return post("ingestInternal", url, ingestionRequest, IngestionProgress.class);
    }
}
