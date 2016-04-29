package com.latticeengines.proxy.exposed.propdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.network.exposed.propdata.IngestionInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("ingestionProxy")
public class IngestionProxy extends BaseRestApiProxy implements IngestionInterface {
    public IngestionProxy() {
        super("propdata/ingestions");
    }

    @Override
    public List<IngestionProgress> scan(String hdfsPod) {
        String url = constructUrl("/?HdfsPod={hdfsPod}", hdfsPod);
        List<?> list = post("scan", url, "", List.class);
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
