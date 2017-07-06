package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.network.exposed.propdata.PublicationInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("publicationProxy")
public class PublicationProxy extends MicroserviceRestApiProxy implements PublicationInterface {

    public PublicationProxy() {
        super("datacloudapi/publications");
    }

    @Override
    public List<PublicationProgress> scan(String hdfsPod) {
        String url = constructUrl("/?podid={hdfsPod}", hdfsPod);
        List<?> list = post("scan_publication", url, "", List.class);
        List<PublicationProgress> progresses = new ArrayList<>();
        for (Object obj : list) {
            String json = JsonUtils.serialize(obj);
            PublicationProgress progress = JsonUtils.deserialize(json, PublicationProgress.class);
            progresses.add(progress);
        }
        return progresses;
    }

    @Override
    public PublicationProgress publish(String publicationName, PublicationRequest publicationRequest, String hdfsPod) {
        hdfsPod = StringUtils.isEmpty(hdfsPod) ? "" : hdfsPod;
        String url = constructUrl("/{pubName}?podid={hdfsPod}", publicationName, hdfsPod);
        return post("publish", url, publicationRequest, PublicationProgress.class);
    }

}
