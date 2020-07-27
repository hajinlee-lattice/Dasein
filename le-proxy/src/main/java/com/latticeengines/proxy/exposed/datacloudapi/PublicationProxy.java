package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationResponse;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("publicationProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class PublicationProxy extends MicroserviceRestApiProxy {

    public PublicationProxy() {
        super("datacloudapi/publications");
    }

    @Deprecated // No use in production
    public List<PublicationProgress> scan(String hdfsPod) {
        String url;
        if (StringUtils.isBlank(hdfsPod)) {
            url = "/";
        } else {
            url = constructUrl("/?podid={hdfsPod}", hdfsPod);
        }
        List<?> list = post("scan_publication", url, "", List.class);
        List<PublicationProgress> progresses = new ArrayList<>();
        for (Object obj : list) {
            String json = JsonUtils.serialize(obj);
            PublicationProgress progress = JsonUtils.deserialize(json, PublicationProgress.class);
            progresses.add(progress);
        }
        return progresses;
    }

    public PublicationResponse publish(String publicationName, PublicationRequest publicationRequest, String hdfsPod) {
        String url;
        if (StringUtils.isBlank(hdfsPod)) {
            url = constructUrl("/{pubName}", publicationName);
        } else {
            url = constructUrl("/{pubName}?podid={hdfsPod}", publicationName, hdfsPod);
        }
        return post("publish", url, publicationRequest, PublicationResponse.class);
    }

}
