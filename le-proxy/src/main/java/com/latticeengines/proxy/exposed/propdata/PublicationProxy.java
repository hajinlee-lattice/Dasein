package com.latticeengines.proxy.exposed.propdata;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.network.exposed.propdata.PublicationInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("publicationProxy")
public class PublicationProxy extends BaseRestApiProxy implements PublicationInterface {

    public PublicationProxy() {
        super("propdata/publications");
    }

    @Override
    public List<PublicationProgress> scan(String hdfsPod) {
        String url = constructUrl("/?podid={hdfsPod}", hdfsPod);
        List<?> list = post("scan_publication", url, "", List.class);
        List<PublicationProgress> progresses = new ArrayList<>();
        for (Object obj : list) {
            if (obj instanceof PublicationProgress) {
                progresses.add((PublicationProgress) obj);
            }
        }
        return progresses;
    }

    @Override
    public PublicationProgress publish(String publicationName, String submitter, String hdfsPod) {
        String url = constructUrl("/{pubName}?submitter={submitter}&podid={hdfsPod}", publicationName, submitter,
                hdfsPod);
        return post("publish", url, "", PublicationProgress.class);
    }

}
