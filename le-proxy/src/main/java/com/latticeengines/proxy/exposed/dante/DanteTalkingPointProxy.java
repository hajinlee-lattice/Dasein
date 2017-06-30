package com.latticeengines.proxy.exposed.dante;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.network.exposed.dante.DanteTalkingPointInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.security.exposed.serviceruntime.exception.PostResponseErrorHandler;

@Component("danteTalkingPointProxy")
public class DanteTalkingPointProxy extends MicroserviceRestApiProxy implements DanteTalkingPointInterface {

    public DanteTalkingPointProxy() {
        super("/dante/talkingpoints");
    }

    public ResponseDocument<?> createOrUpdate(List<DanteTalkingPoint> talkingPoints) {
        String url = constructUrl("/");
        setErrorHandler(new PostResponseErrorHandler());
        return post("createOrUpdate", url, talkingPoints, ResponseDocument.class);
    }

    public ResponseDocument<?> delete(String externalID) {
        String url = constructUrl("/" + externalID);

        try {
            delete("delete", url);
            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<List<DanteTalkingPoint>> findAllByPlayID(String playID) {
        String url = constructUrl("/play/" + playID);
        return get("findAllByPlayID", url, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<DantePreviewResources> getPreviewResources(String customerSpace) {
        String url = constructUrl("/previewresources" + "?customerSpace=" + customerSpace);
        return get("getPreviewResources", url, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<DanteTalkingPoint> findByExternalID(String externalID) {
        String url = constructUrl("/" + externalID);
        return get("findByExternalID", url, ResponseDocument.class);
    }

}
