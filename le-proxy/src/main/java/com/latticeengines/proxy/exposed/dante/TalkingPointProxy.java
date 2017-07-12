package com.latticeengines.proxy.exposed.dante;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TalkingPoint;
import com.latticeengines.network.exposed.dante.TalkingPointInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.security.exposed.serviceruntime.exception.PostResponseErrorHandler;

@Component("talkingPointProxy")
public class TalkingPointProxy extends MicroserviceRestApiProxy implements TalkingPointInterface {

    public TalkingPointProxy() {
        super("/dante/talkingpoints");
    }

    public ResponseDocument<?> createOrUpdate(List<TalkingPoint> talkingPoints) {
        String url = constructUrl("/");
        setErrorHandler(new PostResponseErrorHandler());
        return post("createOrUpdate", url, talkingPoints, ResponseDocument.class);
    }

    public ResponseDocument<?> delete(String name) {
        String url = constructUrl("/" + name);

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
    public ResponseDocument<List<TalkingPoint>> findAllByPlayID(Long playID) {
        String url = constructUrl("/play/" + playID);
        return get("findAllByPlayId", url, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<DantePreviewResources> getPreviewResources(String customerSpace) {
        String url = constructUrl("/previewresources" + "?customerSpace=" + customerSpace);
        return get("getPreviewResources", url, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<?> publish(Long playId) {
        String url = constructUrl("/publish" + "?playId=" + playId);
        return post("publish", url, null, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<TalkingPoint> findByName(String name) {
        String url = constructUrl("/" + name);
        return get("findByName", url, ResponseDocument.class);
    }

}
