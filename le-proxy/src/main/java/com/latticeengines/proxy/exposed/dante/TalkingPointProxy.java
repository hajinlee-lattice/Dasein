package com.latticeengines.proxy.exposed.dante;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.network.exposed.dante.TalkingPointInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.security.exposed.serviceruntime.exception.PostResponseErrorHandler;

@Component("talkingPointProxy")
public class TalkingPointProxy extends MicroserviceRestApiProxy implements TalkingPointInterface {

    public TalkingPointProxy() {
        super("/dante/talkingpoints");
    }

    public ResponseDocument<?> createOrUpdate(List<TalkingPointDTO> talkingPoints, String customerSpace) {
        String url = constructUrl("?customerSpace=" + customerSpace);
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
    public ResponseDocument<List<TalkingPointDTO>> findAllByPlayName(String playName) {
        String url = constructUrl("/play/" + playName);
        return get("findAllByPlayName", url, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<DantePreviewResources> getPreviewResources(String customerSpace) {
        String url = constructUrl("/previewresources?customerSpace=" + customerSpace);
        return get("getPreviewResources", url, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<?> publish(String playName, String customerSpace) {
        String url = constructUrl("/publish" + "?playName=" + playName + "&customerSpace=" + customerSpace);
        return post("publish", url, null, ResponseDocument.class);
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<TalkingPointDTO> findByName(String name) {
        String url = constructUrl("/" + name);
        return get("findByName", url, ResponseDocument.class);
    }

}
