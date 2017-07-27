package com.latticeengines.proxy.exposed.dante;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.network.exposed.dante.TalkingPointInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.security.exposed.serviceruntime.exception.PostResponseErrorHandler;

@Component("talkingPointProxy")
public class TalkingPointProxy extends MicroserviceRestApiProxy implements TalkingPointInterface {

    public TalkingPointProxy() {
        super("/dante/talkingpoints");
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> talkingPoints, String customerSpace) {
        String url = constructUrl("?customerSpace=" + customerSpace);
        setErrorHandler(new PostResponseErrorHandler());
        return post("createOrUpdate", url, talkingPoints, List.class);
    }

    public void delete(String name) {
        String url = constructUrl("/" + name);
        delete("delete", url);
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPointDTO> findAllByPlayName(String playName) {
        String url = constructUrl("/play/" + playName);
        List list = get("findAllByPlayName", url, List.class);
        return JsonUtils.convertList(list, TalkingPointDTO.class);
    }

    @SuppressWarnings("unchecked")
    public TalkingPointPreview getTalkingPointPreview(String playName, String customerSpace) {
        String url = constructUrl("/preview" + "?playName=" + playName + "&customerSpace=" + customerSpace);
        return get("getTalkingPointPreview", url, TalkingPointPreview.class);
    }

    @SuppressWarnings("unchecked")
    public DantePreviewResources getPreviewResources(String customerSpace) {
        String url = constructUrl("/previewresources?customerSpace=" + customerSpace);
        return get("getPreviewResources", url, DantePreviewResources.class);
    }

    @SuppressWarnings("unchecked")
    public void publish(String playName, String customerSpace) {
        String url = constructUrl("/publish" + "?playName=" + playName + "&customerSpace=" + customerSpace);
        post("publish", url, null, String.class);
    }

    @SuppressWarnings("unchecked")
    public TalkingPointDTO findByName(String name) {
        String url = constructUrl("/" + name);
        return get("findByName", url, TalkingPointDTO.class);
    }

}
