package com.latticeengines.proxy.exposed.dante;
//
//import java.util.List;
//
//import org.springframework.stereotype.Component;
//
//import com.latticeengines.common.exposed.util.JsonUtils;
//import com.latticeengines.domain.exposed.dante.DantePreviewResources;
//import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
//import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
//import com.latticeengines.domain.exposed.query.AttributeLookup;
//import com.latticeengines.network.exposed.dante.TalkingPointInterface;
//import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
//import com.latticeengines.security.exposed.serviceruntime.exception.PostResponseErrorHandler;
//
//@Component("talkingPointProxy")
//public class TalkingPointProxy extends MicroserviceRestApiProxy implements TalkingPointInterface {
//
//    public TalkingPointProxy() {
//        super("/dante/talkingpoints");
//    }
//
//    @SuppressWarnings("unchecked")
//    public List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> talkingPoints, String customerSpace) {
//        String url = constructUrl("?customerSpace=" + customerSpace);
//        setErrorHandler(new PostResponseErrorHandler());
//        return post("createOrUpdate", url, talkingPoints, List.class);
//    }
//
//    public void delete(String name) {
//        String url = constructUrl("/" + name);
//        delete("delete", url);
//    }
//
//    public List<TalkingPointDTO> findAllByPlayName(String playName) {
//        return findAllByPlayName(playName, false);
//    }
//
//    @SuppressWarnings("unchecked")
//    public List<TalkingPointDTO> findAllByPlayName(String playName, boolean publishedOnly) {
//        String url = constructUrl("/play/" + playName) + "?publishedonly=" + publishedOnly;
//        return get("findAllByPlayName", url, List.class);
//    }
//
//    public TalkingPointPreview getTalkingPointPreview(String playName, String customerSpace) {
//        String url = constructUrl("/preview" + "?playName=" + playName + "&customerSpace=" + customerSpace);
//        return get("getTalkingPointPreview", url, TalkingPointPreview.class);
//    }
//
//    public DantePreviewResources getPreviewResources(String customerSpace) {
//        String url = constructUrl("/previewresources?customerSpace=" + customerSpace);
//        return get("getPreviewResources", url, DantePreviewResources.class);
//    }
//
//    public void publish(String playName, String customerSpace) {
//        String url = constructUrl("/publish" + "?playName=" + playName + "&customerSpace=" + customerSpace);
//        post("publish", url, null, String.class);
//    }
//
//    @SuppressWarnings("unchecked")
//    public List<TalkingPointDTO> revert(String playName, String customerSpace) {
//        String url = constructUrl("/revert" + "?playName=" + playName + "&customerSpace=" + customerSpace);
//        return post("revert", url, null, List.class);
//    }
//
//    public TalkingPointDTO findByName(String name) {
//        String url = constructUrl("/" + name);
//        return get("findByName", url, TalkingPointDTO.class);
//    }
//
//    public List<AttributeLookup> getAttributesInTalkingPointOfPlay(String playName) {
//        String url = constructUrl("/attributes/" + playName);
//        List<?> list = get("getAttributesInTalkingPointOfPlay", url, List.class);
//        return JsonUtils.convertList(list, AttributeLookup.class);
//    }
//}
