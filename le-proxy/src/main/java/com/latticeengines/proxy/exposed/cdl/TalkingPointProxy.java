package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;
import java.util.List;
import org.springframework.stereotype.Component;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.domain.exposed.query.AttributeLookup;
// import com.latticeengines.network.exposed.dante.TalkingPointInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;
import com.latticeengines.security.exposed.serviceruntime.exception.PostResponseErrorHandler;;

@Component("talkingPointProxy")
public class TalkingPointProxy extends MicroserviceRestApiProxy implements ProxyInterface {
    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/talkingpoints";

    public TalkingPointProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPointDTO> createOrUpdate(String customerSpace,
            List<TalkingPointDTO> talkingPoints) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        setErrorHandler(new PostResponseErrorHandler());
        return post("createOrUpdate", url, talkingPoints, List.class);
    }

    public void delete(String customerSpace, String name) {
        String url = constructUrl(URL_PREFIX + "/" + name, shortenCustomerSpace(customerSpace));
        delete("delete", url);
    }

    public List<TalkingPointDTO> findAllByPlayName(String customerSpace, String playName) {
        return findAllByPlayName(customerSpace, playName, false);
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPointDTO> findAllByPlayName(String customerSpace, String playName,
            boolean publishedOnly) {
        String url =
                constructUrl(URL_PREFIX + "/play/" + playName, shortenCustomerSpace(customerSpace))
                        + "?publishedonly=" + publishedOnly;
        // String url = constructUrl("/play/" + playName) + "?publishedonly=" + publishedOnly;
        return get("findAllByPlayName", url, List.class);
    }

    public TalkingPointPreview getTalkingPointPreview(String customerSpace, String playName) {
        String url = constructUrl(URL_PREFIX + "/preview" + "?playName=" + playName,
                shortenCustomerSpace(customerSpace));
        // String url = constructUrl("/preview" + "?playName=" + playName + "&customerSpace=" +
        // customerSpace);
        return get("getTalkingPointPreview", url, TalkingPointPreview.class);
    }

    public DantePreviewResources getPreviewResources(String customerSpace) {
        String url =
                constructUrl(URL_PREFIX + "/previewresources", shortenCustomerSpace(customerSpace));
        // String url = constructUrl("/previewresources?customerSpace=" + customerSpace);
        return get("getPreviewResources", url, DantePreviewResources.class);
    }

    public void publish(String customerSpace, String playName) {
        String url = constructUrl(URL_PREFIX + "/publish" + "?playName=" + playName,
                shortenCustomerSpace(customerSpace));
        // String url = constructUrl("/publish" + "?playName=" + playName + "&customerSpace=" +
        // customerSpace);
        post("publish", url, null, String.class);
    }

    @SuppressWarnings("unchecked")
    public List<TalkingPointDTO> revert(String customerSpace, String playName) {
        String url = constructUrl(URL_PREFIX + "/revert" + "?playName=" + playName,
                shortenCustomerSpace(customerSpace));
        // String url = constructUrl("/revert" + "?playName=" + playName + "&customerSpace=" +
        // customerSpace);
        return post("revert", url, null, List.class);
    }

    public TalkingPointDTO findByName(String customerSpace, String name) {
        String url = constructUrl(URL_PREFIX + "/" + name, shortenCustomerSpace(customerSpace));
        // String url = constructUrl("/" + name);
        return get("findByName", url, TalkingPointDTO.class);
    }

    public List<AttributeLookup> getAttributesInTalkingPointOfPlay(String customerSpace,
            String playName) {
        String url = constructUrl(URL_PREFIX + "/attributes/" + playName,
                shortenCustomerSpace(customerSpace));
        // String url = constructUrl("/attributes/" + playName);
        List<?> list = get("getAttributesInTalkingPointOfPlay", url, List.class);
        return JsonUtils.convertList(list, AttributeLookup.class);
    }
}
