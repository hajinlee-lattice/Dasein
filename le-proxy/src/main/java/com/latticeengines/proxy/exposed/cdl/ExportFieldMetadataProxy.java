package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("exportFieldMetadataProxy")
public class ExportFieldMetadataProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/export-field-metadata";

    public ExportFieldMetadataProxy() {
        super("cdl");
    }

    public List<ColumnMetadata> getExportFields(String customerSpace, String playChannelId) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        url += String.format("?channelId=%s", playChannelId);
        List<?> list = get("get export fields for play launch channel", url, List.class);
        return JsonUtils.convertList(list, ColumnMetadata.class);
    }
}
