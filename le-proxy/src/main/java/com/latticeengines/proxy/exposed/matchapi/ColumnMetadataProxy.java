package com.latticeengines.proxy.exposed.matchapi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.datacloud.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("columnMetadataProxyMatchapi")
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {

    public ColumnMetadataProxy() {
        super(PropertyUtils.getProperty("proxy.matchapi.rest.endpoint.hostport") + "/match/metadata");
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        String url = constructUrl("/predefined/{selectName}", String.valueOf(selectName.name()));
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url = constructUrl("/predefined/{selectName}?datacloudversion", String.valueOf(selectName.name()),
                    dataCloudVersion);
        }
        List<Map<String, Object>> metadataObjs = get("columnSelection", url, List.class);
        List<ColumnMetadata> metadataList = new ArrayList<>();
        if (metadataObjs == null) {
            return metadataList;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            for (Map<String, Object> obj : metadataObjs) {
                ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
                metadataList.add(metadata);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadataList;
    }

}
