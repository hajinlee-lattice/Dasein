package com.latticeengines.proxy.exposed.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {

    public ColumnMetadataProxy() {
        super("propdata/metadata");
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ColumnMetadata> columnSelection(ColumnSelection.Predefined selectName) {
        String url = constructUrl("/predefined/{selectName}",
                String.valueOf(ColumnSelection.Predefined.LEAD_ENRICHMENT));
        List<Map<String, Object>> metadataObjs = get("columnSelection", url, List.class);
        if (metadataObjs == null) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        List<ColumnMetadata> metadataList = new ArrayList<ColumnMetadata>();
        try {
            for (Map<String, Object> obj : metadataObjs) {
                ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
                metadataList.add(metadata);
            }
        } catch (IOException e) {
            return null;
        }
        return metadataList;
    }

}
