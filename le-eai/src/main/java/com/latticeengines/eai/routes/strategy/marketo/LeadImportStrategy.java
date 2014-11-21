package com.latticeengines.eai.routes.strategy.marketo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;

@Component("leadImportStrategy")
public class LeadImportStrategy extends MarketoImportStrategyBase {
    private static final Log log = LogFactory.getLog(LeadImportStrategy.class);

    public LeadImportStrategy() {
        super("Marketo.Lead");
    }

    @Override
    public void importTable(ProducerTemplate template, Table table, ImportContext ctx) {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Table importTableMetadata(ProducerTemplate template, Table table, ImportContext ctx) {
        Map<String, Object> headers = getHeaders(ctx);
        Map<String, Object> result = template.requestBodyAndHeaders("direct:getLeadMetadata", null, headers, Map.class);
        List<Map<String, Object>> leadMetadata = (List<Map<String, Object>>) result.get("result") ;
        Map<String, Map<String, Object>> metadataMap = new HashMap<>();
        for (Map<String, Object> lead : leadMetadata) {
            String key = (String) ((Map) lead.get("rest")).get("name");
            metadataMap.put(key, lead);
        }
        
        for (Attribute attribute : table.getAttributes()) {
            Map<String, Object> lead = metadataMap.get(attribute.getName());
            if (lead == null) {
                log.error("No metadata for " + attribute.getName());
                continue;
            }
            attribute.setDisplayName((String) lead.get("displayName"));
            attribute.setLogicalDataType((String) lead.get("dataType"));
            attribute.setLength((Integer) lead.get("length"));
        }
        
        return super.importTableMetadata(template, table, ctx);
    }
}
