package com.latticeengines.eai.service.impl.marketo.strategy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.AndNode;
import com.foundationdb.sql.parser.BinaryLogicalOperatorNode;
import com.foundationdb.sql.parser.Visitable;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;

@Component("activityImportStrategy")
public class ActivityImportStrategy extends MarketoImportStrategyBase {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ActivityImportStrategy.class);
    private static final String ACTIVITYDATE = "activitydate";
    private static final String ACTIVITYTYPEID = "activitytypeid";

    public ActivityImportStrategy() {
        super("Marketo.Activity");
    }
    
    @SuppressWarnings("unchecked")
    private List<String> getActivityTypeIds(String filter, boolean withColumnName) {
        Map<String, Object> expressions = parse(filter);
        List<String> activityTypeIds = new ArrayList<>();
        for (Integer activityTypeId : (List<Integer>) expressions.get(ACTIVITYTYPEID)) {
            if (withColumnName) {
                activityTypeIds.add("activityTypeIds=" + activityTypeId);
            } else {
                activityTypeIds.add(activityTypeId.toString());
            }
            
        }
        return activityTypeIds;
    }

    
    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        Map<String, Object> expressions = parse(filter);
        String activityDate = (String) expressions.get(ACTIVITYDATE);
        super.setupPagingToken(template, ctx, activityDate);
        
        List<String> activityTypeIds = getActivityTypeIds(filter, true);
        
        ctx.setProperty(MarketoImportProperty.ACTIVITYTYPES, activityTypeIds);
        Map<String, Object> headers = getHeaders(ctx, table);
        template.sendBodyAndHeaders("direct:getAllLeadActivities", null, headers);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Table importMetadata(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        Set<String> activityTypeIds = new HashSet<>(getActivityTypeIds(filter, false));
        ctx.setProperty(MarketoImportProperty.DOIMPORT, false);
        Map<String, Object> headers = getHeaders(ctx);
        
        Map<String, Object> activityTypes = template.requestBodyAndHeaders("direct:getActivityTypes", null, headers, Map.class);
        
        List<Map<String, Object>> activityTypesFromResult = (List) activityTypes.get("result");
        
        Set<Attribute> newAttributes = new HashSet<>();
        for (Map<String, Object> m : activityTypesFromResult) {
            String id = (String) m.get("id").toString();
            
            if (!activityTypeIds.contains(id)) {
                continue;
            }
            List<Map<String, String>> attributes = (List) m.get("attributes");
            
            for (Map<String, String> attribute : attributes) {
                String name = attribute.get("name");
                String dataType = attribute.get("dataType");
                Attribute attr = new Attribute();
                attr.setName(name.replace(" ", "_"));
                attr.setDisplayName(name);
                attr.setLogicalDataType(dataType);
                newAttributes.add(attr);
            }
        }
        
        for (Attribute newAttribute : newAttributes) {
            table.addAttribute(newAttribute);
        }
        
        return super.importMetadata(template, table, filter, ctx);
    }
    
    @Override
    public ExpressionParserVisitorBase getParser() {
        return new ActivityParserVisitor();
    }
    
    protected static class ActivityParserVisitor extends ExpressionParserVisitorBase {
        @Override
        public Visitable visit(Visitable parseTreeNode) throws StandardException {
            if (parseTreeNode instanceof BinaryLogicalOperatorNode &&
                    !(parseTreeNode instanceof AndNode)) {
                throw new UnsupportedOperationException("Only AND is supported for Marketo activity.");
            }
            return super.visit(parseTreeNode);
        }
        
    }
    
}
