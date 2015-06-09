package com.latticeengines.eai.service.impl.marketo.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.AndNode;
import com.foundationdb.sql.parser.BinaryLogicalOperatorNode;
import com.foundationdb.sql.parser.Visitable;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
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
    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        Map<String, Object> expressions = parse(filter);
        String activityDate = (String) expressions.get(ACTIVITYDATE);
        super.setupPagingToken(template, ctx, activityDate);
        
        List<String> activityTypeIds = new ArrayList<>();
        
        for (Integer activityTypeId : (List<Integer>) expressions.get(ACTIVITYTYPEID)) {
            activityTypeIds.add("activityTypeIds=" + activityTypeId);
        }
        ctx.setProperty(MarketoImportProperty.ACTIVITYTYPES, activityTypeIds);
        Map<String, Object> headers = getHeaders(ctx);
        Map<String, Object> result = template.requestBodyAndHeaders("direct:getAllLeadActivities", null, headers, Map.class);
        System.out.println(result);
    }

    @Override
    public Table importMetadata(ProducerTemplate template, Table table, ImportContext ctx) {
        return super.importMetadata(template, table, ctx);
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
