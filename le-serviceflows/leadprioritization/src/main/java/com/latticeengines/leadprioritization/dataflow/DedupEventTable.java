package com.latticeengines.leadprioritization.dataflow;

import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.dataflow.util.DataFlowUtils;

@Component("dedupEventTable")
public class DedupEventTable extends TypesafeDataFlowBuilder<DedupEventTableParameters> {
    private static final String DOMAIN = "__Domain";
    private static final String SORT = "__Sort";
    public static final int OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY = 45;

    @Override
    public Node construct(DedupEventTableParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);
        Table schema = eventTable.getSourceSchema();
        Node last = eventTable;

        Attribute websiteColumn = schema.getAttribute(parameters.websiteColumn);
        // If website column is available, use that. Otherwise use email column
        if (websiteColumn != null) {
            last = DataFlowUtils.normalizeDomain(last, parameters.websiteColumn, DOMAIN);
        } else {
            last = DataFlowUtils.extractDomainFromEmail(last, parameters.emailColumn, DOMAIN);
            last = DataFlowUtils.normalizeDomain(last, DOMAIN);
        }

        last = addSortColumn(last, parameters, schema, SORT);

        Node emptyDomains = last //
                .filter(String.format("%s == null || %s.equals(\"\")", DOMAIN, DOMAIN), new FieldList(DOMAIN)) //
                .renamePipe("nullDomains");

        last = last //
                .filter(String.format("%s != null && !%s.equals(\"\")", DOMAIN, DOMAIN), new FieldList(DOMAIN)) //
                .groupByAndLimit(new FieldList(DOMAIN), //
                        new FieldList(SORT), //
                        1, //
                        true, //
                        false);
        last = last.merge(emptyDomains);
        return last.discard(new FieldList(DOMAIN, SORT));
    }

    private Node addSortColumn(Node last, DedupEventTableParameters parameters, Table sourceSchema, String field) {
        FieldMetadata targetField = new FieldMetadata(field, String.class);

        if (sourceSchema.getLastModifiedKey() == null) {
            return last.addFunction(String.format("%s", parameters.eventColumn), new FieldList(parameters.eventColumn),
                    targetField);
        } else {
            long optimalCreationTime = DateTime.now().minusDays(OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY).getMillis();
            String lastModifiedField = sourceSchema.getLastModifiedKey().getAttributes().get(0);
            String expression = String.format("(%s ? 1.0 : 0.0) + (1.0 / ((double)Math.abs(%s - %dL) + 1.1))",
                    parameters.eventColumn, lastModifiedField, optimalCreationTime);
            return last.addFunction(expression, new FieldList(parameters.eventColumn, lastModifiedField), targetField);
        }
    }
}
