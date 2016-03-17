package com.latticeengines.leadprioritization.dataflow;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
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
        Node last = eventTable;

        Attribute websiteColumn = eventTable.getSourceAttribute(InterfaceName.Website);
        Attribute domainColumn = eventTable.getSourceAttribute(InterfaceName.Domain);
        Attribute emailColumn = eventTable.getSourceAttribute(InterfaceName.Email);

        if (domainColumn != null) {
            last = last.rename(new FieldList(domainColumn.getName()), new FieldList(DOMAIN));
        } else if (websiteColumn != null) {
            last = DataFlowUtils.normalizeDomain(last, websiteColumn.getName(), DOMAIN);
        } else if (emailColumn != null) {
            last = DataFlowUtils.extractDomainFromEmail(last, emailColumn.getName(), DOMAIN);
            last = DataFlowUtils.normalizeDomain(last, DOMAIN);
        } else {
            throw new RuntimeException("Need a website, domain, or email column");
        }

        last = addSortColumn(last, eventTable, SORT);

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
        return last.discard(new FieldList(SORT, DOMAIN));
    }

    private Node addSortColumn(Node last, Node source, String field) {
        FieldMetadata targetField = new FieldMetadata(field, String.class);

        Attribute event = source.getSourceAttribute(InterfaceName.Event);
        String eventColumn = event.getName();

        Table sourceSchema = source.getSourceSchema();
        if (sourceSchema.getLastModifiedKey() == null) {
            return last.addFunction(String.format("%s", eventColumn), new FieldList(eventColumn), targetField);
        } else {
            long optimalCreationTime = DateTime.now().minusDays(OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY).getMillis();
            String lastModifiedField = sourceSchema.getLastModifiedKey().getAttributes().get(0);
            String expression = String.format("(%s ? 1.0 : 0.0) + (1.0 / ((double)Math.abs(%s - %dL) + 1.1))",
                    eventColumn, lastModifiedField, optimalCreationTime);
            return last.addFunction(expression, new FieldList(eventColumn, lastModifiedField), targetField);
        }
    }
}
