package com.latticeengines.datacloud.dataflow.transformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.BomboraDomainFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.BomboraDomainParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("bomboraDomainFlow")
public class BomboraDomainFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, BomboraDomainParameters> {

    private static ObjectMapper objectMapper = new ObjectMapper();
    private String domain;
    private String id;
    private String bomboraDepivotedDomain;
    private String timestamp;

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(BomboraDomainParameters parameters) {
        init(parameters.getColumns());

        Node currentBomboraDomain = addSource(parameters.getSourceName());
        Node bomboraDepivoted = addSource(parameters.getBaseTables().get(0));
        
        bomboraDepivoted = bomboraDepivoted.groupByAndLimit(new FieldList(bomboraDepivotedDomain),
                new FieldList(bomboraDepivotedDomain), 1, true, true);
        bomboraDepivoted = bomboraDepivoted.retain(new FieldList(bomboraDepivotedDomain));
        bomboraDepivoted = bomboraDepivoted.rename(new FieldList(bomboraDepivotedDomain), new FieldList("Daily_" + bomboraDepivotedDomain));

        Node joined = currentBomboraDomain.join(new FieldList(domain), bomboraDepivoted, new FieldList("Daily_" + bomboraDepivotedDomain),
                JoinType.OUTER);
        
        Node existingDomainsUnmatched = joined.filter(id + " != null && " + bomboraDepivotedDomain + " == null",
                new FieldList(id, bomboraDepivotedDomain));
        existingDomainsUnmatched = existingDomainsUnmatched.retain(new FieldList(id, domain, timestamp));

        Node existingDomainsMatched = joined.filter(id + " != null && " + bomboraDepivotedDomain + " != null",
                new FieldList(id, bomboraDepivotedDomain));
        existingDomainsMatched = existingDomainsMatched.retain(new FieldList(id, domain));
        existingDomainsMatched = existingDomainsMatched.addTimestamp(timestamp, parameters.getTimestamp());

        Node newDomains = joined.filter(id + " == null", new FieldList(id));
        newDomains = newDomains.addRowID("ROWID");
        newDomains = newDomains.apply("Long.valueOf(ROWID) + " + parameters.getCurrentRecords().toString(),
                new FieldList("ROWID"), new FieldMetadata("ROWID", Long.class));
        String[] copyFrom = { "ROWID", "Daily_" + bomboraDepivotedDomain };
        String[] copyTo = {"NEW_ID", "NEW_Domain"};
        List<FieldMetadata> fms = new ArrayList<FieldMetadata>();
        fms.add(new FieldMetadata("NEW_ID", Long.class));
        fms.add(new FieldMetadata("NEW_Domain", String.class));
        newDomains = newDomains.apply(new BomboraDomainFunction(copyTo, copyFrom),
                new FieldList("ROWID", "Daily_" + bomboraDepivotedDomain), fms, new FieldList("NEW_ID", "NEW_Domain"));
        newDomains = newDomains.retain(new FieldList("NEW_ID", "NEW_Domain"));
        newDomains = newDomains.rename(new FieldList("NEW_ID", "NEW_Domain"), new FieldList(id, domain));
        newDomains = newDomains.addTimestamp(timestamp, parameters.getTimestamp());

        return newDomains.merge(existingDomainsUnmatched).merge(existingDomainsMatched);
    }

    private void init(List<SourceColumn> sourceColumns) {
        for (SourceColumn sourceColumn : sourceColumns) {
            JsonNode jsonNode;
            try {
                jsonNode = objectMapper.readTree(sourceColumn.getArguments());
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to parse arguments to json for column " + sourceColumn.getColumnName(), e);
            }
            domain = jsonNode.get("Domain").asText();
            id = jsonNode.get("ID").asText();
            bomboraDepivotedDomain = jsonNode.get("BomboraDepivotedDomain").asText();
            if (sourceColumn.getCalculation() == Calculation.ADD_TIMESTAMP) {
                timestamp = sourceColumn.getColumnName();
            }
        }
    }

}
