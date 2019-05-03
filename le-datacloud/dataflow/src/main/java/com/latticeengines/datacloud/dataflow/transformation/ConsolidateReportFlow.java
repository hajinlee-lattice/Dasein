package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateReportParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;

@Component(ConsolidateReportFlow.DATAFLOW_BEAN_NAME)
public class ConsolidateReportFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, ConsolidateReportParameters> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateReportFlow.class);

    public static final String TRANSFORMER_NAME = "ConsolidateReporter";
    public static final String DATAFLOW_BEAN_NAME = "ConsolidateReportFlow";

    public static final String REPORT_TOPIC = "Topic";
    public static final String REPORT_CONTENT = "Content";
    public static final String REPORT_TOPIC_TOTAL = ReportConstants.TOTAL;
    public static final String REPORT_TOPIC_NEW = ReportConstants.NEW;
    public static final String REPORT_TOPIC_UPDATE = ReportConstants.UPDATE;
    public static final String REPORT_TOPIC_UNMATCH = ReportConstants.UNMATCH;
    public static final String REPORT_TOPIC_MATCH = "MATCH";

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(ConsolidateReportParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        List<Node> reports = new ArrayList<>();
        switch (parameters.getEntity()) {
        case Account:
            Node totalReport = reportTotal(source);
            Node newReport = reportNew(source, parameters);
            Node updateReport = reportUpdate(totalReport, newReport);
            Node unMatchReport = reportUnmatch(source);
            reports.addAll(Arrays.asList(newReport, updateReport, unMatchReport));
            break;
        case Contact:
            totalReport = reportTotal(source);
            newReport = reportNew(source, parameters);
            updateReport = reportUpdate(totalReport, newReport);
            reports.addAll(Arrays.asList(newReport, updateReport));
            if (parameters.getBaseTables().size() > 1) {
                Node matchReport = reportMatchAccount(source, addSource(parameters.getBaseTables().get(1)));
                reports.add(matchReport);
            }
            break;
        case Product:
            totalReport = reportTotal(source);
            newReport = reportNew(source, parameters);
            updateReport = reportUpdate(totalReport, newReport);
            reports.addAll(Arrays.asList(newReport, updateReport));
            break;
        case Transaction:
            totalReport = reportTotal(source);
            totalReport = totalReport.discard(REPORT_TOPIC).addColumnWithFixedValue(REPORT_TOPIC, REPORT_TOPIC_NEW,
                    String.class); // All the rows are NEW for current consolidation of transaction
            reports.add(totalReport);
            break;
        default:
            throw new UnsupportedOperationException(
                    parameters.getEntity() + " is not supported in ConsolidateReportFlow yet");
        }
        return merge(reports);
    }

    private Node reportMatchAccount(Node contact, Node account) {
        contact = contact.filter(String.format("%s != null", InterfaceName.AccountId.name()),
                new FieldList(InterfaceName.AccountId.name()));
        contact = contact.innerJoin(InterfaceName.AccountId.name(), account, InterfaceName.AccountId.name());
        contact = contact.count("__MATCH_COUNT__")
                .rename(new FieldList("__MATCH_COUNT__"), new FieldList(REPORT_CONTENT))
                .addColumnWithFixedValue(REPORT_TOPIC, REPORT_TOPIC_MATCH, String.class).renamePipe("MatchReport");
        return contact;
    }

    private Node reportTotal(Node node) {
        node = node.count("__TOTAL_COUNT__").rename(new FieldList("__TOTAL_COUNT__"), new FieldList(REPORT_CONTENT))
                .addColumnWithFixedValue(REPORT_TOPIC, REPORT_TOPIC_TOTAL, String.class).renamePipe("TotalReport");
        return node;
    }

    private Node reportNew(Node node, ConsolidateReportParameters parameters) {
        long thresholdTime = parameters.getThresholdTime() == null ? //
                parameters.getTimestamp().getTime() : parameters.getThresholdTime();
        log.info(String.format("Entity with %s >= %d is treated as new entity", InterfaceName.CDLCreatedTime.name(),
                thresholdTime));
        node = node.filter(
                String.format("%s != null && %s >= %dL", InterfaceName.CDLCreatedTime.name(),
                        InterfaceName.CDLCreatedTime.name(), thresholdTime),
                new FieldList(InterfaceName.CDLCreatedTime.name()));
        node = node.count("__NEW_COUNT__").rename(new FieldList("__NEW_COUNT__"), new FieldList(REPORT_CONTENT))
                .addColumnWithFixedValue(REPORT_TOPIC, REPORT_TOPIC_NEW, String.class).renamePipe("NewReport");
        return node;
    }

    private Node reportUpdate(Node totalReport, Node newReport) {
        totalReport = totalReport.rename(new FieldList(REPORT_CONTENT), new FieldList("TotalCount"))
                .retain("TotalCount");
        newReport = newReport.rename(new FieldList(REPORT_CONTENT), new FieldList("NewCount")).retain("NewCount");
        Node updateReport = totalReport.combine(newReport)
                .apply(String.format("%s - (%s == null ? Long.valueOf(0) : %s)", "TotalCount", "NewCount", "NewCount"),
                        new FieldList("TotalCount", "NewCount"), new FieldMetadata(REPORT_CONTENT, Long.class))
                .retain(REPORT_CONTENT).addColumnWithFixedValue(REPORT_TOPIC, REPORT_TOPIC_UPDATE, String.class)
                .renamePipe("UpdateReport");
        return updateReport;
    }

    private Node reportUnmatch(Node node) {
        node = node.filter(String.format("%s == null", InterfaceName.LatticeAccountId.name()),
                new FieldList(InterfaceName.LatticeAccountId.name()));
        node = node.count("__UNMATCH_COUNT__").rename(new FieldList("__UNMATCH_COUNT__"), new FieldList(REPORT_CONTENT))
                .addColumnWithFixedValue(REPORT_TOPIC, REPORT_TOPIC_UNMATCH, String.class).renamePipe("UnMatchReport");
        return node;
    }

    private Node merge(List<Node> nodes) {
        List<Node> formatted = new ArrayList<>();
        nodes.forEach(node -> formatted.add(formatContent(node)));
        Node toReturn = formatted.get(0);
        formatted.remove(0);
        if (!formatted.isEmpty()) {
            toReturn = toReturn.merge(formatted);
        }
        return toReturn;
    }

    private Node formatContent(Node node) {
        return node.apply(String.format("String.valueOf(%s)", REPORT_CONTENT), new FieldList(REPORT_CONTENT),
                new FieldMetadata(REPORT_CONTENT, String.class));
    }
}
