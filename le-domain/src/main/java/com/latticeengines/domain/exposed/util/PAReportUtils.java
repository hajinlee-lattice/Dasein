package com.latticeengines.domain.exposed.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public final class PAReportUtils {

    protected PAReportUtils() {
        throw new UnsupportedOperationException();
    }
    private static final Logger log = LoggerFactory.getLogger(PAReportUtils.class);

    public static ObjectNode initEntityReport(BusinessEntity entity) {
        ObjectNode entityNode = JsonUtils.createObjectNode();

        ObjectNode consolidateSummaryNode = JsonUtils.createObjectNode();
        switch (entity) {
            case Account:
                consolidateSummaryNode.put(ReportConstants.NEW, "0");
                consolidateSummaryNode.put(ReportConstants.UPDATE, "0");
                consolidateSummaryNode.put(ReportConstants.UNMATCH, "0");
                break;
            case Contact:
                consolidateSummaryNode.put(ReportConstants.NEW, "0");
                consolidateSummaryNode.put(ReportConstants.UPDATE, "0");
                break;
            case Product:
                consolidateSummaryNode.put(ReportConstants.PRODUCT_ID, "0");
                consolidateSummaryNode.put(ReportConstants.PRODUCT_HIERARCHY, "0");
                consolidateSummaryNode.put(ReportConstants.PRODUCT_BUNDLE, "0");
                consolidateSummaryNode.put(ReportConstants.ERROR_MESSAGE, "");
                consolidateSummaryNode.put(ReportConstants.WARN_MESSAGE, "");
                break;
            case Transaction:
                consolidateSummaryNode.put(ReportConstants.NEW, "0");
                break;
            case PurchaseHistory:
                consolidateSummaryNode.set(ReportConstants.ACTIONS,
                        new ObjectMapper().createArrayNode());
                break;
            default:
                throw new UnsupportedOperationException(
                        entity.name() + " business entity is not supported in P&A report");
        }
        entityNode.set(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey(), consolidateSummaryNode);

        ObjectNode entityNumberNode = JsonUtils.createObjectNode();
        switch (entity) {
            case Product:
                entityNumberNode.put(ReportConstants.TOTAL, "Not Available");
                break;
            default:
                entityNumberNode.put(ReportConstants.TOTAL, "0");
                break;
        }
        entityNode.set(ReportPurpose.ENTITY_STATS_SUMMARY.getKey(), entityNumberNode);

        return entityNode;
    }

    public static ObjectNode appendMessageToProductReport(ObjectNode reportNode, String message,
            boolean isWarningMessage) {
        ObjectNode entitiesSummaryNode = (ObjectNode) reportNode
                .get(ReportPurpose.ENTITIES_SUMMARY.getKey());
        if (entitiesSummaryNode == null) {
            log.info("No entity summary reports found. Create it.");
            entitiesSummaryNode = reportNode.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
        }
        ObjectNode entityNode = entitiesSummaryNode.get(BusinessEntity.Product.name()) != null
                ? (ObjectNode) entitiesSummaryNode.get(BusinessEntity.Product.name())
                : PAReportUtils.initEntityReport(BusinessEntity.Product);
        entitiesSummaryNode.set(BusinessEntity.Product.name(), entityNode);

        ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
        String nodeKey = isWarningMessage ? ReportConstants.WARN_MESSAGE
                : ReportConstants.ERROR_MESSAGE;
        JsonNode messageNode = consolidateSummaryNode.get(nodeKey);
        if (StringUtils.isNotBlank(messageNode.asText())) {
            message = messageNode.asText() + " " + message;
        }
        consolidateSummaryNode.put(nodeKey, message);

        return reportNode;
    }
}
