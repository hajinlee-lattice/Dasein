package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class PAReportUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testAppendMessageToProductReport() {
        String warning = "No Analytic Product found";
        ObjectNode report = JsonUtils.createObjectNode();
        ObjectNode updatedReport = PAReportUtils.appendMessageToProductReport(report, warning, true);
        Assert.assertNotNull(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.PRODUCT_ID));
        Assert.assertNotNull(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey())
                .get(BusinessEntity.Product.name()).get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                .get(ReportConstants.PRODUCT_HIERARCHY));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.PRODUCT_BUNDLE));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.ERROR_MESSAGE));
        Assert.assertEquals(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey())
                .get(BusinessEntity.Product.name()).get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                .get(ReportConstants.WARN_MESSAGE).asText(), warning);

        report = JsonUtils.deserialize(
                "{\"EntitiesSummary\":{\"Transaction\":{\"ConsolidateRecordsSummary\":{\"NEW\":\"30000\",\"DELETE\":\"-30000\"},\"EntityStatsSummary\":{\"TOTAL\":\"60000\"}},\"PurchaseHistory\":{\"ConsolidateRecordsSummary\":{\"PRODUCT\":100,\"METRICS\":2}},\"Account\":{\"ConsolidateRecordsSummary\":{\"NEW\":\"0\",\"UPDATE\":\"0\",\"UNMATCH\":\"0\",\"DELETE\":\"-800\"},\"EntityStatsSummary\":{\"TOTAL\":\"800\"}},\"Contact\":{\"ConsolidateRecordsSummary\":{\"NEW\":\"0\",\"UPDATE\":\"0\",\"DELETE\":\"-2300\"},\"EntityStatsSummary\":{\"TOTAL\":\"2300\"}},\"Product\":{\"ConsolidateRecordsSummary\":{\"PRODUCT_ID\":\"0\",\"PRODUCT_HIERARCHY\":\"0\",\"PRODUCT_BUNDLE\":\"0\",\"ERROR_MESSAGE\":\"\",\"WARN_MESSAGE\":\"\"}}}}",
                ObjectNode.class);
        updatedReport = PAReportUtils.appendMessageToProductReport(report, warning, true);
        Assert.assertNotNull(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.PRODUCT_ID));
        Assert.assertNotNull(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey())
                .get(BusinessEntity.Product.name()).get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                .get(ReportConstants.PRODUCT_HIERARCHY));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.PRODUCT_BUNDLE));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.ERROR_MESSAGE));
        Assert.assertEquals(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey())
                .get(BusinessEntity.Product.name()).get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                .get(ReportConstants.WARN_MESSAGE).asText(), warning);

        String existingWarning = "Invalid Product exists";
        report = JsonUtils.deserialize(
                "{\"EntitiesSummary\":{\"Transaction\":{\"ConsolidateRecordsSummary\":{\"NEW\":\"30000\",\"DELETE\":\"-30000\"},\"EntityStatsSummary\":{\"TOTAL\":\"60000\"}},\"PurchaseHistory\":{\"ConsolidateRecordsSummary\":{\"PRODUCT\":100,\"METRICS\":2}},\"Account\":{\"ConsolidateRecordsSummary\":{\"NEW\":\"0\",\"UPDATE\":\"0\",\"UNMATCH\":\"0\",\"DELETE\":\"-800\"},\"EntityStatsSummary\":{\"TOTAL\":\"800\"}},\"Contact\":{\"ConsolidateRecordsSummary\":{\"NEW\":\"0\",\"UPDATE\":\"0\",\"DELETE\":\"-2300\"},\"EntityStatsSummary\":{\"TOTAL\":\"2300\"}},\"Product\":{\"ConsolidateRecordsSummary\":{\"PRODUCT_ID\":\"0\",\"PRODUCT_HIERARCHY\":\"0\",\"PRODUCT_BUNDLE\":\"0\",\"ERROR_MESSAGE\":\"\",\"WARN_MESSAGE\":\""
                        + existingWarning + "\"}}}}",
                ObjectNode.class);
        updatedReport = PAReportUtils.appendMessageToProductReport(report, warning, true);
        Assert.assertNotNull(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.PRODUCT_ID));
        Assert.assertNotNull(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey())
                .get(BusinessEntity.Product.name()).get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                .get(ReportConstants.PRODUCT_HIERARCHY));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.PRODUCT_BUNDLE));
        Assert.assertNotNull(
                updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey()).get(BusinessEntity.Product.name())
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.ERROR_MESSAGE));
        Assert.assertEquals(updatedReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey())
                .get(BusinessEntity.Product.name()).get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                .get(ReportConstants.WARN_MESSAGE).asText(), existingWarning + " " + warning);
    }
}
