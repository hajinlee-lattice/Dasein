package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.AccountImportsMigrateWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.CDLEntityMatchMigrationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.ContactImportsMigrateWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.ConvertBatchStoreToImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.MigrateDynamoWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.PublishDynamoWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.PublishElasticSearchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.TransactionImportsMigrateWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ConvertBatchStoreToDataTableWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.CuratedAttributesWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateAIRatingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateRatingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateVisitReportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.MatchEntityWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessActivityStreamWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessCatalogWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessContactWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessProductWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessRatingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessTransactionWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildContactWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildProductWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildTransactionWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateContactWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateProductWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateTransactionWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CampaignDeltaCalculationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteAccountWorkFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteContactWorkFlowConfiguratiion;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteTransactionWorkFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertContactWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertTransactionWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CDLDataFeedImportWorkflowConfiguration.class, name = "CDLDataFeedImportWorkflowConfiguration"),
        @Type(value = CDLImportWorkflowConfiguration.class, name = "CDLImportWorkflowConfiguration"),
        @Type(value = CdlModelWorkflowConfiguration.class, name = "CdlMatchAndModelWorkflowConfiguration"),
        @Type(value = CDLOperationWorkflowConfiguration.class, name = "CDLOperationWorkflowConfiguration"),
        @Type(value = RegisterDeleteDataWorkflowConfiguration.class, name = "RegisterDeleteDataWorkflowConfiguration"),
        @Type(value = CustomEventMatchWorkflowConfiguration.class, name = "CustomEventMatchWorkflowConfiguration"),
        @Type(value = CustomEventModelingWorkflowConfiguration.class, name = "CustomEventModelingWorkflowConfiguration"),
        @Type(value = GenerateRatingWorkflowConfiguration.class, name = "GenerateRatingWorkflowConfiguration"),
        @Type(value = GenerateAIRatingWorkflowConfiguration.class, name = "GenerateAIRatingWorkflowConfiguration"),
        @Type(value = CampaignLaunchWorkflowConfiguration.class, name = "CampaignLaunchWorkflowConfiguration"),
        @Type(value = DeltaCampaignLaunchWorkflowConfiguration.class, name = "DeltaCampaignLaunchWorkflowConfiguration"),
        @Type(value = ProcessAnalyzeWorkflowConfiguration.class, name = "ProcessAnalyzeWorkflowConfiguration"),
        @Type(value = ProcessAccountWorkflowConfiguration.class, name = "ProcessAccountWorkflowConfiguration"),
        @Type(value = UpdateAccountWorkflowConfiguration.class, name = "UpdateAccountWorkflowConfiguration"),
        @Type(value = RebuildAccountWorkflowConfiguration.class, name = "RebuildAccountWorkflowConfiguration"),

        @Type(value = CDLEntityMatchMigrationWorkflowConfiguration.class, name = "CDLEntityMatchMigrationWorkflowConfiguration"),
        @Type(value = AccountImportsMigrateWorkflowConfiguration.class, name = "AccountImportsMigrateWorkflowConfiguration"),
        @Type(value = ContactImportsMigrateWorkflowConfiguration.class, name = "ContactImportsMigrateWorkflowConfiguration"),
        @Type(value = TransactionImportsMigrateWorkflowConfiguration.class, name = "TransactionImportsMigrateWorkflowConfiguration"),
        @Type(value = ConvertBatchStoreToImportWorkflowConfiguration.class, name = "ConvertBatchStoreToImportWorkflowConfiguration"),

        @Type(value = MatchEntityWorkflowConfiguration.class, name = "MatchEntityWorkflowConfiguration"),
        @Type(value = ProcessContactWorkflowConfiguration.class, name = "ProcessContactWorkflowConfiguration"),
        @Type(value = UpdateContactWorkflowConfiguration.class, name = "UpdateContactWorkflowConfiguration"),
        @Type(value = RebuildContactWorkflowConfiguration.class, name = "RebuildContactWorkflowConfiguration"),

        @Type(value = ProcessProductWorkflowConfiguration.class, name = "ProcessProductWorkflowConfiguration"),
        @Type(value = UpdateProductWorkflowConfiguration.class, name = "UpdateProductWorkflowConfiguration"),
        @Type(value = RebuildProductWorkflowConfiguration.class, name = "RebuildProductWorkflowConfiguration"),

        @Type(value = ProcessTransactionWorkflowConfiguration.class, name = "ProcessTransactionWorkflowConfiguration"),
        @Type(value = UpdateTransactionWorkflowConfiguration.class, name = "UpdateTransactionWorkflowConfiguration"),
        @Type(value = RebuildTransactionWorkflowConfiguration.class, name = "RebuildTransactionWorkflowConfiguration"),

        @Type(value = ProcessCatalogWorkflowConfiguration.class, name = "ProcessCatalogWorkflowConfiguration"),
        @Type(value = ProcessActivityStreamWorkflowConfiguration.class, name = "ProcessActivityStreamWorkflowConfiguration"),

        @Type(value = CuratedAttributesWorkflowConfiguration.class, name = "CuratedAttributesWorkflowConfiguration"),
        @Type(value = ProcessRatingWorkflowConfiguration.class, name = "ProcessRatingWorkflowConfiguration"),
        @Type(value = CrossSellImportMatchAndModelWorkflowConfiguration.class, name = "CrossSellImportMatchAndModelWorkflowConfiguration"),

        @Type(value = EntityExportWorkflowConfiguration.class, name = "EntityExportWorkflowConfiguration"),
        @Type(value = MatchCdlAccountWorkflowConfiguration.class, name = "MatchCdlAccountWorkflowConfiguration"),
        @Type(value = SegmentExportWorkflowConfiguration.class, name = "SegmentExportWorkflowConfiguration"),
        @Type(value = OrphanRecordsExportWorkflowConfiguration.class, name = "OrphanRecordsExportWorkflowConfiguration"),
        @Type(value = TimelineExportWorkflowConfiguration.class, name = "TimelineExportWorkflowConfiguration"),

        @Type(value = MockActivityStoreWorkflowConfiguration.class, name = "MockActivityStoreWorkflowConfiguration"),
        @Type(value = AtlasProfileReportWorkflowConfiguration.class, name = "AtlasProfileReportWorkflowConfiguration"),

        @Type(value = ConvertBatchStoreToDataTableWorkflowConfiguration.class, name = "ConvertBatchStoreToDataTableWorkflowConfiguration"),
        @Type(value = ConvertAccountWorkflowConfiguration.class, name = "ConvertAccountWorkflowConfiguration"),
        @Type(value = ConvertContactWorkflowConfiguration.class, name = "ConvertContactWorkflowConfiguration"),
        @Type(value = ConvertTransactionWorkflowConfiguration.class, name = "ConvertTransactionWorkflowConfiguration"),

        @Type(value = LegacyDeleteWorkflowConfiguration.class, name = "LegacyDeleteWorkflowConfiguration"),
        @Type(value = LegacyDeleteAccountWorkFlowConfiguration.class, name = "LegacyDeleteAccountWorkFlowConfiguration"),
        @Type(value = LegacyDeleteContactWorkFlowConfiguratiion.class, name = "LegacyDeleteContactWorkFlowConfiguratiion"),
        @Type(value = LegacyDeleteTransactionWorkFlowConfiguration.class, name = "LegacyDeleteTransactionWorkFlowConfiguration"),
        @Type(value = CampaignDeltaCalculationWorkflowConfiguration.class, name = "CampaignDeltaCalculationWorkflowConfiguration"),
        @Type(value = PublishDynamoWorkflowConfiguration.class, name = "PublishDynamoWorkflowConfiguration"),
        @Type(value = MigrateDynamoWorkflowConfiguration.class, name = "MigrateDynamoWorkflowConfiguration"),
        @Type(value = PublishAccountLookupWorkflowConfiguration.class, name = PublishAccountLookupWorkflowConfiguration.NAME),
        @Type(value = PublishElasticSearchWorkflowConfiguration.class, name = "PublishElasticSearchWorkflowConfiguration"),
        @Type(value = PublishAccountLookupWorkflowConfiguration.class, name = PublishAccountLookupWorkflowConfiguration.NAME),
        @Type(value = GenerateIntentEmailAlertWorkflowConfiguration.class, name = GenerateIntentEmailAlertWorkflowConfiguration.NAME),
        @Type(value = ImportListSegmentWorkflowConfiguration.class, name = "ImportListSegmentWorkflowConfiguration"),

        @Type(value = GenerateIntentEmailAlertWorkflowConfiguration.class, name = GenerateIntentEmailAlertWorkflowConfiguration.NAME),
        @Type(value = PublishVIDataWorkflowConfiguration.class, name = "PublishVIDataWorkflowConfiguration"),
        @Type(value = GenerateVisitReportWorkflowConfiguration.class, name = "GenerateVisitReportWorkflowConfiguration"),
        @Type(value = PublishTableToElasticSearchWorkflowConfiguration.class, name = "PublishTableToElasticSearchWorkflowConfiguration"),
        @Type(value = PublishActivityAlertWorkflowConfiguration.class, name = PublishActivityAlertWorkflowConfiguration.NAME),
        @Type(value = BrokerFullLoadWorkflowConfiguration.class, name = "BrokerFullLoadWorkflowConfiguration"),
        @Type(value = BrokerAggregationWorkflowConfiguration.class, name = "BrokerAggregationWorkflowConfiguration")
})
public class BaseCDLWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.CDL.getName());
    }

}
