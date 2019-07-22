package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.AccountImportsMigrateWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.CDLEntityMatchMigrationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.ContactImportsMigrateWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.TransactionImportsMigrateWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.CuratedAttributesWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateAIRatingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateRatingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.MatchEntityWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CDLDataFeedImportWorkflowConfiguration.class, name = "CDLDataFeedImportWorkflowConfiguration"),
        @Type(value = CDLImportWorkflowConfiguration.class, name = "CDLImportWorkflowConfiguration"),
        @Type(value = CdlModelWorkflowConfiguration.class, name = "CdlMatchAndModelWorkflowConfiguration"),
        @Type(value = CDLOperationWorkflowConfiguration.class, name = "CDLOperationWorkflowConfiguration"),
        @Type(value = CustomEventMatchWorkflowConfiguration.class, name = "CustomEventMatchWorkflowConfiguration"),
        @Type(value = CustomEventModelingWorkflowConfiguration.class, name = "CustomEventModelingWorkflowConfiguration"),
        @Type(value = GenerateRatingWorkflowConfiguration.class, name = "GenerateRatingWorkflowConfiguration"),
        @Type(value = GenerateAIRatingWorkflowConfiguration.class, name = "GenerateAIRatingWorkflowConfiguration"),
        @Type(value = PlayLaunchWorkflowConfiguration.class, name = "PlayLaunchWorkflowConfiguration"),
        @Type(value = CampaignLaunchWorkflowConfiguration.class, name = "CampaignLaunchWorkflowConfiguration"),
        @Type(value = ProcessAnalyzeWorkflowConfiguration.class, name = "ProcessAnalyzeWorkflowConfiguration"),
        @Type(value = ProcessAccountWorkflowConfiguration.class, name = "ProcessAccountWorkflowConfiguration"),
        @Type(value = UpdateAccountWorkflowConfiguration.class, name = "UpdateAccountWorkflowConfiguration"),
        @Type(value = RebuildAccountWorkflowConfiguration.class, name = "RebuildAccountWorkflowConfiguration"),

        @Type(value = CDLEntityMatchMigrationWorkflowConfiguration.class, name = "CDLEntityMatchMigrationWorkflowConfiguration"),
        @Type(value = AccountImportsMigrateWorkflowConfiguration.class, name = "AccountImportsMigrateWorkflowConfiguration"),
        @Type(value = ContactImportsMigrateWorkflowConfiguration.class, name = "ContactImportsMigrateWorkflowConfiguration"),
        @Type(value = TransactionImportsMigrateWorkflowConfiguration.class, name = "TransactionImportsMigrateWorkflowConfiguration"),

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

        @Type(value = CuratedAttributesWorkflowConfiguration.class, name = "CuratedAttributesWorkflowConfiguration"),
        @Type(value = ProcessRatingWorkflowConfiguration.class, name = "ProcessRatingWorkflowConfiguration"),
        @Type(value = CrossSellImportMatchAndModelWorkflowConfiguration.class, name = "CrossSellImportMatchAndModelWorkflowConfiguration"),

        @Type(value = EntityExportWorkflowConfiguration.class, name = "EntityExportWorkflowConfiguration"),
        @Type(value = MatchCdlAccountWorkflowConfiguration.class, name = "MatchCdlAccountWorkflowConfiguration"),
        @Type(value = SegmentExportWorkflowConfiguration.class, name = "SegmentExportWorkflowConfiguration"),
        @Type(value = OrphanRecordsExportWorkflowConfiguration.class, name = "OrphanRecordsExportWorkflowConfiguration"),

        @Type(value = CampaignDeltaCalculationWorkflowConfiguration.class, name = "CampaignDeltaCalculationWorkflowConfiguration") })
public class BaseCDLWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.CDL.getName());
    }

}
