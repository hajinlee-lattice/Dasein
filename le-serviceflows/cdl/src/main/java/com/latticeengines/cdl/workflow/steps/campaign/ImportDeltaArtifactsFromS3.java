package com.latticeengines.cdl.workflow.steps.campaign;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.ImportDeltaArtifactsFromS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.util.AttrRepoUtils;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("importDeltaArtifactsFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDeltaArtifactsFromS3 extends BaseImportExportS3<ImportDeltaArtifactsFromS3Configuration> {
    private static final Logger log = LoggerFactory.getLogger(ImportDeltaArtifactsFromS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private PlayProxy playProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();

        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace.getTenantId(), configuration.getPlayId(),
                configuration.getChannelId());

        if (StringUtils.isNotBlank(channel.getCurrentLaunchedAccountUniverseTable())
                && !channel.getResetDeltaCalculationData()) {
            log.info("Importing PreviousAccountLaunchUniverse table: "
                    + channel.getCurrentLaunchedAccountUniverseTable());
            addTableToRequestForImport(metadataProxy.getTable(customerSpace.getTenantId(),
                    channel.getCurrentLaunchedAccountUniverseTable()), requests);
            putStringValueInContext(PREVIOUS_ACCOUNTS_UNIVERSE, channel.getCurrentLaunchedAccountUniverseTable());
        }

        if (StringUtils.isNotBlank(channel.getCurrentLaunchedContactUniverseTable())
                && !channel.getResetDeltaCalculationData()) {
            log.info("Importing PreviousContactLaunchUniverse table: "
                    + channel.getCurrentLaunchedContactUniverseTable());
            addTableToRequestForImport(metadataProxy.getTable(customerSpace.getTenantId(),
                    channel.getCurrentLaunchedContactUniverseTable()), requests);
            putStringValueInContext(PREVIOUS_CONTACTS_UNIVERSE, channel.getCurrentLaunchedContactUniverseTable());
        }

        AttributeRepository attrRepo = buildAttrRepo(customerSpace);

        if (channel.getChannelConfig().getAudienceType().asBusinessEntity() == BusinessEntity.Contact) {
            try {
                AttrRepoUtils.getTablePath(attrRepo, BusinessEntity.Contact);
            } catch (QueryEvaluationException e) {
                log.error("Unable to Launch Contact based channel since no contact data exists in attribute Repo");
                throw new LedpException(LedpCode.LEDP_32000, e,
                        new String[] { "Failed to find Contact data in Attribute repo" });
            }
        }

        attrRepo.getTableNames().forEach(tblName -> {
            Table table = metadataProxy.getTable(customerSpace.toString(), tblName);
            if (table == null) {
                throw new RuntimeException("Table " + tblName + " for customer " //
                        + CustomerSpace.shortenCustomerSpace(customerSpace.toString()) //
                        + " in attr repo does not exists.");
            }
            addTableToRequestForImport(table, requests);
        });
    }

    private AttributeRepository buildAttrRepo(CustomerSpace customerSpace) {
        AttributeRepository attrRepo = dataCollectionProxy.getAttrRepo(customerSpace.toString(), //
                configuration.getVersion());
        insertAccountExport(attrRepo, customerSpace, configuration.getVersion());
        insertPurchaseHistory(attrRepo, customerSpace, configuration.getVersion());
        WorkflowStaticContext.putObject(ATTRIBUTE_REPO, attrRepo);
        return attrRepo;
    }

    private void insertAccountExport(AttributeRepository attrRepo, CustomerSpace customerSpace,
            DataCollection.Version version) {
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.AccountExport, version);
        if (table != null) {
            log.info("Insert account export table into attribute repository.");
            attrRepo.appendServingStore(BusinessEntity.Account, table);
        } else {
            log.warn("Did not find account export table in version " + version);
        }
    }

    private void insertPurchaseHistory(AttributeRepository attrRepo, CustomerSpace customerSpace,
            DataCollection.Version version) {
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.CalculatedPurchaseHistory, version);
        if (table != null) {
            log.info("Insert purchase history table into attribute repository.");
            attrRepo.appendServingStore(BusinessEntity.PurchaseHistory, table);
        } else {
            log.warn("Did not find purchase history table in version " + version);
        }
    }
}
