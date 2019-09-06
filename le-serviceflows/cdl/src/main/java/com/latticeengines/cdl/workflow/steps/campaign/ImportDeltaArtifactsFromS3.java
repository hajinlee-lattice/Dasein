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

import com.latticeengines.cdl.workflow.steps.export.ImportExtractEntityFromS3;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.ImportDeltaArtifactsFromS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("importDeltaArtifactsFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDeltaArtifactsFromS3 extends BaseImportExportS3<ImportDeltaArtifactsFromS3Configuration> {
    private static final Logger log = LoggerFactory.getLogger(ImportExtractEntityFromS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private PlayProxy playProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();

        AttributeRepository attrRepo = buildAttrRepo(customerSpace);
        attrRepo.getTableNames().forEach(tblName -> {
            Table table = metadataProxy.getTable(customerSpace.toString(), tblName);
            if (table == null) {
                throw new RuntimeException("Table " + tblName + " for customer " //
                        + CustomerSpace.shortenCustomerSpace(customerSpace.toString()) //
                        + " in attr repo does not exists.");
            }
            addTableToRequestForImport(table, requests);
        });

        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace.getTenantId(), configuration.getPlayId(),
                configuration.getChannelId());
        if (StringUtils.isNotBlank(channel.getCurrentLaunchedContactUniverseTable())) {
            addTableToRequestForImport(metadataProxy.getTable(customerSpace.getTenantId(),
                    channel.getCurrentLaunchedAccountUniverseTable()), requests);
        }

        if (StringUtils.isNotBlank(channel.getCurrentLaunchedContactUniverseTable())) {
            addTableToRequestForImport(metadataProxy.getTable(customerSpace.getTenantId(),
                    channel.getCurrentLaunchedContactUniverseTable()), requests);
        }
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
