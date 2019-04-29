package com.latticeengines.cdl.workflow.steps.export;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;


@Component("importExtractEntityFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportExtractEntityFromS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        AttributeRepository attrRepo = dataCollectionProxy.getAttrRepo(customerSpace.toString(), //
                configuration.getVersion());
        WorkflowStaticContext.putObject(ATTRIBUTE_REPO, attrRepo);
        attrRepo.getTableNames().forEach(tblName -> {
            Table table = metadataProxy.getTable(customerSpace.toString(), tblName);
            if (table == null) {
                throw new RuntimeException("Table " + tblName + " for customer "  //
                        + CustomerSpace.shortenCustomerSpace(customerSpace.toString()) //
                        + " in attr repo does not exists.");
            }
            addTableToRequestForImport(table, requests);
        });
    }

}
