package com.latticeengines.cdl.workflow.steps;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanTxnExportParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.OrphanRecordExportConfiguration;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("OrphanRecordExportStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OrphanRecordExportStep extends RunDataFlow<OrphanRecordExportConfiguration> {

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public void onConfigurationInitialized() {
        OrphanRecordExportConfiguration configuration = getConfiguration();
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String exportId = configuration.getOrphanRecordExportId();
        configuration.setBeanName("OrphanTxnExportFlow");

        try{
            configuration.setDataFlowParams(createDataFlowParameters(configuration));
            internalResourceRestApiProxy.updateMetadataSegmentExport(customerSpace, exportId, MetadataSegmentExport.Status.COMPLETED);
        }catch (Exception ex){
            internalResourceRestApiProxy.updateMetadataSegmentExport(customerSpace, exportId, MetadataSegmentExport.Status.FAILED);
            throw new LedpException(LedpCode.LEDP_18167, ex);
        }
    }

    private DataFlowParameters createDataFlowParameters(OrphanRecordExportConfiguration configuration) {
        String txnTableName = configuration.getTxnTableName();
        String accountTableName = configuration.getAccountTableName();
        String productTableName = configuration.getProductTableName();
        OrphanTxnExportParameters parameters = new OrphanTxnExportParameters(
                accountTableName, productTableName, txnTableName
        );
        return parameters;
    }
}
