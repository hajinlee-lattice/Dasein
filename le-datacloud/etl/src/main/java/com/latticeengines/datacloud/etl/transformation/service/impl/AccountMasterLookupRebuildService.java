package com.latticeengines.datacloud.etl.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterLookup;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

@Component("accountMasterLookupRebuildService")
public class AccountMasterLookupRebuildService
        extends SimpleTransformationServiceBase<BasicTransformationConfiguration, TransformationFlowParameters>
        implements TransformationService<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(AccountMasterLookupRebuildService.class);

    @Autowired
    private AccountMasterLookup source;

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    public String getDataFlowBeanName() {
        return "accountMasterLookupRebuildFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "accountMasterLookupRebuildService";
    }

    @Override
    public Class<BasicTransformationConfiguration> getConfigurationClass() {
        return BasicTransformationConfiguration.class;
    }
}
