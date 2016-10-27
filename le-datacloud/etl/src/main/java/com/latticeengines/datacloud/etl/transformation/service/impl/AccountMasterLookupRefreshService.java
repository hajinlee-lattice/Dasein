package com.latticeengines.datacloud.etl.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterLookup;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterLookupConfiguration;

@Component("accountMasterLookupRefreshService")
public class AccountMasterLookupRefreshService
        extends SimpleTransformationServiceBase<AccountMasterLookupConfiguration, TransformationFlowParameters>
        implements TransformationService<AccountMasterLookupConfiguration> {

    private static final Log log = LogFactory.getLog(AccountMasterLookupRefreshService.class);

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
        return "accountMasterLookupRefreshFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "accountMasterLookupRefreshService";
    }

    @Override
    public Class<AccountMasterLookupConfiguration> getConfigurationClass() {
        return AccountMasterLookupConfiguration.class;
    }
}
