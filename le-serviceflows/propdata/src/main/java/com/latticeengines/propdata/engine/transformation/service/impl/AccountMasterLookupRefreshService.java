package com.latticeengines.propdata.engine.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.AccountMasterLookup;
import com.latticeengines.propdata.engine.transformation.configuration.impl.AccountMasterLookupConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

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
