package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

@Component("accountMasterSeedRebuildService")
public class AccountMasterSeedRebuildService
        extends SimpleTransformationServiceBase<BasicTransformationConfiguration, TransformationFlowParameters>
        implements TransformationService<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(AccountMasterSeedRebuildService.class);

    @Autowired
    private AccountMasterSeed source;

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    @Override
    protected String getDataFlowBeanName() {
        return "accountMasterSeedRebuildFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "accountMasterSeedRebuildService";
    }

    @Override
    public List<String> findUnprocessedBaseVersions() {
        return Collections.emptyList();
    }
}
