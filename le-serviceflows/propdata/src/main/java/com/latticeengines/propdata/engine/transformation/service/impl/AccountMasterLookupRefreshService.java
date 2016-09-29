package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.AccountMasterLookup;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.AccountMasterLookupConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.AccountMasterLookupInputSourceConfig;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("accountMasterLookupRefreshService")
public class AccountMasterLookupRefreshService
        extends SimpleTransformationServiceBase<AccountMasterLookupConfiguration, TransformationFlowParameters>
        implements TransformationService<AccountMasterLookupConfiguration> {

    private static final String DATA_FLOW_BEAN_NAME = "accountMasterLookupRefreshFlow";

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
    public String getDataFlowBeanName() { return DATA_FLOW_BEAN_NAME; }

    @Override
    protected TransformationFlowParameters getDataFlowParameters(TransformationProgress progress,
                                                                 AccountMasterLookupConfiguration configuration) {
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        enrichStandardDataFlowParameters(parameters, configuration, progress);
        return parameters;
    }

    @Override
    Date checkTransformationConfigurationValidity(AccountMasterLookupConfiguration conf) {
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    AccountMasterLookupConfiguration createNewConfiguration(List<String> latestBaseVersion, String newLatestVersion,
            List<SourceColumn> sourceColumns) {
        AccountMasterLookupConfiguration configuration = new AccountMasterLookupConfiguration();
        AccountMasterLookupInputSourceConfig accountMasterLookupInputSourceConfig = new AccountMasterLookupInputSourceConfig();
        accountMasterLookupInputSourceConfig.setVersion(latestBaseVersion.get(0));
        configuration.setAccountMasterLookupInputSourceConfig(accountMasterLookupInputSourceConfig);
        setAdditionalDetails(newLatestVersion, sourceColumns, configuration);
        return configuration;
    }

    @Override
    AccountMasterLookupConfiguration parseTransConfJsonInsideWorkflow(String confStr) throws IOException {
        return JsonUtils.deserialize(confStr, AccountMasterLookupConfiguration.class);
    }

    @Override
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return AccountMasterLookupConfiguration.class;
    }
}
