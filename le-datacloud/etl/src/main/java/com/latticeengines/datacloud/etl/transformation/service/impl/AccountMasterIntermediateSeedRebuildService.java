package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterIntermediateSeed;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterIntermediateSeedParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("accountMasterIntermediateSeedRebuildService")
public class AccountMasterIntermediateSeedRebuildService extends
        SimpleTransformationServiceBase<BasicTransformationConfiguration, AccountMasterIntermediateSeedParameters>
        implements TransformationService<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(AccountMasterIntermediateSeedRebuildService.class);

    @Autowired
    private AccountMasterIntermediateSeed source;

    @Autowired
    private CountryCodeService countryCodeService;

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    protected Log getLogger() {
        return log;
    }

    protected Class<AccountMasterIntermediateSeedParameters> getDataFlowParametersClass() {
        return AccountMasterIntermediateSeedParameters.class;
    }

    @Override
    protected String getDataFlowBeanName() {
        return "accountMasterIntermediateSeedRebuildFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "accountMasterIntermediateSeedRebuildService";
    }

    @Override
    public List<String> findUnprocessedBaseVersions() {
        return Collections.emptyList();
    }

    @Override
    protected AccountMasterIntermediateSeedParameters getDataFlowParameters(TransformationProgress progress,
            BasicTransformationConfiguration transConf) {
        AccountMasterIntermediateSeedParameters parameters;
        try {
            parameters = getDataFlowParametersClass().newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException("Failed construct a new progress object by empty constructor", e);
        }

        parameters.setTimestampField(getSource().getTimestampField());
        try {
            parameters.setTimestamp(HdfsPathBuilder.dateFormat.parse(progress.getVersion()));
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25012, e,
                    new String[] { getSource().getSourceName(), e.getMessage() });
        }
        parameters.setColumns(sourceColumnEntityMgr.getSourceColumns(getSource().getSourceName()));

        DerivedSource derivedSource = (DerivedSource) getSource();
        if (derivedSource.getBaseSources().length == 1) {
            parameters.setBaseTables(Collections.singletonList(derivedSource.getBaseSources()[0].getSourceName()));
        } else {
            List<String> baseTables = new ArrayList<String>();
            for (Source baseSource : derivedSource.getBaseSources()) {
                baseTables.add(baseSource.getSourceName());
            }
            parameters.setBaseTables(baseTables);
        }
        parameters.setPrimaryKeys(Arrays.asList(getSource().getPrimaryKey()));
        parameters.setStandardCountries(countryCodeService.getStandardCountries());
        return parameters;
    }
}