package com.latticeengines.propdata.engine.transformation.service.impl;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.dataflow.BomboraDomainParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraDomain;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("bomboraDomainService")
public class BomboraDomainService
        extends SimpleTransformationServiceBase<BasicTransformationConfiguration, BomboraDomainParameters>
        implements TransformationService<BasicTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(BomboraDomainService.class);

    @Autowired
    private BomboraDomain source;

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
        return "bomboraDomainFlow";
    }

    @Override
    protected String getServiceBeanName() {
        return "bomboraDomainService";
    }

    @Override
    protected Class<BomboraDomainParameters> getDataFlowParametersClass() {
        return BomboraDomainParameters.class;
    }

    @Override
    protected BomboraDomainParameters getDataFlowParameters(TransformationProgress progress,
            BasicTransformationConfiguration transConf) {
        BomboraDomainParameters parameters;
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
        parameters.setBaseTables(Collections.singletonList(derivedSource.getBaseSources()[0].getSourceName()));
        parameters.setSourceName(derivedSource.getSourceName());
        parameters.setPrimaryKeys(Arrays.asList(getSource().getPrimaryKey()));
        Long currentRecords = hdfsSourceEntityMgr.count(derivedSource,
                hdfsSourceEntityMgr.getCurrentVersion(derivedSource));
        parameters.setCurrentRecords(currentRecords);
        log.info("Current records: " + currentRecords.toString());
        return parameters;
    }

}
