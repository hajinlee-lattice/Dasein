package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.dataflow.LatticeIdAssignFlowParameter;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.LatticeIdAssignConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("latticeIdAssignTransformer")
public class LatticeIdAssignTransformer
        extends AbstractDataflowTransformer<LatticeIdAssignConfig, LatticeIdAssignFlowParameter> {
    private static final Log log = LogFactory.getLog(LatticeIdAssignTransformer.class);

    private static String transfomerName = "latticeIdAssignTransformer";

    private static String dataFlowBeanName = "latticeIdAssignFlow";

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Override
    protected String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @Override
    public String getName() {
        return transfomerName;
    }

    @Override
    protected Class<LatticeIdAssignFlowParameter> getDataFlowParametersClass() {
        return LatticeIdAssignFlowParameter.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return LatticeIdAssignConfig.class;
    }

    @Override
    protected void updateParameters(LatticeIdAssignFlowParameter parameters, Source[] baseTemplates,
            Source targetTemplate, LatticeIdAssignConfig config) {
        parameters.setAmSeedIdField(config.getAmSeedIdField());
        parameters.setAmSeedDunsField(config.getAmSeedDunsField());
        parameters.setAmSeedDomainField(config.getAmSeedDomainField());
        parameters.setAmIdSrcIdField(config.getAmIdSrcIdField());
        parameters.setAmIdSrcDunsField(config.getAmIdSrcDunsField());
        parameters.setAmIdSrcDomainField(config.getAmIdSrcDomainField());
        // AMSeed must be put at 0th position of base source
        Long currentCount = hdfsSourceEntityMgr.count(baseTemplates[0],
                hdfsSourceEntityMgr.getCurrentVersion(baseTemplates[0]));
        if (currentCount == null || currentCount.longValue() <= 0) {
            throw new RuntimeException("Fail to get current count of AccountMasterSeed");
        }
        parameters.setCurrentCount(currentCount);
    }

    @Override
    protected boolean validateConfig(LatticeIdAssignConfig config, List<String> sourceNames) {
        if (StringUtils.isEmpty(config.getAmSeedIdField()) || StringUtils.isEmpty(config.getAmIdSrcIdField())) {
            log.error("Empty string or null is not allowed for AMSeedIdField or AMIdSrcIdField");
            return false;
        }
        if (StringUtils.isEmpty(config.getAmSeedDunsField()) || StringUtils.isEmpty(config.getAmIdSrcDunsField())) {
            log.error("Empty string or null is not allowed for AMSeedDunsField or AMIdSrcDunsField");
            return false;
        }
        if (StringUtils.isEmpty(config.getAmSeedDomainField()) || StringUtils.isEmpty(config.getAmIdSrcDomainField())) {
            log.error("Empty string or null is not allowed for AMSeedDomainField or AMIdSrcDomainField");
            return false;
        }
        return true;
    }
}
