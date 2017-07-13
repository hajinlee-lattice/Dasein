package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.etl.transformation.entitymgr.LatticeIdStrategyEntityMgr;
import com.latticeengines.domain.exposed.datacloud.dataflow.LatticeIdRefreshFlowParameter;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.LatticeIdRefreshConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("latticeIdAssignTransformer")
public class LatticeIdAssignTransformer
        extends AbstractDataflowTransformer<LatticeIdRefreshConfig, LatticeIdRefreshFlowParameter> {
    private static final Logger log = LoggerFactory.getLogger(LatticeIdAssignTransformer.class);

    @Autowired
    private LatticeIdStrategyEntityMgr latticeIdStrategyEntityMgr;

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
    protected Class<LatticeIdRefreshFlowParameter> getDataFlowParametersClass() {
        return LatticeIdRefreshFlowParameter.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return LatticeIdRefreshConfig.class;
    }

    @Override
    protected void updateParameters(LatticeIdRefreshFlowParameter parameters, Source[] baseTemplates,
            Source targetTemplate, LatticeIdRefreshConfig config, List<String> baseVersions) {
        parameters.setStrategy(latticeIdStrategyEntityMgr.getStrategyByName(config.getStrategy()));
    }

    @Override
    protected boolean validateConfig(LatticeIdRefreshConfig config, List<String> sourceNames) {
        if (StringUtils.isEmpty(config.getStrategy())) {
            log.error("Entity is not provided");
            return false;
        }
        if (CollectionUtils.isEmpty(sourceNames) || sourceNames.size() != 2) {
            log.error("Number of base sources must be 2");
            return false;
        }
        return true;
    }
}
