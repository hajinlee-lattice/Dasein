package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.dataflow.transformation.LatticeIdAssignFlow;
import com.latticeengines.datacloud.dataflow.transformation.LatticeIdRefreshFlow;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.datacloud.etl.transformation.entitymgr.LatticeIdStrategyEntityMgr;
import com.latticeengines.domain.exposed.datacloud.dataflow.LatticeIdRefreshFlowParameter;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.LatticeIdRefreshConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(LatticeIdAssignTransformer.TRANSFORMER_NAME)
public class LatticeIdAssignTransformer
        extends AbstractDataflowTransformer<LatticeIdRefreshConfig, LatticeIdRefreshFlowParameter> {
    private static final Logger log = LoggerFactory.getLogger(LatticeIdAssignTransformer.class);

    @Autowired
    private LatticeIdStrategyEntityMgr latticeIdStrategyEntityMgr;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    public static final String TRANSFORMER_NAME = "latticeIdAssignTransformer";

    @Override
    protected String getDataFlowBeanName() {
        return LatticeIdAssignFlow.BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
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
        if (isIdSrc(baseTemplates[0], baseVersions.get(0))) {
            parameters.setIdSrcIdx(0);
            parameters.setEntitySrcIdx(1);
        } else {
            parameters.setIdSrcIdx(1);
            parameters.setEntitySrcIdx(0);
        }
    }

    private boolean isIdSrc(Source src, String version) {
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(src, version);
        if (schema == null) {
            String avroGlob = TransformerUtils.avroPath(src, version, hdfsPathBuilder);
            schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        }
        for (Field field : schema.getFields()) {
            if (field.name().equals(LatticeIdRefreshFlow.REDIRECT_FROM_FIELD)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean validateConfig(LatticeIdRefreshConfig config, List<String> sourceNames) {
        String error = null;
        if (StringUtils.isEmpty(config.getStrategy())) {
            error = "Entity is not provided";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        if (CollectionUtils.isEmpty(sourceNames) || sourceNames.size() != 2) {
            error = "Number of base sources must be 2";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }
}
