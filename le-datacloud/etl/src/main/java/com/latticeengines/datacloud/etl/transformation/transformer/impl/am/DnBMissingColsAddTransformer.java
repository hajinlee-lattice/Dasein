package com.latticeengines.datacloud.etl.transformation.transformer.impl.am;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.seed.DnbMissingColsAddFromPrevFlow;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractDataflowTransformer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.DnBAddMissingColsConfig;

@Component(DnbMissingColsAddFromPrevFlow.TRANSFORMER_NAME)
public class DnBMissingColsAddTransformer
        extends AbstractDataflowTransformer<DnBAddMissingColsConfig, TransformationFlowParameters> {
    private static final Logger log = LoggerFactory.getLogger(DnBMissingColsAddTransformer.class);

    @Override
    protected String getDataFlowBeanName() {
        return DnbMissingColsAddFromPrevFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return DnbMissingColsAddFromPrevFlow.TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return DnBAddMissingColsConfig.class;
    }
    
    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir,
            TransformationFlowParameters parameters, DnBAddMissingColsConfig config) {
        Source[] baseSources = step.getBaseSources();
        List<String> versions = hdfsSourceEntityMgr.getVersions(baseSources[0]);
        if (CollectionUtils.isEmpty(versions)) {
            throw new RuntimeException(
                    "Fail to find versions for the source : " + baseSources[0].getSourceName());
        }
        Collections.sort(versions, Collections.reverseOrder());
        if (versions.size() < 2) {
            throw new RuntimeException(
                    "Fail to find previous version for the source " + baseSources[0].getSourceName()
                            + " to compare to current version to add missing columns");
        }
        String dnbVersion = versions.get(1);
        log.info("Choose DnBCacheSeed previous version : @" + dnbVersion
                + " to compare to current latest version : " + versions.get(0));
        List<String> baseVersions = step.getBaseVersions();
        baseVersions.set(0, dnbVersion);
    }

}
