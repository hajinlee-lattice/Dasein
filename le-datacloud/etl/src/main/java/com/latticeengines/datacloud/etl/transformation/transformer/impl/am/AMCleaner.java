package com.latticeengines.datacloud.etl.transformation.transformer.impl.am;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.am.AMCleaner.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CLEANER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractDataflowTransformer;
import com.latticeengines.domain.exposed.datacloud.dataflow.am.AMCleanerParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.am.AMCleanerParameters.CleanOpt;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.AMCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(TRANSFORMER_NAME)
public class AMCleaner extends AbstractDataflowTransformer<AMCleanerConfig, AMCleanerParameters> {
    public static final String TRANSFORMER_NAME = TRANSFORMER_CLEANER;
    public static final String DATAFLOW_BEAN_NAME = "AMCleanFlow";
    public static final String DATA_CLOUD_VERSION = "DataCloudVersion";
    public static final String MINI_PREFIX = "MINI_";
    public static final String ACCOUNT_MASTER = "AccountMaster";
    public static final String CLEAN = "CLEAN";

    @Inject
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    @Inject
    private DataCloudVersionService dataCloudVersionService;

    @Override
    protected String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return AMCleanerConfig.class;
    }

    @Override
    protected Class<AMCleanerParameters> getDataFlowParametersClass() {
        return AMCleanerParameters.class;
    }

    @Override
    protected void updateParameters(AMCleanerParameters parameters, Source[] baseTemplates, Source targetTemplate,
            AMCleanerConfig config, List<String> baseVersions) {
        String dataCloudVersion = null;
        String currentApprovedVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        if (StringUtils.isNotBlank(config.getDataCloudVersion())) {
            dataCloudVersion = config.getDataCloudVersion();
        } else if (!config.getIsUpdate()) {
            dataCloudVersion = dataCloudVersionService.nextMinorVersion(currentApprovedVersion);
        } else {
            dataCloudVersion = currentApprovedVersion; // current approved version
        }
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes(ACCOUNT_MASTER, CLEAN, TRANSFORMER_NAME,
                dataCloudVersion, false);
        parameters.setDataCloudVersion(dataCloudVersion);
        Map<String, CleanOpt> mapAttr = new HashMap<>();
        for (SourceAttribute srcAttr : srcAttrs) {
            mapAttr.put(srcAttr.getAttribute(), AMCleanerParameters.castCleanOpt(srcAttr.getArguments()));
        }
        parameters.setAttrOpts(mapAttr);
    }
}
