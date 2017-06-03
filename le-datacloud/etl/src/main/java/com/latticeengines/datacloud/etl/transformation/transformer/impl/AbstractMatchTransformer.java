package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

abstract class AbstractMatchTransformer extends AbstractTransformer<MatchTransformerConfig> {

    private static final Log log = LogFactory.getLog(AbstractTransformer.class);

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Override
    public boolean validateConfig(MatchTransformerConfig config, List<String> baseSources) {
        if (baseSources.size() != 1) {
            log.error("Match only one result at a time");
            return false;
        }
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return MatchTransformerConfig.class;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        Source[] baseSources = step.getBaseSources();
        List<String> baseSourceVersions = step.getBaseVersions();
        String confStr = step.getConfig();
        String sourceDirInHdfs = null;
        if (!(baseSources[0] instanceof TableSource)) {
            sourceDirInHdfs = hdfsPathBuilder.constructTransformationSourceDir(baseSources[0],
                    baseSourceVersions.get(0)).toString();
        } else {
            sourceDirInHdfs = hdfsPathBuilder.constructTablePath(baseSources[0].getSourceName(),
                    CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE), "").toString();
        }
        return match(sourceDirInHdfs, workflowDir, getConfiguration(confStr));
    }

    abstract boolean match(String inputAvroPath, String outputAvroPath, MatchTransformerConfig config);
}
