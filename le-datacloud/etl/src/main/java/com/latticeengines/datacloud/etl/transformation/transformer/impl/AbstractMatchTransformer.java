package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;

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
    protected boolean transform(TransformationProgress progress, String workflowDir, Source[] baseSources, List<String> baseSourceVersions,
                                Source[] baseTemplates, Source targetTemplate, MatchTransformerConfig config, String confStr) {

         String sourceDirInHdfs = hdfsPathBuilder.constructTransformationSourceDir(baseSources[0], baseSourceVersions.get(0)).toString();

         return match(sourceDirInHdfs, workflowDir, config);
    }

    abstract boolean match(String inputAvroPath, String outputAvroPath, MatchTransformerConfig config);
}

