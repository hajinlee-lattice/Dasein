package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;

@Component
public class PMMLModelJsonTypeHandler extends DefaultModelJsonTypeHandler {
    @Override
    public boolean accept(String modelJsonType) {
        return PMML_MODEL.equals(modelJsonType);
    }

    @Override
    protected boolean shouldStopCheckForScoreDerivation(String path) throws IOException {
        // for PmmlModel then relax condition for score derivation file
        if (!HdfsUtils.fileExists(yarnConfiguration, path)) {
            return true;
        }
        return false;
    }

    @Override
    protected ModelEvaluator initModelEvaluator(FSDataInputStream is) {
        return new PMMLModelEvaluator(is);
    }

    @Override
    public void checkForMissingEssentialFields(boolean hasOneOfDomain, boolean hasCompanyName, boolean hasCompanyState,
            List<String> missingMatchFields) {
    }
}
