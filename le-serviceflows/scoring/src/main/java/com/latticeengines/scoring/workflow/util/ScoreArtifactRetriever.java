package com.latticeengines.scoring.workflow.util;

import static com.latticeengines.domain.exposed.scoringapi.Model.FIT_FUNCTION_PARAMETERS_FILENAME;
import static com.latticeengines.domain.exposed.scoringapi.Model.HDFS_ENHANCEMENTS_DIR;
import static com.latticeengines.domain.exposed.scoringapi.Model.HDFS_SCORE_ARTIFACT_APPID_DIR;
import static com.latticeengines.domain.exposed.scoringapi.Model.HDFS_SCORE_ARTIFACT_BASE_DIR;
import static com.latticeengines.domain.exposed.scoringapi.Model.SCORE_DERIVATION_FILENAME;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

public class ScoreArtifactRetriever {
    private static final Logger log = LoggerFactory.getLogger(ScoreArtifactRetriever.class);

    private ModelSummaryProxy modelSummaryProxy;
    private Configuration yarnConfiguration;

    public ScoreArtifactRetriever(ModelSummaryProxy modelSummaryProxy, Configuration yarnConfiguration) {
        this.modelSummaryProxy = modelSummaryProxy;
        this.yarnConfiguration = yarnConfiguration;
    }

    private ModelSummary getModelSummary(CustomerSpace customerSpace, ModelSummaryProxy modelSummaryProxy,
            String modelId) {
        ModelSummary modelSummary = modelSummaryProxy.getModelSummaryFromModelId(customerSpace.toString(), modelId);

        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_31027, new String[] { modelId });
        }
        return modelSummary;
    }

    private AbstractMap.SimpleEntry<String, String> parseModelNameAndVersion(ModelSummary modelSummary) {
        String[] tokens = modelSummary.getLookupId().split("\\|");
        String modelName = tokens[1];
        String modelVersion = tokens[2];

        return new AbstractMap.SimpleEntry<>(modelName, modelVersion);
    }

    private String getModelAppIdSubfolder(CustomerSpace customerSpace, ModelSummary modelSummary) {
        String appId = modelSummary.getApplicationId();
        if (!StringUtils.isBlank(appId) && appId.length() > "application_".length()) {
            appId = appId.substring("application_".length());
            if (!StringUtils.isBlank(appId)) {
                log.info("Parsed appId foldername from modelsummary:" + appId);
                return appId;
            }
        }

        AbstractMap.SimpleEntry<String, String> modelNameAndVersion = parseModelNameAndVersion(modelSummary);
        String hdfsScoreArtifactAppIdDir = String.format(HDFS_SCORE_ARTIFACT_APPID_DIR, customerSpace.toString(),
                modelNameAndVersion.getKey(), modelNameAndVersion.getValue());
        try {
            List<String> folders = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsScoreArtifactAppIdDir);
            if (folders.size() == 1) {
                appId = folders.get(0).substring(folders.get(0).lastIndexOf("/") + 1);
            } else {
                throw new LedpException(LedpCode.LEDP_31007,
                        new String[] { modelSummary.getId(), JsonUtils.serialize(folders) });
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_31000, new String[] { hdfsScoreArtifactAppIdDir });
        }

        log.info("Found appId folder name by discovery:" + appId);

        return appId;
    }

    private String getScoreArtifactBaseDir(CustomerSpace customerSpace, ModelSummary modelSummary) {
        AbstractMap.SimpleEntry<String, String> modelNameAndVersion = parseModelNameAndVersion(modelSummary);
        String appId = getModelAppIdSubfolder(customerSpace, modelSummary);

        String hdfsScoreArtifactBaseDir = String.format(HDFS_SCORE_ARTIFACT_BASE_DIR, customerSpace.toString(),
                modelNameAndVersion.getKey(), modelNameAndVersion.getValue(), appId);

        return hdfsScoreArtifactBaseDir;
    }

    private String retrieveScoreDerivationFromHdfs(String hdfsScoreArtifactBaseDir) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + SCORE_DERIVATION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            // ScoreDerivation scoreDerivation = JsonUtils.deserialize(content,
            // ScoreDerivation.class);
            return content;
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
    }

    private String retrieveFitFunctionParametersFromHdfs(String hdfsScoreArtifactBaseDir) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + FIT_FUNCTION_PARAMETERS_FILENAME;
        try {
            String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            return content;
        } catch (IOException e) {
            log.warn("Cannot find fit function parameters file at " + path + ", model may be old");
            return null;
        }
    }

    public String getScoreDerivation(CustomerSpace customerSpace, //
            String modelId) {
        log.info(String.format("Retrieving score derivation from HDFS for model:%s", modelId));
        ModelSummary modelSummary = getModelSummary(customerSpace, modelSummaryProxy, modelId);

        String hdfsScoreArtifactBaseDir = getScoreArtifactBaseDir(customerSpace, modelSummary);

        return retrieveScoreDerivationFromHdfs(hdfsScoreArtifactBaseDir);
    }

    public String getFitFunctionParameters(CustomerSpace customerSpace, //
            String modelId) {
        log.info(String.format("Retrieving fit function parameters from HDFS for model:%s", modelId));
        ModelSummary modelSummary = getModelSummary(customerSpace, modelSummaryProxy, modelId);

        String hdfsScoreArtifactBaseDir = getScoreArtifactBaseDir(customerSpace, modelSummary);

        return retrieveFitFunctionParametersFromHdfs(hdfsScoreArtifactBaseDir);
    }

    public String getEVScoreDerivation(CustomerSpace customerSpace, //
            String modelId) {
        log.info(String.format("Retrieving ev model score derivation from HDFS for model:%s", modelId));
        ModelSummary modelSummary = getModelSummary(customerSpace, modelSummaryProxy, modelId);

        String hdfsScoreArtifactBaseDir = getScoreArtifactBaseDir(customerSpace, modelSummary);

        return retrieveScoreDerivationFromHdfs(hdfsScoreArtifactBaseDir);
    }

    public String getEVFitFunctionParameters(CustomerSpace customerSpace, //
            String modelId) {
        log.info(String.format("Retrieving ev model fit function parameters from HDFS for model:%s", modelId));
        ModelSummary modelSummary = getModelSummary(customerSpace, modelSummaryProxy, modelId);

        String hdfsScoreArtifactBaseDir = getScoreArtifactBaseDir(customerSpace, modelSummary);

        return retrieveFitFunctionParametersFromHdfs(hdfsScoreArtifactBaseDir);
    }
}
