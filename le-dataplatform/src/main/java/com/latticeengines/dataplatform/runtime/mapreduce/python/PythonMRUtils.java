package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class PythonMRUtils {
    public static final String METADATA_JSON_PATH = "./metadata.json";

    public static String setupArchiveFilePath(Classifier classifier, String version) {
        List<String> paths = new ArrayList<String>();
        paths.add(classifier.getPythonPipelineLibHdfsPath());
        paths.add(String.format("/app/%s/dataplatform/scripts/leframework.tar.gz", version));

        return StringUtils.join(paths, ",");
    }

    public static String setupProfilingCacheFiles(Classifier classifier, String dependencyCacheFiles, String version) {
        List<String> paths = new ArrayList<String>();
        paths.add(dependencyCacheFiles);
        paths.add(classifier.getTrainingDataHdfsPath());
        return setupCacheFiles(paths, classifier, version);
    }

    public static String setupModelingCacheFiles(Classifier classifier, List<String> trainingSets, String dependencyCacheFiles, String version) {
        List<String> paths = new ArrayList<String>();
        paths.add(dependencyCacheFiles);
        paths.add(classifier.getDataProfileHdfsPath());
        // Temporary redundancy to keep launcher.py from failing
        paths.add(classifier.getTrainingDataHdfsPath());

        String trainingHdfsDir = StringUtils.substringBeforeLast(classifier.getTrainingDataHdfsPath(), "/");
        for (String trainingSet : trainingSets) {
            paths.add(trainingHdfsDir + "/" + trainingSet);
        }

        return setupCacheFiles(paths, classifier, version);
    }

    private static String setupCacheFiles(List<String> paths, Classifier classifier, String version) {
        paths.add(String.format("/app/%s/dataplatform/hadoop-metrics2.properties", version));
        paths.add(String.format("/app/%s/dataplatform/scripts/launcher.py", version));
        paths.add(String.format("/app/%s/dataplatform/scripts/pipelinefwk.py", version));
        paths.add("/datascientist/modelpredictorextraction.py");
        paths.add(classifier.getTestDataHdfsPath());
        paths.add(classifier.getSchemaHdfsPath());
        paths.add(classifier.getPythonScriptHdfsPath());
        paths.add(classifier.getPythonPipelineScriptHdfsPath());
        
        String pipelineDriver = classifier.getPipelineDriver();
        
        if (StringUtils.isEmpty(pipelineDriver)) {
            pipelineDriver = new RandomForestAlgorithm().getPipelineDriver();
        }
        paths.add(pipelineDriver);
        
        String script = new AggregationAlgorithm().getScript();
        String afterPart = StringUtils.substringAfter(script, "/app");
        script = "/app/" + version + afterPart;
        paths.add(script);

        String configMetadata = classifier.getConfigMetadataHdfsPath();
        if (configMetadata.endsWith(".avsc")) {
            paths.add(configMetadata);
        }

        return StringUtils.join(paths, ",");
    }

    public static void writeMetedataJsonToLocal(Classifier classifier) throws IOException {
        String metadata = JsonUtils.serialize(classifier);
        File metadataFile = new File(METADATA_JSON_PATH);
        FileUtils.writeStringToFile(metadataFile, metadata);
    }

    public static void copyMetedataJsonToHdfs(Configuration config, String hdfsPath) throws Exception {
        HdfsUtils.copyLocalToHdfs(config, METADATA_JSON_PATH, hdfsPath);
    }

}
