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

public class PythonMRUtils {
    public static final String METADATA_JSON_PATH = "./metadata.json";

    public static String setupArchiveFilePath(Classifier classifier) {
        List<String> paths = new ArrayList<String>();
        paths.add(classifier.getPythonPipelineLibHdfsPath());
        paths.add("/app/dataplatform/scripts/leframework.tar.gz");

        return StringUtils.join(paths, ",");
    }

    public static String setupProfilingCacheFiles(Classifier classifier, String dependencyCacheFiles) {
        List<String> paths = new ArrayList<String>();
        paths.add(dependencyCacheFiles);
        paths.add(classifier.getTrainingDataHdfsPath());
        return setupCacheFiles(paths, classifier);
    }

    public static String setupModelingCacheFiles(Classifier classifier, List<String> trainingSets, String dependencyCacheFiles) {
        List<String> paths = new ArrayList<String>();
        paths.add(dependencyCacheFiles);
        paths.add(classifier.getDataProfileHdfsPath());
        // Temporary redundancy to keep launcher.py from failing
        paths.add(classifier.getTrainingDataHdfsPath());

        String trainingHdfsDir = StringUtils.substringBeforeLast(classifier.getTrainingDataHdfsPath(), "/");
        for (String trainingSet : trainingSets) {
            paths.add(trainingHdfsDir + "/" + trainingSet);
        }

        return setupCacheFiles(paths, classifier);
    }

    private static String setupCacheFiles(List<String> paths, Classifier classifier) {
        paths.add("/app/dataplatform/hadoop-metrics2.properties");
        paths.add("/app/dataplatform/scripts/launcher.py");
        paths.add("/app/dataplatform/scripts/pipelinefwk.py");
        paths.add("/datascientist/modelpredictorextraction.py");
        paths.add(classifier.getTestDataHdfsPath());
        paths.add(classifier.getSchemaHdfsPath());
        paths.add(classifier.getPythonScriptHdfsPath());
        paths.add(classifier.getPythonPipelineScriptHdfsPath());
        paths.add(new AggregationAlgorithm().getScript());

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

    public static void CopyMetedataJsonToHdfs(Configuration config, String hdfsPath) throws Exception {
        HdfsUtils.copyLocalToHdfs(config, METADATA_JSON_PATH, hdfsPath);
    }

}
