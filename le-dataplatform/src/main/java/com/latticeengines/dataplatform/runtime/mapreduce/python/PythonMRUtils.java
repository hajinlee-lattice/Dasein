package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;

public class PythonMRUtils {
    public static final String METADATA_JSON_PATH = "./metadata.json";
    private static final String DS = "datascience";

    private static final Logger log = LoggerFactory.getLogger(PythonMRUtils.class);

    public static String setupArchiveFilePath(Classifier classifier, String version) {
        List<String> paths = new ArrayList<>();
        paths.add(classifier.getPythonPipelineLibHdfsPath());
        paths.add(String.format("/%s/%s/dataplatform/scripts/leframework.tar.gz", DS, version));
        paths = getScriptPathsWithVersion(paths, version);
        return StringUtils.join(paths, ",");
    }

    public static String setupProfilingCacheFiles(Classifier classifier, String dependencyCacheFiles, String version) {
        List<String> paths = new ArrayList<String>();
        paths.add(dependencyCacheFiles);
        paths.add(classifier.getTrainingDataHdfsPath());
        return setupCacheFiles(paths, classifier, version);
    }

    public static String setupModelingCacheFiles(Classifier classifier, List<String> trainingSets,
            String dependencyCacheFiles, String version) {
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

    public static String getScriptPathWithVersion(String script, String version) {
        String afterPart = StringUtils.substringAfter(script, "/" + DS);
        String expaned = "/" + DS + "/" + version + afterPart;
        log.info(String.format("Expanded path %s to %s", script, expaned));
        return expaned;
    }

    private static List<String> getScriptPathsWithVersion(List<String> paths, String version) {
        List<String> expandedPaths = new ArrayList<>();
        paths.forEach(path -> {
            if (path.startsWith("/" + DS + "/") && !path.startsWith("/" + DS + "/" + version)) {
                expandedPaths.add(getScriptPathWithVersion(path, version));
            } else {
                expandedPaths.add(path);
            }
        });
        log.info("paths: " + expandedPaths);
        return expandedPaths;
    }

    private static String setupCacheFiles(List<String> paths, Classifier classifier, String version) {
        paths.add(String.format("/%s/%s/dataplatform/scripts/launcher.py", DS, version));
        paths.add(String.format("/%s/%s/dataplatform/scripts/pipelinefwk.py", DS, version));
        paths.add(String.format("/%s/%s/dataplatform/scripts/rulefwk.py", DS, version));
        paths.add(String.format("/%s/%s/dataplatform/scripts/pythonlauncher.sh", DS, version));
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
        paths.add(script);

        String configMetadata = classifier.getConfigMetadataHdfsPath();
        if (configMetadata.endsWith(".avsc")) {
            paths.add(configMetadata);
        }

        paths = getScriptPathsWithVersion(paths, version);
        return StringUtils.join(paths, ",");
    }

    public static void writeMetadataJsonToLocal(Classifier classifier) throws IOException {
        String metadata = JsonUtils.serialize(classifier);
        File metadataFile = new File(METADATA_JSON_PATH);
        FileUtils.writeStringToFile(metadataFile, metadata, Charset.defaultCharset());
    }

    public static void copyMetadataJsonToHdfs(Configuration config, String hdfsPath) throws Exception {
        HdfsUtils.copyLocalToHdfs(config, METADATA_JSON_PATH, hdfsPath);
    }

    public static String getRuntimeConfig(Configuration config, ProgressMonitor monitor) {
        String runtimeConfigFile = config.get(PythonContainerProperty.RUNTIME_CONFIG.name());
        if (runtimeConfigFile == null) {
            log.info("There's no run time config file specified.");
            return null;
        }
        try (FileWriter writer = new FileWriter(runtimeConfigFile)) {
            Properties runtimeConfig = new Properties();
            runtimeConfig.put("host", monitor.getHost());
            runtimeConfig.put("port", Integer.toString(monitor.getPort()));
            runtimeConfig.store(writer, null);
            log.info("Writing runtime host: " + monitor.getHost() + " port: " + monitor.getPort());
            return runtimeConfigFile;
        } catch (Exception ex) {
            log.warn("Failed to write run time config!", ex);
        }
        return null;
    }

}
