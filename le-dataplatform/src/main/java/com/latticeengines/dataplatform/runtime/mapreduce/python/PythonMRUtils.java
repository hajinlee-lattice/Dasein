package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class PythonMRUtils {
    public static final String METADATA_JSON_PATH = "./metadata.json";
    
    private static final Log log = LogFactory.getLog(PythonMRUtils.class);
    
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

    private static String getScriptPathWithVersion(String script, String version) {
        String afterPart = StringUtils.substringAfter(script, "/app");
        return "/app/" + version + afterPart;
    }

    private static String setupCacheFiles(List<String> paths, Classifier classifier, String version) {
        paths.add(String.format("/app/%s/conf/latticeengines.properties", version));
        paths.add(String.format("/app/%s/dataplatform/scripts/launcher.py", version));
        paths.add(String.format("/app/%s/dataplatform/scripts/pipelinefwk.py", version));
        paths.add(String.format("/app/%s/dataplatform/scripts/rulefwk.py", version));
        paths.add(String.format("/app/%s/dataplatform/scripts/pythonlauncher.sh", version));
        paths.add(classifier.getTestDataHdfsPath());
        paths.add(classifier.getSchemaHdfsPath());
        paths.add(classifier.getPythonScriptHdfsPath());
        paths.add(classifier.getPythonPipelineScriptHdfsPath());

        String pipelineDriver = classifier.getPipelineDriver();

        if (StringUtils.isEmpty(pipelineDriver)) {
            pipelineDriver = new RandomForestAlgorithm().getPipelineDriver();
            pipelineDriver = getScriptPathWithVersion(pipelineDriver, version);
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

    public static void writeMetadataJsonToLocal(Classifier classifier) throws IOException {
        String metadata = JsonUtils.serialize(classifier);
        File metadataFile = new File(METADATA_JSON_PATH);
        FileUtils.writeStringToFile(metadataFile, metadata);
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
