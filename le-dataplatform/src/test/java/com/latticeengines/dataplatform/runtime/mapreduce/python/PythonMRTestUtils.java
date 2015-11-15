package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.Classifier;

public class PythonMRTestUtils {

    public static Classifier setupDummyClassifier() {
        Classifier classifier = new Classifier();
        classifier.setName("classifier");
        classifier.setModelHdfsDir("modelPath");
        List<String> feature = new ArrayList<String>();
        feature.add("feature1");
        feature.add("feature2");
        classifier.setFeatures(feature);
        classifier.setTargets(new ArrayList<String>());
        classifier.setKeyCols(new ArrayList<String>());
        classifier.setPythonScriptHdfsPath("pythonScriptPath");

        classifier.setPythonPipelineLibHdfsPath("pipelineLibScriptPath");
        classifier.setPythonPipelineScriptHdfsPath("pipelineScriptPath");
        classifier.setDataFormat("avro");
        classifier.setAlgorithmProperties("algorithmProperties");
        classifier.setProvenanceProperties("provenanceProperties");
        classifier.setDataProfileHdfsPath("profilePath");
        classifier.setConfigMetadataHdfsPath("configMetadataPath");
        classifier.setDataDiagnosticsPath("dataDiagnosticsPath");

        classifier.setTrainingDataHdfsPath("trainingPath");
        classifier.setTestDataHdfsPath("testPath");
        classifier.setSchemaHdfsPath("schemaPath");
        return classifier;
    }

    public static Classifier readClassifier(String localDir, String filename) {
        Classifier classifier = null;
        String localPath = ClassLoader.getSystemResource(localDir).getPath();

        try {
            String metadata = FileUtils.readFileToString(new File(localPath + "/" + filename));
            classifier = JsonUtils.deserialize(metadata, Classifier.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return classifier;
    }
}
