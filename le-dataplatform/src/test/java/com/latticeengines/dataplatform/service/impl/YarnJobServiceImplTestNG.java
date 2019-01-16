package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataplatformMiniClusterFunctionalTestNG;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.yarn.exposed.client.ContainerProperty;

public class YarnJobServiceImplTestNG extends DataplatformMiniClusterFunctionalTestNG {

    private String baseDir = "/functionalTests/" + suffix;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        super.setup();
        FileSystem fs = FileSystem.get(miniclusterConfiguration);

        fs.mkdirs(new Path(baseDir + "/training"));
        fs.mkdirs(new Path(baseDir + "/test"));
        fs.mkdirs(new Path(baseDir + "/datascientist1"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        URL modelUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/model.txt");

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/iris.json");
        String pythonScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.py");
        FileUtils.copyFileToDirectory(new File(modelUrl.getFile()), new File("/tmp"));
        copyEntries.add(new CopyEntry(trainingFilePath, baseDir + "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, baseDir + "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, baseDir + "/datascientist1", false));
        copyEntries.add(new CopyEntry(pythonScriptPath, baseDir + "/datascientist1", false));
        doCopy(fs, copyEntries);
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        Classifier classifier = new Classifier();
        classifier.setName("IrisClassifier");
        classifier.setFeatures(Arrays.asList("sepal_length", "sepal_width", "petal_length", "petal_width"));
        classifier.setTargets(Collections.singletonList("category"));
        classifier.setSchemaHdfsPath(baseDir + "/datascientist1/iris.json");
        classifier.setModelHdfsDir(baseDir + "/datascientist1/result");
        classifier.setPythonScriptHdfsPath(baseDir + "/datascientist1/nn_train.py");
        classifier.setTrainingDataHdfsPath(baseDir + "/training/nn_train.dat");
        classifier.setTestDataHdfsPath(baseDir + "/test/nn_test.dat");
        classifier.setDataFormat("csv");
        classifier.setDataProfileHdfsPath(baseDir + "/datascientist1/EventMetadata");
        classifier.setConfigMetadataHdfsPath(baseDir + "/datascientist1/EventMetadata");

        classifier.setPythonPipelineLibHdfsPath(manifestService.getLedsPath() //
                + "/dataplatform/scripts/lepipeline.tar.gz");
        classifier.setPythonPipelineScriptHdfsPath(manifestService.getLedsPath() + "/dataplatform/scripts/pipeline.py");
        classifier.setPipelineDriver(manifestService.getLedsPath() + "/dataplatform/scripts/pipeline.json");

        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();
        Properties containerProperties = createContainerPropertiesForYarnJob();
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());
        ApplicationId appId = testYarnJob("pythonClient", appMasterProperties, containerProperties);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }
}
