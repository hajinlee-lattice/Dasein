package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.proxy.exposed.propdata.TransformationProxy;

public abstract class FirehoseTransformationDeploymentTestNGBase extends TransformationServiceImplTestNGBase {

    @Autowired
    TransformationProxy transformationProxy;

    @Test(groups = "deployment")
    public void testDeploymentWholeProgress() {
        try {
            cleanupActiveFromProgressTables();
            uploadBaseGZFile();
            List<TransformationProgress> transformationProgressList = transformationProxy
                    .scan("FunctionalBomboraFirehose");
            Assert.assertNotNull(transformationProgressList);
            Assert.assertTrue(transformationProgressList.size() > 0);
            Assert.assertNotNull(transformationProgressList.get(0).getYarnAppId());

            finish(transformationProgressList.get(0));
        } finally {
            cleanupProgressTables();
        }
    }

    private void uploadBaseGZFile() {
        List<String> fileList = new ArrayList<>();
        fileList.add("SampleBomboraData.csv.gz");
        uploadFileToHdfs(fileList);
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructIngestionDir(source.getSourceName()).toString() + "/" + baseSourceVersion;
    }

}
