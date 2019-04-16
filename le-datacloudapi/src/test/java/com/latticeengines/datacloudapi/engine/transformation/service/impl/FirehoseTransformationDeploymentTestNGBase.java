package com.latticeengines.datacloudapi.engine.transformation.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;

public abstract class FirehoseTransformationDeploymentTestNGBase<T extends TransformationConfiguration>
        extends TransformationDeploymentTestNGBase<T> {

    @Autowired
    TransformationProxy transformationProxy;

    @Test(groups = "deployment")
    public void testDeploymentWholeProgress() {
        try {
            cleanupActiveFromProgressTables();
            uploadBaseGZFile();
            TransformationProgress progress = transformationProxy.transform(getTransformationRequest(), podId);
            Assert.assertNotNull(progress);
            Assert.assertNotNull(progress.getYarnAppId());
            finish(progress);
            confirmResultFile(progress);
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

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructRawDir(source) + "/" + baseSourceVersion;
    }

}
