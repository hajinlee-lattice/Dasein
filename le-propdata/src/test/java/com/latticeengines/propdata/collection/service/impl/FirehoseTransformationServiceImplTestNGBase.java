package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.proxy.exposed.propdata.TransformationProxy;

public abstract class FirehoseTransformationServiceImplTestNGBase extends TransformationServiceImplTestNGBase {

    @Autowired
    TransformationProxy transformationProxy;

    @Test(groups = "collection")
    public void testTransformation() {
        uploadBaseGZFile();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        cleanupProgressTables();
    }

    @Test(groups = "collection", dependsOnMethods = { "testTransformation" })
    public void testTransformationWithBadData() {
        uploadBadBaseGZFile();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        cleanupProgressTables();
    }

    private void uploadBaseGZFile() {
        List<String> fileList = new ArrayList<>();
        fileList.add("SampleBomboraData.csv.gz");
        uploadFileToHdfs(fileList);
    }

    private void uploadBadBaseGZFile() {
        List<String> fileList = new ArrayList<>();
        fileList.add("SampleBomboraData_bad.csv.gz");
        uploadFileToHdfs(fileList);
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructIngestionDir(source.getSourceName()).toString() + "/" + baseSourceVersion;
    }

}
