package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;

public abstract class FirehoseTransformationServiceImplTestNGBase<T extends TransformationConfiguration>
        extends TransformationServiceImplTestNGBase<T> {

    @Test(groups = "functional")
    public void testTransformation() {
        uploadBaseGZFile();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Test(groups = "functional", dependsOnMethods = { "testTransformation" })
    public void testTransformationWithBadData() {
        uploadBadBaseGZFile();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
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

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructRawDir(source) + "/" + baseSourceVersion;
    }
}
