package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;

public abstract class FirehoseTransformationServiceImplTestNGBase extends TransformationServiceImplTestNGBase {

    // @Test(groups = "collection")
    public void testWholeProgress() {
        uploadBaseGZFile();
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

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructIngestionDir(source.getSourceName()).toString() + "/" + baseSourceVersion;
    }

}
