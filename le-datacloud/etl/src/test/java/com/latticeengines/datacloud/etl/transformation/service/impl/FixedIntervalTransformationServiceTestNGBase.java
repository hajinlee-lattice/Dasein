package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.FixedIntervalSource;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;

public abstract class FixedIntervalTransformationServiceTestNGBase<T extends TransformationConfiguration>
        extends TransformationServiceImplTestNGBase<T> {

    @Test(groups = "pipeline2")
    public void testWholeProgress() {
        uploadBaseAvroFile();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void uploadBaseAvroFile() {
        List<String> fileList = new ArrayList<>();
        fileList.add("SampleBomboraData.avro");
        uploadFileToHdfs(fileList);
    }

    @Override
    protected String getPathToUploadBaseData() {
        FixedIntervalSource sourceFixedInterval = (FixedIntervalSource) getSource();
        String path = hdfsPathBuilder
                .constructSnapshotDir(sourceFixedInterval.getBaseSources()[0].getSourceName(), baseSourceVersion)
                .toString();
        return path.replace("/Snapshot/", "/Raw/");
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotRootDir(source.getSourceName()) + "/" + baseSourceVersion;
    }
}
