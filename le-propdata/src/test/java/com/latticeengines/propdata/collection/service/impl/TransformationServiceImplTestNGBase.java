package com.latticeengines.propdata.collection.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public abstract class TransformationServiceImplTestNGBase extends PropDataCollectionFunctionalTestNGBase {

    TransformationService transformationService;
    TransformationProgressEntityMgr progressEntityMgr;
    Source source;
    Collection<TransformationProgress> progresses = new HashSet<>();
    String baseSourceVersion = HdfsPathBuilder.dateFormat.format(new Date());

    abstract TransformationService getTransformationService();

    abstract TransformationProgressEntityMgr getProgressEntityMgr();

    abstract Source getSource();

    abstract String getPathToUploadBaseData();

    abstract TransformationConfiguration createTransformationConfiguration();

    @BeforeMethod(groups = "collection")
    public void setUp() throws Exception {
        source = getSource();
        prepareCleanPod(source);
        transformationService = getTransformationService();
        progressEntityMgr = getProgressEntityMgr();
    }

    @AfterMethod(groups = "collection")
    public void tearDown() throws Exception {
    }

    protected void uploadFileToHdfs(List<String> fileNames) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, getPathToUploadBaseData())) {
                HdfsUtils.rmdir(yarnConfiguration, getPathToUploadBaseData());
            }
            for (String fileName : fileNames) {
                InputStream fileStream = ClassLoader.getSystemResourceAsStream("sources/" + fileName);
                String targetPath = getPathToUploadBaseData() + "/" + fileName;
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
                InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
                String successPath = getPathToUploadBaseData() + "/" + fileName + "_SUCCESS";
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected TransformationProgress createNewProgress() {
        TransformationProgress progress = transformationService.startNewProgress(createTransformationConfiguration(),
                progressCreator);
        Assert.assertNotNull(progress, "Should have a progress in the job context.");
        Long pid = progress.getPid();
        Assert.assertNotNull(pid, "The new progress should have a pid assigned.");
        progresses.add(progress);
        return progress;
    }

    protected TransformationProgress transformData(TransformationProgress progress) {
        TransformationProgress response = transformationService.transform(progress);

        Assert.assertEquals(response.getStatus(), TransformationProgressStatus.FINISHED);

        TransformationProgress progressInDb = progressEntityMgr
                .findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), TransformationProgressStatus.FINISHED);

        return response;
    }

    protected TransformationProgress finish(TransformationProgress progress) {
        TransformationProgress response = transformationService.finish(progress);

        Assert.assertEquals(response.getStatus(), TransformationProgressStatus.FINISHED);

        TransformationProgress progressInDb = progressEntityMgr
                .findProgressByRootOperationUid(progress.getRootOperationUID());
        Assert.assertEquals(progressInDb.getStatus(), TransformationProgressStatus.FINISHED);

        return response;
    }

    protected void cleanupProgressTables() {
        for (TransformationProgress progress : progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }
}
