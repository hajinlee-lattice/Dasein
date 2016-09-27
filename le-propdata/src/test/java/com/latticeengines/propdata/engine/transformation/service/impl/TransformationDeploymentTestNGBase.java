package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationRequest;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.testframework.PropDataEngineDeploymentTestNGBase;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public abstract class TransformationDeploymentTestNGBase<T extends TransformationConfiguration>
        extends PropDataEngineDeploymentTestNGBase {

    private static final int MAX_LOOPS = 100;
    private static final Log log = LogFactory.getLog(TransformationDeploymentTestNGBase.class);

    @Autowired
    TransformationProgressEntityMgr progressEntityMgr;

    Source source;
    TransformationService<T> transformationService;
    Collection<TransformationProgress> progresses = new HashSet<>();
    String baseSourceVersion = HdfsPathBuilder.dateFormat.format(new Date());

    protected abstract TransformationService<T> getTransformationService();

    protected abstract String getTransformationServiceBeanName();

    protected abstract Source getSource();

    protected abstract String getPathToUploadBaseData();

    protected abstract String getPathForResult();

    @BeforeMethod(groups = { "deployment" })
    public void setUp() throws Exception {
        source = getSource();
        prepareCleanPod(source.getSourceName());
        transformationService = getTransformationService();
    }

    protected void cleanupActiveFromProgressTables() {
        TransformationProgress progress = progressEntityMgr.findRunningProgress(getSource());
        if (progress != null) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

    protected TransformationRequest getTransformationRequest() {
        TransformationRequest request = new TransformationRequest();
        request.setSourceBeanName(getTransformationServiceBeanName());
        request.setSubmitter(progressCreator);
        return request;
    }

    protected TransformationProgress finish(TransformationProgress progress) {
        TransformationProgress progressInDb = null;
        for (int i = 0; i < MAX_LOOPS; i++) {
            progressInDb = progressEntityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
            Assert.assertNotNull(progressInDb);
            if (progressInDb.getStatus().equals(ProgressStatus.FINISHED)) {
                break;
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Assert.assertEquals(progressInDb.getStatus(), ProgressStatus.FINISHED);

        return progressInDb;
    }

    protected void confirmResultFile(TransformationProgress progress) {
        String path = getPathForResult();
        log.info("Checking for result file: " + path);
        List<String> files;
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(files.size() >= 2);
        for (String file : files) {
            if (!file.endsWith(SUCCESS_FLAG)) {
                Assert.assertTrue(file.endsWith(".avro"));
                continue;
            }
            Assert.assertTrue(file.endsWith(SUCCESS_FLAG));
        }
    }

    protected void uploadFileToHdfs(List<String> fileNames) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, getPathToUploadBaseData())) {
                HdfsUtils.rmdir(yarnConfiguration, getPathToUploadBaseData());
            }
            for (String fileName : fileNames) {
                log.info("Uploading file " + fileName + " to hdfs folder " + getPathToUploadBaseData());
                InputStream fileStream = ClassLoader.getSystemResourceAsStream("sources/" + fileName);
                String targetPath = getPathToUploadBaseData() + "/" + fileName;
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
                InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
                String successPath = getPathToUploadBaseData() + SUCCESS_FLAG;
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void cleanupProgressTables() {
        for (TransformationProgress progress : progresses) {
            progressEntityMgr.deleteProgressByRootOperationUid(progress.getRootOperationUID());
        }
    }

}
