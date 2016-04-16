package com.latticeengines.propdata.engine.transformation;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.LoggingUtils;

@Component
public class HdfsHelper {

    private static final String AVRO_EXTENSION = ".avro";

    private static final String WILD_CARD = "*";

    private static final String HDFS_PATH_SEPARATOR = "/";

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    public String snapshotDirInHdfs(TransformationProgress progress, Source source) {
        return hdfsPathBuilder.constructSnapshotDir(source, getVersionString(progress)).toString();
    }

    public boolean cleanupHdfsDir(String targetDir, TransformationProgress progress, Log logger) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
        } catch (Exception e) {
            LoggingUtils.logError(logger, progress, "Failed to cleanup hdfs dir " + targetDir, e);
            return false;
        }
        return true;
    }

    public String getVersionString(TransformationProgress progress) {
        return HdfsPathBuilder.dateFormat.format(progress.getCreateTime());
    }

    public void extractSchema(TransformationProgress progress, Source source, String avroDir, String version)
            throws Exception {
        String avscPath = hdfsPathBuilder.constructSchemaFile(source, version).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
            HdfsUtils.rmdir(yarnConfiguration, avscPath);
        }

        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration,
                avroDir + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_EXTENSION);
        if (files.size() > 0) {
            String avroPath = files.get(0);
            if (HdfsUtils.fileExists(yarnConfiguration, avroPath)) {
                Schema schema = AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
                HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
            }
        } else {
            throw new IllegalStateException("No avro file found at " + avroDir);
        }
    }

    public Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public HdfsPathBuilder getHdfsPathBuilder() {
        return hdfsPathBuilder;
    }

}
