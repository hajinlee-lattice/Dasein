package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;

public class TableCloneUtils {

    private static final Logger log = LoggerFactory.getLogger(TableCloneUtils.class);

    public static Table cloneDataTable(Configuration yarnConfiguration, //
                                       CustomerSpace customerSpace, //
                                       String cloneName, //
                                       Table original, //
                                       String distCpQueue) {
        Table clone = JsonUtils.clone(original);
        clone.setTableType(TableType.DATATABLE);
        clone.setName(cloneName);

        String cloneDataPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                customerSpace, original.getNamespace()).append(clone.getName()).toString();

        Extract newExtract = new Extract();
        newExtract.setPath(cloneDataPath + "/*.avro");
        newExtract.setName(NamingUtils.uuid("Extract"));
        AtomicLong count = new AtomicLong(0);
        if (original.getExtracts() != null && original.getExtracts().size() > 0) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, cloneDataPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, cloneDataPath);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to create table dir at " + cloneDataPath);
            }

            original.getExtracts().forEach(extract -> {
                String srcPath = extract.getPath();
                if (!srcPath.endsWith(".avro") && !srcPath.endsWith("*.avro")) {
                    srcPath = srcPath.endsWith("/") ? srcPath : srcPath + "/";
                    srcPath += "*.avro";
                }
                String srcDir = srcPath.substring(0, srcPath.lastIndexOf("/"));
                try {
                    log.info(String.format("Copying table data from %s to %s", srcDir, cloneDataPath));
                    HdfsUtils.distcp(yarnConfiguration, srcDir, cloneDataPath, distCpQueue);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", srcPath,
                            cloneDataPath), e);
                }

                if (extract.getProcessedRecords() != null && extract.getProcessedRecords() > 0) {
                    count.addAndGet(extract.getProcessedRecords());
                }
            });
        }
        newExtract.setProcessedRecords(count.get());
        newExtract.setExtractionTimestamp(System.currentTimeMillis());
        clone.setExtracts(Collections.singletonList(newExtract));

        String oldTableSchema = PathBuilder.buildDataTableSchemaPath(CamilleEnvironment.getPodId(),
                customerSpace, original.getNamespace()).append(original.getName()).toString();
        String cloneTableSchema = oldTableSchema.replace(original.getName(), clone.getName());

        try {
            if (HdfsUtils.fileExists(yarnConfiguration, oldTableSchema)) {
                HdfsUtils.copyFiles(yarnConfiguration, oldTableSchema, cloneTableSchema);
                log.info(String.format("Copying table schema from %s to %s", oldTableSchema, cloneTableSchema));
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to copy schema in HDFS from %s to %s", oldTableSchema,
                    cloneTableSchema), e);
        }

        return clone;
    }

}
