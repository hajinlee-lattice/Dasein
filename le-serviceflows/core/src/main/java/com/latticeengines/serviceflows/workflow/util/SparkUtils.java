package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.util.MetadataConverter;


public final class SparkUtils {

    public static Table hdfsUnitToTable(String tableName, HdfsDataUnit hdfsDataUnit, //
                                           Configuration yarnConfiguration, //
                                           String podId, CustomerSpace customerSpace) {
        String srcPath = hdfsDataUnit.getPath();
        Table table = MetadataConverter.getTable(yarnConfiguration, srcPath, //
                null, null, true);
        table.setName(tableName);

        String tgtPath = PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
        try {
            HdfsUtils.moveFile(yarnConfiguration, srcPath, tgtPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to move data from " + srcPath + " to " + tgtPath);
        }

        Extract extract = new Extract();
        extract.setPath(tgtPath);
        if (hdfsDataUnit.getCount() != null) {
            extract.setProcessedRecords(hdfsDataUnit.getCount());
        }
        extract.setName(NamingUtils.timestamp("Extract"));
        extract.setExtractionTimestamp(System.currentTimeMillis());
        table.setExtracts(Collections.singletonList(extract));

        return table;
    }
}
