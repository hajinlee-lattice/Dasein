package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.util.MetadataConverter;


public final class SparkUtils {

    public static Table hdfsUnitToTable(String tableName, String primaryKey, HdfsDataUnit hdfsDataUnit, //
                                        Configuration yarnConfiguration, //
                                        String podId, CustomerSpace customerSpace) {
        String srcPath = hdfsDataUnit.getPath();
        String tgtPath = PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
        try {
            HdfsUtils.moveFile(yarnConfiguration, srcPath, tgtPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to move data from " + srcPath + " to " + tgtPath);
        }

        Table table;
        if (DataUnit.DataFormat.PARQUET.equals(hdfsDataUnit.getDataFormat())) {
            table = MetadataConverter.getParquetTable(yarnConfiguration, tgtPath, //
                    primaryKey, null, true);
        } else {
            table = MetadataConverter.getTable(yarnConfiguration, tgtPath, //
                    primaryKey, null, true);
        }
        table.setName(tableName);
        if (hdfsDataUnit.getCount() != null) {
            table.getExtracts().get(0).setProcessedRecords(hdfsDataUnit.getCount());
        }

        return table;
    }

}
