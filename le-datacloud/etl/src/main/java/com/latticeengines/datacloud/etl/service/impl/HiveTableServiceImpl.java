package com.latticeengines.datacloud.etl.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.HiveTableService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.metadata.hive.HiveTableDao;

@Component("hiveTableService")
public class HiveTableServiceImpl implements HiveTableService {

    private static final Log log = LogFactory.getLog(HiveTableServiceImpl.class);

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private HiveTableDao hiveTableDao;

    @Value("${datacloud.etl.hive.enabled}")
    private boolean hiveEnabled;

    @Override
    public void createTable(String sourceName, String version) {
        String tableName = tableName(sourceName, version);
        if (hiveEnabled) {
            String avroDir = hdfsPathBuilder.constructSnapshotDir(sourceName, version).toString();
            String avscPath = hdfsPathBuilder.constructSchemaFile(sourceName, version).toString();
            hiveTableDao.deleteIfExists(tableName);
            hiveTableDao.create(tableName, avroDir, avscPath);
            log.info("Registered the hive table " + tableName);
        } else {
            log.info("Hive is not enabled, skip registering hive table " + tableName);
        }
    }

    @Override
    public void createTable(String tableName, CustomerSpace customerSpace, String namespace) {
        if (hiveEnabled) {
            String avroDir = hdfsPathBuilder.constructTablePath(tableName, customerSpace, namespace).toString();
            String avscPath = hdfsPathBuilder.constructTableSchemaFilePath(tableName, customerSpace, namespace).toString();
            hiveTableDao.deleteIfExists(tableName);
            hiveTableDao.create(tableName, avroDir, avscPath);
            log.info("Registered the hive table " + tableName);
        } else {
            log.info("Hive is not enabled, skip registering hive table " + tableName);
        }
    }

    private static String tableName(String sourceName, String version) {
        return "LDC_" + sourceName + "_" + version.replace("-", "_");
    }
}
