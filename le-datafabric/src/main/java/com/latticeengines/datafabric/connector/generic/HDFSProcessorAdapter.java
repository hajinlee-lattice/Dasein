package com.latticeengines.datafabric.connector.generic;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.datastore.impl.FabricDataServiceImpl;
import com.latticeengines.datafabric.service.datastore.impl.HDFSDataServiceProvider;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class HDFSProcessorAdapter extends AbstractProcessorAdapter {
    private final Logger log = LoggerFactory.getLogger(HDFSProcessorAdapter.class);

    private GenericSinkConnectorConfig connectorConfig;
    private Configuration yarnConfig;

    @Override
    public void setup(GenericSinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public int write(String repository, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> pairs) {

        int count = 0;
        String hadoopConfDir = connectorConfig.getProperty(GenericSinkConnectorConfig.HADOOP_CONF_DIR, String.class);
        String hdfsBaseDir = connectorConfig.getProperty(GenericSinkConnectorConfig.HDFS_BASE_DIR, String.class);

        Configuration conf = getYarnConfig(hadoopConfDir);

        HDFSDataServiceProvider hdfsProvider = new HDFSDataServiceProvider(conf, hdfsBaseDir, repository);
        FabricDataService dataService = new FabricDataServiceImpl();
        dataService.addServiceProvider(hdfsProvider);

        Pair<GenericRecordRequest, GenericRecord> pair = pairs.values().iterator().next().get(0);
        Schema schema = pair.getValue().getSchema();
        schema = AvroUtils.extractSimpleSchema(schema);
        for (Map.Entry<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> entry : pairs.entrySet()) {
            String fileName = getFileName(entry.getKey());
            FabricDataStore dataStore = dataService.constructDataStore(hdfsProvider.getName(), fileName, pair.getKey()
                    .getRecordType(), schema);
            Map<String, Pair<GenericRecord, Map<String, Object>>> pairMap = getPairMap(entry.getValue());
            count += entry.getValue().size();
            dataStore.createRecords(pairMap);
        }
        log.info("Wrote generic connector records, count=" + count + " store=" + hdfsProvider.getName()
                + " repository=" + repository);
        return count;
    }

    private Configuration getYarnConfig(String hadoopConfDir) {
        if (yarnConfig != null) {
            return yarnConfig;
        }
        Configuration conf = new Configuration();
        if (StringUtils.isBlank(hadoopConfDir)) {
            String hadoopHome = System.getenv("HADOOP_HOME");
            if (StringUtils.isNotBlank(hadoopHome)) {
                hadoopConfDir = hadoopHome + "/etc/hadoop";
            }
        }
        log.info("Hadoop config Dir=" + hadoopConfDir);

        conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
        return conf;
    }

    protected void setYarnConfig(Configuration yarnConfig) {
        this.yarnConfig = yarnConfig;
    }

}
