package com.latticeengines.datafabric.connector.generic;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.datastore.impl.FabricDataServiceImpl;
import com.latticeengines.datafabric.service.datastore.impl.S3DataServiceProvider;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class S3ProcessorAdapter extends AbstractProcessorAdapter {
    private final Logger log = LoggerFactory.getLogger(S3ProcessorAdapter.class);

    private GenericSinkConnectorConfig connectorConfig;
    private Configuration yarnConfig;

    @Override
    public void setup(GenericSinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public int write(String repository, Map<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> pairs) {

        int count = 0;
        String s3AccessKey = connectorConfig.getProperty(GenericSinkConnectorConfig.S3_ACCESS_KEY, String.class);
        String s3SecretKey = connectorConfig.getProperty(GenericSinkConnectorConfig.S3_SECRET_KEY, String.class);
        String s3BaseDir = connectorConfig.getProperty(GenericSinkConnectorConfig.S3_BASE_DIR, String.class);
        String s3LocalDir = connectorConfig.getProperty(GenericSinkConnectorConfig.S3_LOCAL_DIR, String.class);

        Configuration conf = getYarnConfig(s3AccessKey, s3SecretKey);

        S3DataServiceProvider s3Provider = new S3DataServiceProvider(conf, s3BaseDir, s3LocalDir, repository);
        FabricDataService dataService = new FabricDataServiceImpl();
        dataService.addServiceProvider(s3Provider);

        Pair<GenericRecordRequest, GenericRecord> pair = pairs.values().iterator().next().get(0);
        Schema schema = pair.getValue().getSchema();
        schema = AvroUtils.extractSimpleSchema(schema);
        for (Map.Entry<TopicPartition, List<Pair<GenericRecordRequest, GenericRecord>>> entry : pairs.entrySet()) {
            String fileName = getFileName(entry.getKey());
            FabricDataStore dataStore = dataService.constructDataStore(s3Provider.getName(), fileName,
                    pair.getKey().getRecordType(), schema);
            Map<String, Pair<GenericRecord, Map<String, Object>>> pairMap = getPairMap(entry.getValue());
            count += entry.getValue().size();
            dataStore.createRecords(pairMap);
        }
        if (log.isDebugEnabled()) {
            log.debug("Wrote generic connector records, count=" + count + " store=" + s3Provider.getName()
                    + " repository=" + repository);
        }

        return count;
    }

    private Configuration getYarnConfig(String accessKey, String secretKey) {
        if (yarnConfig != null) {
            return yarnConfig;
        }
        accessKey = CipherUtils.decrypt(accessKey);
        secretKey = CipherUtils.decrypt(secretKey);
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("fs.s3a.access.key", accessKey);
        yarnConfiguration.set("fs.s3a.secret.key", secretKey);
        return yarnConfiguration;
    }

    protected void setYarnConfig(Configuration yarnConfig) {
        this.yarnConfig = yarnConfig;
    }

}
