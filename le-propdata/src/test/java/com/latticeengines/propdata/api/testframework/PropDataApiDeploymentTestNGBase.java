package com.latticeengines.propdata.api.testframework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.Path;

public abstract class PropDataApiDeploymentTestNGBase extends PropDataApiAbstractTestNGBase {

    @Value("${propdata.api.deployment.hostport}")
    private String hostPort;

    @Autowired
    protected Configuration yarnConfiguration;

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    protected void uploadAvroData(List<List<Object>> data, List<String> fieldNames, List<Class<?>> fieldTypes,
            String avroDir, String fileName) {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            schemaMap.put(fieldNames.get(i), fieldTypes.get(i));
        }
        String tableName = fileName.endsWith(".avro") ? fileName.replace(".avro", "") : fileName;
        Schema schema = AvroUtils.constructSchema(tableName, schemaMap);
        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (List<Object> tuple : data) {
            for (int i = 0; i < tuple.size(); i++) {
                builder.set(fieldNames.get(i), tuple.get(i));
            }
            records.add(builder.build());
        }

        try {
            String fullPath = new Path(avroDir).append(fileName).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, fullPath)) {
                HdfsUtils.rmdir(yarnConfiguration, fullPath);
            }
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, fullPath, records);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }
    }

}
