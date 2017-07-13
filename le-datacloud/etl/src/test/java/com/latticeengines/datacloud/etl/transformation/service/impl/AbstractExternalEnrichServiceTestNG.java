package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.datacloud.etl.transformation.service.ExternalEnrichService;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.ExternalEnrichRequest;

public class AbstractExternalEnrichServiceTestNG extends DataCloudEtlFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AbstractExternalEnrichServiceTestNG.class);

    private static final String ROOT_DIR = "/tmp/AbstractExternalEnrichServiceTestNG";
    private static final String INPUT_AVRO_PATH = ROOT_DIR + "/input/input.avro";
    private static final String INPUT_AVRO_GLOB = ROOT_DIR + "/input/*.avro";
    private static final String OUTPUT_AVRO_DIR = ROOT_DIR + "/output";
    private static final String INPUT_RECORD_NAME = "DummySource";
    private static final String OUTPUT_RECORD_NAME = "DummySource";

    @Autowired
    @Qualifier("domainToNameEnrichService")
    private ExternalEnrichService enrichService;

    @Test(groups = "functional")
    public void testValidateInput() throws Exception {
        cleanup();
        uploadData();

        enrichService.enrich(getEnrichRequest());

        verify();
    }

    private void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, ROOT_DIR);
    }

    private void uploadData() {
        Object[][] data = {
                {1, "a.com", "123@a.com"},
                {2, "b.com", "123@a.com"},
                {3, "c.com", null},
                {4, null, "234@d.com"},
                {5, "e.com", "234@e.com"}
        };

        List<String> fieldNames = Arrays.asList("ID", "Domain", "Email");
        List<Class<?>> clz = Arrays.asList((Class<?>) Integer.class, String.class, String.class);
        uploadDataToHdfs(data, fieldNames, clz, INPUT_AVRO_PATH, INPUT_RECORD_NAME);
    }

    private void verify() {
        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, OUTPUT_AVRO_DIR + "/*.avro");
        while (iterator.hasNext()) {
            GenericRecord record = iterator.next();
            int id = (Integer) record.get("ID");
            switch (id) {
                case 1:
                    Assert.assertEquals(record.get("Domain").toString(), "a.com");
                    Assert.assertEquals(record.get("Name").toString(), "a.com");
                    break;
                case 2:
                    Assert.assertEquals(record.get("Domain").toString(), "b.com");
                    Assert.assertEquals(record.get("Name").toString(), "b.com");
                    break;
                case 3:
                    Assert.assertEquals(record.get("Domain").toString(), "c.com");
                    Assert.assertEquals(record.get("Name").toString(), "c.com");
                    break;
                case 4:
                    Assert.assertEquals(record.get("Domain").toString(), "d.com");
                    Assert.assertNull(record.get("Name"));
                    break;
                case 5:
                    Assert.assertEquals(record.get("Domain").toString(), "e.com");
                    Assert.assertEquals(record.get("Name").toString(), "e.com");
                    break;
                default:
            }
        }
    }

    private ExternalEnrichRequest getEnrichRequest() {
        ExternalEnrichRequest request = new ExternalEnrichRequest();
        request.setRecordName(OUTPUT_RECORD_NAME);
        request.setAvroInputGlob(INPUT_AVRO_GLOB);
        request.setAvroOuptutDir(OUTPUT_AVRO_DIR);

        Map<MatchKey, List<String>> inputKeyMap = new HashMap<>();
        inputKeyMap.put(MatchKey.Domain, Arrays.asList("Domain", "Email"));
        request.setInputKeyMapping(inputKeyMap);

        Map<MatchKey, String> outputKeyMap = new HashMap<>();
        outputKeyMap.put(MatchKey.Domain, "Domain");
        outputKeyMap.put(MatchKey.Name, "Name");
        outputKeyMap.put(MatchKey.DUNS, "DUNS");
        request.setOutputKeyMapping(outputKeyMap);

        return request;
    }

}
