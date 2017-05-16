package com.latticeengines.datacloud.dataflow.bucket;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import com.latticeengines.datacloud.dataflow.transformation.BucketEncode;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;

public class BucketEncodeAccountMasterTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return BucketEncode.BEAN_NAME;
    }

    @Override
    protected String getScenarioName() {
        return "AccountMaster";
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        BucketEncodeParameters parameters = getParameters();
        executeDataFlow(parameters);
        verifyResult();
    }

    private BucketEncodeParameters getParameters() {
        BucketEncodeParameters parameters = new BucketEncodeParameters();

        // read encoded attrs
        InputStream encAttrsIs = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(getDirectory() + File.separator + "config.json");
        if (encAttrsIs == null) {
            throw new RuntimeException("Failed ot find resource config.json");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<List<DCEncodedAttr>> typeRef = new TypeReference<List<DCEncodedAttr>>() { };
        List<DCEncodedAttr> encAttrs;
        try {
            encAttrs = objectMapper.readValue(encAttrsIs, typeRef);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse json config.", e);
        }
        parameters.encAttrs = encAttrs;

        // exclude fields
        List<String> excludeAttrs = new ArrayList<>();

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(getDirectory() + File.separator + "exclude.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource PublicDomains.txt");
        }
        Scanner scanner = new Scanner(is);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (StringUtils.isNotEmpty(line)) {
                excludeAttrs.add(line);
            }
        }
        scanner.close();
        parameters.excludeAttrs = excludeAttrs;

        parameters.setBaseTables(Collections.singletonList("AccountMaster"));
        parameters.rowIdField = "LatticeID";
        parameters.renameRowIdField = "LatticeAccountId";

        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        int numRows = 0;
        for (GenericRecord record : records) {
            System.out.println(record);
            numRows++;
        }
        // Assert.assertEquals(numRows, 3);
    }

}
