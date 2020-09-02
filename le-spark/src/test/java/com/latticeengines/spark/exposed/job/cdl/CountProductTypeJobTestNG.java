package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CountProductTypeConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CountProductTypeJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CountProductTypeJobTestNG.class);

    private static final String spending = ProductType.Spending.name();
    private static final String analytic = ProductType.Analytic.name();
    private static final String txnId = InterfaceName.TransactionId.name();
    private static final String productType = InterfaceName.ProductType.name();

    private static final Long SPENDING_COUNT = 1L;
    private static final Long ANALYTIC_COUNT = 3L;

    @Test(groups = "functional")
    @SuppressWarnings("unchecked")
    private void test() {
        List<String> inputs = Collections.singletonList(prepareTransactionTable());
        CountProductTypeConfig config = new CountProductTypeConfig();
        config.types = Arrays.asList(spending, analytic);
        SparkJobResult result = runSparkJob(CountProductTypeJob.class, config, inputs, getWorkspace());
        log.info("Output: {}", result.getOutput());
        Assert.assertTrue(StringUtils.isNotBlank(result.getOutput()));
        Map<String, Long> countMap = JsonUtils.deserializeByTypeRef(result.getOutput(),
                new TypeReference<HashMap<String, Long>>() {
                });
        Assert.assertEquals(countMap.get(spending), SPENDING_COUNT);
        Assert.assertEquals(countMap.get(analytic), ANALYTIC_COUNT);
        List<String> typesToRetain = countMap.entrySet().stream().filter(entry -> entry.getValue() > SPENDING_COUNT)
                .map(Map.Entry::getKey).collect(Collectors.toList());
        Assert.assertEquals(typesToRetain.size(), 1);
        Assert.assertTrue(typesToRetain.contains(analytic));
    }

    private String prepareTransactionTable() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(Pair.of(txnId, String.class),
                Pair.of(productType, String.class));
        Object[][] data = new Object[][] { //
                { "t1", spending }, //
                { "t1", analytic }, //
                { "t1", analytic }, //
                { "t1", analytic } //
        };
        return uploadHdfsDataUnit(data, fields);
    }
}
