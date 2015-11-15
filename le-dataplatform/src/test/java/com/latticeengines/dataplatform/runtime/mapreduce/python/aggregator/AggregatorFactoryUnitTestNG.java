package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AggregatorFactoryUnitTestNG {

    @Test(groups = "unit")
    public void testAggregatorFactorySingleton() throws Exception {
        AggregatorFactory factory = AggregatorFactory.getInstance();

        ProfileAvroAggregator aggregator = new ProfileAvroAggregator();
        factory.registerFileAggregator(FileAggregator.PROFILE_AVRO, aggregator);

        AggregatorFactory factory2 = AggregatorFactory.getInstance();
        FileAggregator aggregator2 = factory2.getAggregator(FileAggregator.PROFILE_AVRO);
        assertEquals(aggregator, aggregator2);
    }

    @Test(groups = "unit")
    public void testAggregatorFactory() throws Exception {
        AggregatorFactory factory = AggregatorFactory.getInstance();

        ProfileAvroAggregator aggregator = new ProfileAvroAggregator();
        DiagnosticsJsonAggregator aggregator2 = new DiagnosticsJsonAggregator();
        Map<String, FileAggregator> fileAggregators = new HashMap<>();
        fileAggregators.put(FileAggregator.PROFILE_AVRO, aggregator);
        fileAggregators.put(FileAggregator.DIAGNOSTICS_JSON, aggregator2);
        factory.registerFileAggregators(fileAggregators);

        assertEquals(aggregator, factory.getAggregator(FileAggregator.PROFILE_AVRO));
        assertEquals(aggregator2, factory.getAggregator(FileAggregator.DIAGNOSTICS_JSON));

        ModelPickleAggregator aggregator3 = new ModelPickleAggregator();
        factory.registerFileAggregator(FileAggregator.MODEL_PICKLE, aggregator3);
        assertEquals(aggregator3, factory.getAggregator(FileAggregator.MODEL_PICKLE));
    }
}
