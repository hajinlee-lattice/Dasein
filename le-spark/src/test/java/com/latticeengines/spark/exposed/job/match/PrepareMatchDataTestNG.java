package com.latticeengines.spark.exposed.job.match;

import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.City;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Country;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Domain;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Name;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.State;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.InternalId;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.PrepareMatchDataJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class PrepareMatchDataTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PrepareMatchDataTestNG.class);

    private static final List<Pair<String, Class<?>>> FIELDS = Arrays.asList( //
            Pair.of(InternalId.name(), Long.class), //
            Pair.of(Name.name(), String.class), //
            Pair.of(City.name(), String.class), //
            Pair.of(State.name(), String.class), //
            Pair.of(Country.name(), String.class), //
            Pair.of(Domain.name(), String.class) //
    );
    private static final List<String> FIELD_NAMES = FIELDS.stream().map(Pair::getKey).collect(Collectors.toList());
    private static final List<String> MATCH_FIELDS = Arrays.asList(InternalId.name(), Name.name(), Domain.name());
    private static final String MATCH_GROUP_ID = InternalId.name();

    @Test(groups = "functional")
    private void test() {
        PrepareMatchDataJobConfig config = prepareInput();
        SparkJobResult result = runSparkJob(PrepareMatchDataJob.class, config);
        verifyResult(result);
    }

    private PrepareMatchDataJobConfig prepareInput() {
        // upload input data

        Object[][] data = new Object[][] { //
                /*-
                 * some duplicate matchGroupId that will be deduped
                 */
                { 1L, "Name1_1", "City1", "State1", "Country1", "d.com" }, //
                { 1L, "Name1_2", "City1", "State1", "Country1", "d.com" }, //
                { 2L, "Name2_1", "City2", "State2", "Country2", "d.com" }, //
                { 2L, "Name2_2", "City2", "State2", "Country2", "d.com" }, //
                { 2L, "Name2_3", "City2", "State2", "Country2", "d.com" }, //
                { 3L, "Name3", "City3", "State3", "Country3", "d.com" }, //
                /*-
                 * null ID will be deduped too (actually empty will be deduped with null if it is string type)
                 */
                { null, "Name_null_1", "City_null", "State_null", "Country_null", "d.com" }, //
                { null, "Name_null_2", "City_null", "State_null", "Country_null", "d.com" }, //
        };
        uploadHdfsDataUnit(data, FIELDS);

        PrepareMatchDataJobConfig config = new PrepareMatchDataJobConfig();
        config.matchFields = MATCH_FIELDS;
        config.matchGroupId = MATCH_GROUP_ID;
        return config;
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger(0);
        Set<Object> matchGroupIds = new HashSet<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            List<String> nameValues = FIELD_NAMES.stream().map(field -> String.format("%s=%s", field, record.get(field)))
                    .collect(Collectors.toList());
            log.info(Strings.join(nameValues, ","));

            // check retained match fields
            FIELD_NAMES.forEach(field -> {
                if (MATCH_FIELDS.contains(field)) {
                    Assert.assertNotNull(record.getSchema().getField(field),
                            String.format("Match field %s should be retained", field));
                } else {
                    Assert.assertNull(record.getSchema().getField(field),
                            String.format("Non match field %s should be dropped", field));
                }
            });
            count.incrementAndGet();
            Assert.assertTrue(matchGroupIds.add(record.get(MATCH_GROUP_ID)),
                    String.format("Got duplicate matchGroupId(%s=%s)", MATCH_GROUP_ID, record.get(MATCH_GROUP_ID)));
        });
        Assert.assertEquals(count.get(), 4);
        return true;
    }
}
