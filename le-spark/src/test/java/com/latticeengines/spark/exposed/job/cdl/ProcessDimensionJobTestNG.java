package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SourceMedium;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SourceMediumId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ProcessDimensionConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ProcessDimensionJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ProcessDimensionJobTestNG.class);

    private static final List<Pair<String, Class<?>>> WEB_VISIT_PTN_FIELDS = Arrays.asList( //
            Pair.of(PathPatternName.name(), String.class), //
            Pair.of(SourceMedium.name(), String.class), //
            Pair.of(PathPattern.name(), String.class));

    private static final List<Pair<String, Class<?>>> SOURCE_MEDIUM_FIELDS = Collections.singletonList( //
            Pair.of(SourceMedium.name(), String.class));

    private AtomicInteger counter = new AtomicInteger(0);

    @Test(groups = "functional")
    private void test() {
        Pair<ProcessDimensionConfig, List<String>> testData = prepareDimensionData();
        log.info("Config = {}", JsonUtils.serialize(testData.getLeft()));
        SparkJobResult result = runSparkJob(ProcessDimensionJob.class, testData.getLeft(), testData.getRight(),
                getWorkspace());
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Override
    protected void verifyOutput(String output) {
        Assert.assertNotNull(output);

        Map<?, ?> rawMap = JsonUtils.deserialize(output, Map.class);
        Map<String, DimensionMetadata> dimMetadata = JsonUtils.convertMap(rawMap, String.class,
                DimensionMetadata.class);
        Assert.assertNotNull(dimMetadata);
        Assert.assertEquals(dimMetadata.size(), 2);
        Assert.assertTrue(dimMetadata.containsKey(PathPatternId.name()));
        Assert.assertTrue(dimMetadata.containsKey(SourceMediumId.name()));

        // path ptn, total 4 diff values, top 3 is chosen
        verifyMetadata(dimMetadata.get(PathPatternId.name()), 4L, PathPatternId.name(),
                Arrays.asList("video content 1", "video content 2", "content"));

        // source medium, total 3
        verifyMetadata(dimMetadata.get(SourceMediumId.name()), 3L, SourceMediumId.name(),
                Arrays.asList("google/paid", "google/organic", "facebook/paid"));
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        // no output when collectMetadata is true
        return Collections.emptyList();
    }

    private void verifyMetadata(DimensionMetadata metadata, long expectedCardinality, String idColumn,
            List<String> expectedIds) {
        Assert.assertNotNull(metadata);
        Assert.assertEquals(metadata.getCardinality(), expectedCardinality);
        Assert.assertNotNull(metadata.getDimensionValues());
        Assert.assertEquals(metadata.getDimensionValues().size(), expectedIds.size());

        Set<String> ids = metadata.getDimensionValues() //
                .stream() //
                .map(value -> (String) value.get(idColumn)) //
                .collect(Collectors.toSet());
        // hashed id
        Assert.assertEquals(ids,
                expectedIds.stream().map(DimensionGenerator::hashDimensionValue).collect(Collectors.toSet()));
    }

    private Pair<ProcessDimensionConfig.Dimension, String> prepareWebVisitPtnData() {
        Object[][] ptnData = new Object[][] { //
                /*-
                 * content ptn, freq = 2
                 */
                { "content", "google/paid", "https://google.com/contents" }, //
                { "content", "google/paid", "https://google.com/contents" }, //
                /*-
                 * freq = 1, will be pushed out (not top 3)
                 */
                { "video content", "google/organic", "https://google.com/contents/videos" }, //
                /*-
                 * freq = 5 & 4 same ptnId, one of these two will be chosen
                 */
                { "video content 1", "facebook/paid", "https://google.com/contents/videos/1" }, //
                { "video content 1", "facebook/paid", "https://google.com/contents/videos/1" }, //
                { "video content 1", "google/paid", "https://google.com/contents/videos/1" }, //
                { "video content 1", "google/paid", "https://google.com/contents/videos/1" }, //
                { "video content 1", "google/paid", "https://google.com/contents/videos/1" }, //

                { "video content 1", "facebook/paid", "https://google.com/contents/videos/1?test=123" }, //
                { "video content 1", null, "https://google.com/contents/videos/1?test=123" }, //
                { "video content 1", null, "https://google.com/contents/videos/1?test=123" }, //
                { "video content 1", "facebook/paid", "https://google.com/contents/videos/1?test=123" }, //
                /*-
                 * content ptn 2, freq = 3
                 */
                { "video content 2", null, "https://google.com/contents/videos/2" }, //
                { "video content 2", "google/paid", "https://google.com/contents/videos/2" }, //
                { "video content 2", "google/paid", "https://google.com/contents/videos/2" }, //
                /*-
                 * missing required fields, ignored
                 */
                { null, "facebook/paid", "https://google.com/contents/videos/null" }, //
                { null, "facebook/paid", null }, //
        };

        ProcessDimensionConfig.Dimension dim = new ProcessDimensionConfig.Dimension();
        dim.inputIdx = counter.getAndIncrement();
        dim.hashAttrs = Collections.singletonMap(PathPatternName.name(), PathPatternId.name());
        dim.attrs = Sets.newHashSet(PathPattern.name(), PathPatternName.name(), PathPatternId.name());
        dim.dedupAttrs = Sets.newHashSet(PathPatternId.name());
        dim.valueLimit = 3;
        return Pair.of(dim, uploadHdfsDataUnit(ptnData, WEB_VISIT_PTN_FIELDS));
    }

    private Pair<ProcessDimensionConfig.Dimension, String> prepareSourceMediumData() {
        Object[][] smData = new Object[][] { //
                { "google/paid" }, //
                { "google/paid" }, //
                { "google/paid" }, //
                { "google/organic" }, //
                { "google/organic" }, //
                { "facebook/paid" }, //
        };

        ProcessDimensionConfig.Dimension dim = new ProcessDimensionConfig.Dimension();
        dim.inputIdx = counter.getAndIncrement();
        dim.hashAttrs = Collections.singletonMap(SourceMedium.name(), SourceMediumId.name());
        dim.attrs = Sets.newHashSet(SourceMedium.name(), SourceMediumId.name());
        dim.dedupAttrs = Collections.singleton(SourceMediumId.name());
        dim.valueLimit = null; // keep everything
        return Pair.of(dim, uploadHdfsDataUnit(smData, SOURCE_MEDIUM_FIELDS));
    }

    private Pair<ProcessDimensionConfig, List<String>> prepareDimensionData() {
        Pair<ProcessDimensionConfig.Dimension, String> ptnData = prepareWebVisitPtnData();
        Pair<ProcessDimensionConfig.Dimension, String> smData = prepareSourceMediumData();

        List<String> inputs = Arrays.asList(ptnData.getRight(), smData.getRight());
        ProcessDimensionConfig config = new ProcessDimensionConfig();
        config.dimensions = new HashMap<>();
        config.dimensions.put(PathPatternId.name(), ptnData.getLeft());
        config.dimensions.put(SourceMediumId.name(), smData.getLeft());
        config.collectMetadata = true;
        return Pair.of(config, inputs);
    }
}
