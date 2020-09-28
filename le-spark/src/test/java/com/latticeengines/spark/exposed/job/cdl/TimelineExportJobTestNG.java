package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.DUNS;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Detail1;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Detail2;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Domain;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.DomesticUltimateDuns;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EventTimestamp;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EventType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.GlobalUltimateDuns;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Id;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.IsPrimaryDomain;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Source;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StreamType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateTimelineExportArtifactsJobConfig;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class TimelineExportJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TimelineExportJobTestNG.class);

    private static final List<Pair<String, Class<?>>> TIMELINE_FIELDS = Arrays.asList( //
            Pair.of(Id.name(), String.class),
            Pair.of(AccountId.name(), String.class), //
            Pair.of(EventTimestamp.name(), Long.class), //
            Pair.of(ContactId.name(), String.class), //
            Pair.of(EventType.name(), String.class), //
            Pair.of(StreamType.name(), String.class), //
            Pair.of(Detail1.name(), String.class), //
            Pair.of(Detail2.name(), String.class), //
            Pair.of(Source.name(), String.class), //
            Pair.of(ContactName.name(), String.class)
    );

    private static final List<Pair<String, Class<?>>> LATTICE_ACCOUNT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(DomesticUltimateDuns.name(), String.class), //
            Pair.of(DUNS.name(), String.class), //
            Pair.of(Domain.name(), String.class), //
            Pair.of(GlobalUltimateDuns.name(), String.class), //
            Pair.of(IsPrimaryDomain.name(), Boolean.class)
    );

    private static final List<Pair<String, Class<?>>> SEGMWNT_ACCOUNT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class)
    );

    private static final long now = LocalDate.of(2019, 11, 23) //
            .atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

    private Map<String, String> timelineTableNames = new HashMap<>();
    private Map<String, Integer> inputIdx = new HashMap<>();
    private Integer latticeAccountTableIdx;
    private Integer accountListIdx;

    @Test(groups = "functional")
    private void test() {
        prepareData();

        SparkJobResult result = runSparkJob(GenerateTimelineExportArtifacts.class, baseConfig());
        log.info("result is {}.", result.getTargets().stream().map(HdfsDataUnit::getPath).collect(Collectors.toList()));
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verify, this::verify);
    }

    private Boolean verify(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        List<String> verifyColumns = new ArrayList<>(TimeLineStoreUtils.TimelineExportColumn.getColumnNames());
        Map<String, List<String>> expectedDetail2Map = prepareExpectedDetail2Result();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            log.info(debugStr(record, verifyColumns));
            Object detail1 = record.get(InterfaceName.Detail1.name());
            if (detail1 != null) {
                List<String> expectedDetail2 = expectedDetail2Map.get(detail1.toString());
                String detail2 = record.get(InterfaceName.Detail2.name()).toString();
                List<String> detail2Arr = Arrays.asList(detail2.split(","));
                log.info("detail1 is {}, detail2 is {}.", detail1.toString(), detail2);
                Assert.assertEquals(detail2Arr, expectedDetail2);
            }
            counter.incrementAndGet();
        });
        log.info("Number of records = {}", counter.get());
        return true;
    }

    private void prepareData() {
        // Id, AccountId, EventTimestamp, ContactId, EventType, StreamType, Detail1, Detail2, Source, ContactName
        Object[][] timeline1Table = new Object[][]{ //
                {"asr_x5dak3tozrhggxxoolhgclq", "A2", 1574398800000L, "C3", "Form Filled", "MarketingActivity", null
                        , null, "Salesforce", "tourist"}, //
                {"asr_bsd9zrq1ssg6vqpf3kyxhw", "A3", 1573707600000L, "C4", "Email Sent", "MarketingActivity", null,
                        null, "Marketo", "hello world"}, //
                {"asr_x8kzc2x3cr_emczqpk_a_kg", "A2", 1574398800000L, "C3", "Form Filled", "MarketingActivity", null
                        , null, "Salesforce", "tourist"}, //
                {"asr_u1akgakbqnihenjoarox1a", "a100", 1574398800000L, null, "Page Visit", "WebVisit", "https://dnb" +
                        ".com/contents/audios/1", "all content pages", "Salesforce", null}, //
        };
        String timeline1TableName = uploadHdfsDataUnit(timeline1Table, TIMELINE_FIELDS);
        timelineTableNames.put("timelineName1", timeline1TableName);
        inputIdx.put(timeline1TableName, 0);
        // Id, AccountId, EventTimestamp, ContactId, EventType, StreamType, Detail1, Detail2, Source, ContactName
        Object[][] timeline2Table = new Object[][]{ //
                {"asr_x_l_ppotsh2pgugaki8viq", "a100", 1574398800000L, null, "Page Visit", "WebVisit", "https://dnb.com/contents/audios/1"
                        , "all content pages", "Salesforce", null}, //
                {"asr_qeadbo2iqx2n8s7fbyhvvw", "a101", 1573794000000L, null, "Page Visit", "WebVisit", "https://dnb.com/contents/videos/1",
                        "all content pages", "Website", null}, ////
        };
        String timeline2TableName = uploadHdfsDataUnit(timeline2Table, TIMELINE_FIELDS);
        timelineTableNames.put("timelineName2", timeline2TableName);
        inputIdx.put(timeline2TableName, 1);

        // AccountId, DU_DUNS, DUNS, Domain, GU_DUNS, IsPrimaryDomain
        Object[][] latticeAccountTable = new Object[][]{ //
                {"a100", "123456789", "123456789", "dnb.com", "123456789", true}, //
                {"a101", "12345678", "12345678", "dnb.net", "12345678", false}, ////
                {"A2", "1234568", "1234568", "video.net", "1234568", false}, ////
        };
        uploadHdfsDataUnit(latticeAccountTable, LATTICE_ACCOUNT_FIELDS);
        latticeAccountTableIdx = 2;

        Object[][] segmentAccountTable = new Object[][]{ //
                {"a100"}, //
        };
        uploadHdfsDataUnit(segmentAccountTable, SEGMWNT_ACCOUNT_FIELDS);
        accountListIdx = 3;

    }

    private GenerateTimelineExportArtifactsJobConfig baseConfig() {
        GenerateTimelineExportArtifactsJobConfig config = new GenerateTimelineExportArtifactsJobConfig();
        config.inputIdx = inputIdx;
        config.latticeAccountTableIdx = latticeAccountTableIdx;
        config.timelineTableNames = timelineTableNames;
        config.fromDateTimestamp = 1573894000000L;
        config.toDateTimestamp = 1574398800000L;
        config.eventTypes = Collections.singletonList("Page Visit");
        config.rollupToDaily = true;
        config.timeZone = "America/New_York";
        config.accountListIdx = accountListIdx;
        return config;
    }

    private Map<String, List<String>> prepareExpectedDetail2Result() {
        Map<String, List<String>> detail2ResultMap = new HashMap<>();
        detail2ResultMap.put("https://dnb.com/contents/audios/1", Collections.singletonList("all content pages"));
        detail2ResultMap.put("https://dnb.com/contents/videos/1", Arrays.asList("all content pages"));
        return detail2ResultMap;
    }
}
