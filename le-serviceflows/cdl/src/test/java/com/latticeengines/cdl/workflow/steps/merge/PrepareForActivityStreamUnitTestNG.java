package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation.COUNT;
import static com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation.MAX;
import static com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation.MIN;
import static com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation.SUM;
import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior.Append;
import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior.Replace;
import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior.Upsert;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static com.latticeengines.domain.exposed.util.WebVisitUtils.pathPtnDimension;
import static com.latticeengines.domain.exposed.util.WebVisitUtils.sourceMediumDimension;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.WebVisitUtils;

public class PrepareForActivityStreamUnitTestNG {

    private static final List<String> DEFAULT_MATCH_ENTITIES = singletonList(Account.name());
    private static final List<String> DEFAULT_AGGR_ENTITIES = singletonList(Account.name());
    private static final String DEFAULT_DATE_ATTR = InterfaceName.CreatedDate.name();
    private static final String DEFAULT_STREAM_NAME = "test_stream";
    private static final String DEFAULT_TASK_ID = UUID.randomUUID().toString();

    @Test(groups = "unit", dataProvider = "streamsNeedRebuild")
    private void testStreamsNeedRebuild(AtlasStream previous, AtlasStream current, boolean shouldRebuild) {
        PrepareForActivityStream step = new PrepareForActivityStream();
        // when ingestion behavior is not replace, pass in the same object, shouldn't
        // need to rebuild
        if (previous.getDataFeedTaskIngestionBehavior() != Replace) {
            Assert.assertFalse(step.requireRebuild(previous, previous));
        }
        if (current.getDataFeedTaskIngestionBehavior() != Replace) {
            Assert.assertFalse(step.requireRebuild(current, current));
        }
        // run with previous/current state
        boolean result = step.requireRebuild(previous, current);
        Assert.assertEquals(result, shouldRebuild, String.format("shouldRebuild=%s, previous=%s, current=%s",
                shouldRebuild, JsonUtils.serialize(previous), JsonUtils.serialize(current)));
    }

    @DataProvider(name = "streamsNeedRebuild")
    private Object[][] provideStreamsNeedRebuildTestData() {
        return new Object[][] { //
                { defaultStream(), defaultStream(), false }, //
                { defaultStream(), dateAttr(InterfaceName.CDLCreatedTime.name()), true }, // date attr changed
                /*-
                 * entities
                 */
                { defaultStream(), aggrEntities(DEFAULT_AGGR_ENTITIES), false },
                { defaultStream(), aggrEntities(Arrays.asList(Account.name(), Contact.name())), true },
                { defaultStream(), aggrEntities(Collections.emptyList()), true },
                { defaultStream(), aggrEntities(singletonList(Contact.name())), true },
                { defaultStream(), matchEntities(DEFAULT_MATCH_ENTITIES), false },
                { defaultStream(), matchEntities(Arrays.asList(Account.name(), Contact.name())), true },
                { defaultStream(), matchEntities(Collections.emptyList()), true },
                { defaultStream(), matchEntities(singletonList(Contact.name())), true },
                /*-
                 * need to rebuild when ingestion behavior is replace
                 */
                { defaultStream(), ingestionBehavior(Replace), true }, //
                { defaultStream(), ingestionBehavior(Upsert), false }, //
                { defaultStream(), ingestionBehavior(Append), false }, //
                /*-
                 * dimensions changed
                 */
                { defaultStream(), dimensions(getDefaultDimensions()), false }, //
                { defaultStream(), addDimension(pathPtnDimension(dummyStream(), null)), true }, //
                { defaultStream(), dimensions(singletonList(sourceMediumDimension(dummyStream()))), true }, //
                { defaultStream(),
                        dimensions(Arrays.asList(sourceMediumDimension(dummyStream()),
                                pathPtnDimension(dummyStream(), null))),
                        true }, //
                /*-
                 * attribute deriver
                 */
                { defaultStream(), attrDerivers(singletonList(deriver(COUNT))), false }, //
                { defaultStream(), attrDerivers(singletonList(deriver(SUM))), true }, //
                { defaultStream(), addAttrDeriver(deriver(SUM)), true }, //
                { defaultStream(), attrDerivers(Arrays.asList(deriver(MAX), deriver(MIN))), true }, //
        }; //
    }

    private StreamAttributeDeriver deriver(StreamAttributeDeriver.Calculation calculation) {
        StreamAttributeDeriver visitCount = new StreamAttributeDeriver();
        visitCount.setSourceAttributes(Collections.singletonList(InterfaceName.InternalId.name()));
        visitCount.setTargetAttribute(InterfaceName.TotalVisits.name());
        visitCount.setCalculation(calculation);
        return visitCount;
    }

    private AtlasStream addAttrDeriver(StreamAttributeDeriver deriver) {
        AtlasStream stream = defaultStream();
        List<StreamAttributeDeriver> derivers = new ArrayList<>(stream.getAttributeDerivers());
        derivers.add(deriver);
        stream.setAttributeDerivers(derivers);
        return stream;
    }

    private AtlasStream attrDerivers(List<StreamAttributeDeriver> derivers) {
        AtlasStream stream = defaultStream();
        stream.setAttributeDerivers(derivers);
        return stream;
    }

    private AtlasStream addDimension(StreamDimension dimension) {
        AtlasStream stream = defaultStream();
        List<StreamDimension> dimensions = new ArrayList<>(stream.getDimensions());
        dimensions.add(dimension);
        stream.setDimensions(dimensions);
        return stream;
    }

    private AtlasStream dimensions(List<StreamDimension> dimensions) {
        AtlasStream stream = defaultStream();
        stream.setDimensions(dimensions);
        return stream;
    }

    private AtlasStream dateAttr(String dateAttribute) {
        AtlasStream stream = defaultStream();
        stream.setDateAttribute(dateAttribute);
        return stream;
    }

    private AtlasStream aggrEntities(List<String> entities) {
        AtlasStream stream = defaultStream();
        stream.setAggrEntities(entities);
        return stream;
    }

    private AtlasStream matchEntities(List<String> entities) {
        AtlasStream stream = defaultStream();
        stream.setMatchEntities(entities);
        return stream;
    }

    private AtlasStream ingestionBehavior(DataFeedTask.IngestionBehavior behavior) {
        AtlasStream stream = defaultStream();
        stream.setDataFeedTaskIngestionBehavior(behavior);
        return stream;
    }

    private AtlasStream defaultStream() {
        AtlasStream stream = new AtlasStream();
        stream.setName(DEFAULT_STREAM_NAME);
        stream.setMatchEntities(DEFAULT_MATCH_ENTITIES);
        stream.setAggrEntities(DEFAULT_AGGR_ENTITIES);
        stream.setAttributeDerivers(getDefaultAttrDerivers());
        stream.setDateAttribute(DEFAULT_DATE_ATTR);
        stream.setDataFeedTaskUniqueId(DEFAULT_TASK_ID);
        stream.setDataFeedTaskIngestionBehavior(DataFeedTask.IngestionBehavior.Append);
        stream.setRetentionDays(3);
        stream.setPeriods(Arrays.asList(PeriodStrategy.Template.Week.name(), PeriodStrategy.Template.Day.name()));
        stream.setDimensions(getDefaultDimensions());
        stream.setAttributeDerivers(singletonList(deriver(COUNT)));
        return stream;
    }

    private List<StreamDimension> getDefaultDimensions() {
        AtlasStream stream = dummyStream();
        return Arrays.asList(sourceMediumDimension(stream), WebVisitUtils.userIdDimension(stream));
    }

    private AtlasStream dummyStream() {
        AtlasStream stream = new AtlasStream();
        stream.setTenant(new Tenant(getClass().getSimpleName() + "_" + UUID.randomUUID().toString()));
        return stream;
    }

    private List<StreamAttributeDeriver> getDefaultAttrDerivers() {
        StreamAttributeDeriver d1 = new StreamAttributeDeriver();
        d1.setSourceAttributes(Arrays.asList("c1", "c2"));
        d1.setTargetAttribute("t1");
        d1.setCalculation(COUNT);
        return singletonList(d1);
    }
}
