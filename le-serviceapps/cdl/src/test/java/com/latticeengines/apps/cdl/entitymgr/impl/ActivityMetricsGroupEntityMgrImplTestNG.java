package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.repository.reader.StringTemplateReaderRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.StringTemplate;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({SimpleRetryListener.class})
public class ActivityMetricsGroupEntityMgrImplTestNG extends ActivityRelatedEntityMgrImplTestNGBase {

    private static ActivityMetricsGroup TEST_GROUP_TOTAL_VISIT;
    private static AtlasStream STREAM_WEBVISIT;

    private static final String PERIOD = PeriodStrategy.Template.Week.name();

    private static final String GROUPNAME_TOTAL_VISIT = "Total Web Visits";
    private static final String STREAM_NAME_WEBVISIT = "WebVisit";
    private static final String ROLLUP_DIM = "rollupDim";
    private static final String DISPLAY_NAME_TMPL = StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME;
    private static final String SUBCATEGORY_TMPL = StringTemplateConstants.ACTIVITY_METRICS_GROUP_SUBCATEGORY;
    private static final String JAVA_CLASS_LONG = Long.class.getSimpleName();
    private static final NullMetricsImputation NULL_IMPUTATION = NullMetricsImputation.ZERO;

    private static Map<String, StringTemplate> templateCache = new HashMap<>();

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

    @Inject
    private StringTemplateReaderRepository stringTemplateReaderRepository;

    @BeforeClass(groups = "functional")
    public void setupData() {
        setupTestEnvironmentWithDataCollection();
        prepareDataFeed();
        prepareCatalog();
        prepareStream();

        STREAM_WEBVISIT = createStream(STREAM_NAME_WEBVISIT);
        TEST_GROUP_TOTAL_VISIT = prepareMetricsGroup(GROUPNAME_TOTAL_VISIT, STREAM_WEBVISIT);

        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "functional", dataProvider = "testGroupsDataProvider", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testCreate(ActivityMetricsGroup group, ActivityMetricsGroup expected) {
        activityMetricsGroupEntityMgr.createOrUpdate(group);
        Assert.assertNotNull(group.getPid());
        validateGroupsEqual(group, expected);
    }

    @Test(groups = "functional", dataProvider = "testGroupsDataProvider", dependsOnMethods = "testCreate")
    public void testGetByPid(ActivityMetricsGroup group, ActivityMetricsGroup expected) {
        ActivityMetricsGroup retrived = activityMetricsGroupEntityMgr.findByPid(group.getPid());
        Assert.assertNotNull(retrived);
        validateGroupsEqual(group, retrived);
    }

    @Test(groups = "functional", dataProvider = "testGroupsDataProvider", dependsOnMethods = "testCreate")
    public void testGetByGroupId(ActivityMetricsGroup group, ActivityMetricsGroup expected) {
        ActivityMetricsGroup retrived = activityMetricsGroupEntityMgr.findByGroupId(group.getGroupId());
        Assert.assertNotNull(retrived);
        validateGroupsEqual(group, retrived);
    }

    @Test(groups = "functional", dataProvider = "groupNamesDataProvider", dependsOnMethods = "testCreate")
    public void testGetNextAvailableGroupId(String groupName, String expected) {
        String groupIdBase = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(groupName);
        String nextAvailable = activityMetricsGroupEntityMgr.getNextAvailableGroupId(groupIdBase);
        Assert.assertEquals(nextAvailable, expected);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testFindByTenant() {
        List<ActivityMetricsGroup> groups = activityMetricsGroupEntityMgr.findByTenant(mainTestTenant);
        Assert.assertNotNull(groups);
        Assert.assertEquals(groups.size(), 1);
        groups.forEach(g -> Assert.assertEquals(g.getTenant().getPid(), mainTestTenant.getPid()));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testFindByStream() {
        List<ActivityMetricsGroup> groups = activityMetricsGroupEntityMgr.findByStream(STREAM_WEBVISIT);
        Assert.assertNotNull(groups);
        Assert.assertEquals(groups.size(), 1);
        groups.forEach(g -> Assert.assertEquals(g.getStream().getPid(), STREAM_WEBVISIT.getPid()));
    }

    @DataProvider(name = "testGroupsDataProvider")
    public Object[][] testGroupsDataProvider() {
        return new Object[][]{
                {TEST_GROUP_TOTAL_VISIT, prepareMetricsGroup(GROUPNAME_TOTAL_VISIT, STREAM_WEBVISIT)}};
    }

    @DataProvider(name = "groupNamesDataProvider")
    public Object[][] groupNameDataProvider() {
        return new Object[][]{{GROUPNAME_TOTAL_VISIT,
                ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(GROUPNAME_TOTAL_VISIT) + "1"}};
    }

    private ActivityTimeRange createTimeRollup() {
        ActivityTimeRange activityTimeRange = new ActivityTimeRange();
        activityTimeRange.setOperator(ComparisonType.WITHIN);
        activityTimeRange.setParamSet(new HashSet<>(Collections.singletonList(Collections.singletonList(2))));
        activityTimeRange.setPeriods(new HashSet<>(Collections.singletonList(PERIOD)));
        return activityTimeRange;
    }

    private ActivityMetricsGroup prepareMetricsGroup(String groupName, AtlasStream stream) {
        ActivityTimeRange activityTimeRange = createTimeRollup();
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setGroupName(groupName);
        group.setGroupId(ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(groupName));
        group.setTenant(mainTestTenant);
        group.setStream(stream);
        group.setEntity(BusinessEntity.Account);
        group.setRollupDimensions(ROLLUP_DIM);
        group.setAggregation(stream.getAttributeDerivers().get(0));
        group.setActivityTimeRange(activityTimeRange);
        group.setDisplayNameTmpl(getTemplate(DISPLAY_NAME_TMPL));
        group.setCategory(Category.WEBSITE_PROFILE);
        group.setSubCategoryTmpl(getTemplate(SUBCATEGORY_TMPL));
        group.setJavaClass(JAVA_CLASS_LONG);
        group.setNullImputation(NULL_IMPUTATION);

        return group;
    }

    private void validateGroupsEqual(ActivityMetricsGroup group, ActivityMetricsGroup retrieved) {
        Assert.assertEquals(group.getGroupId(), retrieved.getGroupId());
        Assert.assertEquals(group.getGroupName(), retrieved.getGroupName());
        Assert.assertNotNull(group.getTenant());
        Assert.assertNotNull(retrieved.getTenant());
        Assert.assertNotNull(group.getStream());
        Assert.assertNotNull(retrieved.getStream());
        Assert.assertEquals(group.getStream().getPid(), retrieved.getStream().getPid());
        Assert.assertEquals(group.getTenant().getPid(), retrieved.getTenant().getPid());
        Assert.assertEquals(group.getEntity(), retrieved.getEntity());
        Assert.assertEquals(group.getRollupDimensions(), retrieved.getRollupDimensions());
        Assert.assertEquals(group.getAggregation().getCalculation(), retrieved.getAggregation().getCalculation());
        Assert.assertEquals(group.getDisplayNameTmpl().getTemplate(), retrieved.getDisplayNameTmpl().getTemplate());
        Assert.assertEquals(group.getCategory(), retrieved.getCategory());
        Assert.assertEquals(group.getSubCategoryTmpl().getTemplate(), retrieved.getSubCategoryTmpl().getTemplate());
        Assert.assertEquals(group.getJavaClass(), retrieved.getJavaClass());
        Assert.assertEquals(group.getNullImputation(), retrieved.getNullImputation());
    }

    private StringTemplate getTemplate(String name) {
        if (!templateCache.containsKey(name)) {
            templateCache.put(name, stringTemplateReaderRepository.findByName(name));
        }
        StringTemplate tmpl = templateCache.get(name);
        if (tmpl == null) {
            throw new IllegalStateException(String.format("Default template %s is not added to database", name));
        }
        return tmpl;
    }
}
