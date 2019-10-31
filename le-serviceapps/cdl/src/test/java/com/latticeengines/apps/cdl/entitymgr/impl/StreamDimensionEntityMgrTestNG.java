package com.latticeengines.apps.cdl.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.IsClosed;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LeadSource;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class StreamDimensionEntityMgrTestNG extends ActivityRelatedEntityMgrImplTestNGBase {
    private static final String CATALOG_WEBVISIT = "WebVisitPathPatterns";
    private static final List<String> CATALOG_NAMES = Arrays.asList(CATALOG_WEBVISIT);

    private static final String STREAM_WEBVISIT = "WebVisit";
    private static final String STREAM_OPP = "Oppotunity";
    private static final List<String> STREAM_NAMES = Arrays.asList(STREAM_WEBVISIT, STREAM_OPP);

    private static final String DIM_PATH_PATTERN_ID = PathPatternId.name();
    private static final String DIM_LEAD_SOURCE = LeadSource.name();
    private static final String DIM_IS_CLOSED = IsClosed.name();

    @Inject
    private DataCollectionStatusEntityMgr dataCollectionStatusEntityMgr;

    private Map<String, StreamDimension> dimensions = new HashMap<>();
    // stream name -> associated lsit of dimensions
    private Map<String, List<StreamDimension>> dimensionMap = new HashMap<>();
    @Override
    protected List<String> getCatalogNames() {
        return CATALOG_NAMES;
    };

    @Override
    protected List<String> getStreamNames() {
        return STREAM_NAMES;
    };


    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        prepareDataFeed();
        prepareCatalog();
        prepareStream();
    }

    @Test(groups = "functional", dataProvider = "Dimensions")
    public void testCreate(StreamDimension dimension) {
        dimensionEntityMgr.create(dimension);
        dimensions.put(dimension.getName(), dimension);
        Assert.assertNotNull(dimension.getPid());

        String streamName = dimension.getStream().getName();
        dimensionMap.putIfAbsent(streamName, new ArrayList<>());
        dimensionMap.get(streamName).add(dimension);

        // make sure stream & dimension can be saved into datacollection status
        DataCollectionStatus status = new DataCollectionStatus();
        status.setTenant(mainTestTenant);
        status.setDataCollection(dataCollection);
        status.setVersion(DataCollection.Version.Blue);
        status.setAccountCount(10L);
        status.setActivityStreamMap(streams);
        dataCollectionStatusEntityMgr.createOrUpdate(status);
    }

    // [ Name + Stream + Tenant ] need to be unique
    @Test(groups = "functional", dependsOnMethods = "testCreate", expectedExceptions = {
            DataIntegrityViolationException.class })
    public void testCreateConflict() {
        StreamDimension dimension = getWebVisitDimension();
        dimensionEntityMgr.create(dimension);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testFind() {
        for (StreamDimension d : dimensions.values()) {
            StreamDimension dimension = dimensionEntityMgr.findByNameAndTenantAndStream(d.getName(), mainTestTenant,
                    d.getStream());
            validateDimension(dimension);
        }
        Assert.assertEquals(dimensionEntityMgr.findByTenant(mainTestTenant).size(), dimensions.size());
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testDimensionInflation() {
        List<AtlasStream> streams = streamEntityMgr.findByTenant(mainTestTenant, true);
        Assert.assertNotNull(streams);
        Set<String> streamNames = streams.stream().map(AtlasStream::getName).collect(Collectors.toSet());
        Assert.assertEquals(streamNames, new HashSet<>(getStreamNames()));

        streams.forEach(stream -> {
            if (dimensionMap.containsKey(stream.getName())) {
                Assert.assertNotNull(stream.getDimensions());
                Assert.assertEquals(stream.getDimensions().size(), dimensionMap.get(stream.getName()).size(),
                        String.format("Stream %s should have one dimension", stream.getName()));
            } else {
                Assert.assertTrue(CollectionUtils.isEmpty(stream.getDimensions()));
            }
        });
    }

    @DataProvider(name = "Dimensions")
    protected Object[][] provideDimensions() {
        return new Object[][] { //
                { getWebVisitDimension() }, //
                { getLeadSourceDimension() }, //
                { getIsClosedDimension() }, //
        };
    }

    private void validateDimension(StreamDimension dimension) {
        Assert.assertNotNull(dimension);
        Assert.assertNotNull(dimension.getName());
        Assert.assertNotNull(dimension.getDisplayName());
        Assert.assertNotNull(dimension.getTenant());
        Assert.assertNotNull(dimension.getStream());
        if (STREAM_OPP.equals(dimension.getStream().getName())) {
            Assert.assertNull(dimension.getCatalog());
        } else {
            Assert.assertNotNull(dimension.getCatalog());
        }
        Assert.assertNotNull(dimension.getGenerator());
        Assert.assertNotNull(dimension.getCalculator());
        if (STREAM_WEBVISIT.equals(dimension.getStream().getName())) {
            Assert.assertTrue(dimension.getCalculator() instanceof DimensionCalculatorRegexMode);
        } else {
            Assert.assertNotNull(dimension.getCalculator());
        }
    }

    private StreamDimension getWebVisitDimension() {
        StreamDimension dimension = new StreamDimension();
        dimension.setName(DIM_PATH_PATTERN_ID);
        dimension.setDisplayName(dimension.getName());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_WEBVISIT));
        dimension.setCatalog(catalogs.get(CATALOG_WEBVISIT));
        dimension.addUsages(StreamDimension.Usage.Pivot);

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(PathPatternName.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGeneratorOption.HASH);
        dimension.setGenerator(generator);

        DimensionCalculatorRegexMode calculator = new DimensionCalculatorRegexMode();
        calculator.setAttribute(WebVisitPageUrl.name());
        calculator.setPatternAttribute(PathPattern.name());
        calculator.setPatternFromCatalog(true);
        dimension.setCalculator(calculator);

        return dimension;
    }

    private StreamDimension getLeadSourceDimension() {
        StreamDimension dimension = new StreamDimension();
        dimension.setName(DIM_LEAD_SOURCE);
        dimension.setDisplayName(dimension.getName());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_OPP));
        dimension.setCatalog(catalogs.get(STREAM_OPP));
        dimension.addUsages(StreamDimension.Usage.Pivot, StreamDimension.Usage.Filter);

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(LeadSource.name());
        generator.setFromCatalog(false);
        generator.setOption(DimensionGeneratorOption.ENUM);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(LeadSource.name());
        dimension.setCalculator(calculator);

        return dimension;
    }

    private StreamDimension getIsClosedDimension() {
        StreamDimension dimension = new StreamDimension();
        dimension.setName(DIM_IS_CLOSED);
        dimension.setDisplayName(dimension.getName());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_OPP));
        dimension.setCatalog(catalogs.get(STREAM_OPP));
        dimension.addUsages(StreamDimension.Usage.Pivot, StreamDimension.Usage.Filter, StreamDimension.Usage.Dedup);

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(IsClosed.name());
        generator.setFromCatalog(false);
        generator.setOption(DimensionGeneratorOption.BOOLEAN);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(IsClosed.name());
        dimension.setCalculator(calculator);

        return dimension;
    }
}
