package com.latticeengines.apps.cdl.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.IsClosed;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LeadSource;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataIntegrityViolationException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.activity.Dimension;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator.DimensionCalculatorOption;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class DimensionEntityMgrTestNG extends ActivityRelatedEntityMgrImplTestNGBase {
    private static final String CATALOG_WEBVISIT = "WebVisitPathPatterns";
    private static final List<String> CATALOG_NAMES = Arrays.asList(CATALOG_WEBVISIT);

    private static final String STREAM_WEBVISIT = "WebVisit";
    private static final String STREAM_OPP = "Oppotunity";
    private static final List<String> STREAM_NAMES = Arrays.asList(STREAM_WEBVISIT, STREAM_OPP);

    private static final String DIM_PATH_PATTERN_ID = PathPatternId.name();
    private static final String DIM_LEAD_SOURCE = LeadSource.name();
    private static final String DIM_IS_CLOSED = IsClosed.name();

    private Map<String, Dimension> dimensions = new HashMap<>();

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
    public void testCreate(Dimension dimension) {
        dimensionEntityMgr.create(dimension);
        dimensions.put(dimension.getName(), dimension);
        Assert.assertNotNull(dimension.getPid());
    }

    // [ Name + Stream + Tenant ] need to be unique
    @Test(groups = "functional", dependsOnMethods = "testCreate", expectedExceptions = {
            DataIntegrityViolationException.class })
    public void testCreateConflict() {
        Dimension dimension = getWebVisitDimension();
        dimensionEntityMgr.create(dimension);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testFind() {
        for (Dimension d : dimensions.values()) {
            Dimension dimension = dimensionEntityMgr.findByNameAndTenantAndStream(d.getName(), mainTestTenant,
                    d.getStream());
            validateDimension(dimension);
        }
        Assert.assertEquals(dimensionEntityMgr.findByTenant(mainTestTenant).size(), dimensions.size());
    }

    @DataProvider(name = "Dimensions")
    protected Object[][] provideDimensions() {
        return new Object[][] { //
                { getWebVisitDimension() }, //
                { getLeadSourceDimension() }, //
                { getIsClosedDimension() }, //
        };
    }

    private void validateDimension(Dimension dimension) {
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
    }

    private Dimension getWebVisitDimension() {
        Dimension dimension = new Dimension();
        dimension.setName(DIM_PATH_PATTERN_ID);
        dimension.setDisplayName(dimension.getName());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_WEBVISIT));
        dimension.setCatalog(catalogs.get(CATALOG_WEBVISIT));

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(PathPatternName.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGeneratorOption.HASH);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(WebVisitPageUrl.name());
        calculator.setOption(DimensionCalculatorOption.REGEX);
        calculator.setPatternAttribute(PathPattern.name());
        calculator.setPatternFromCatalog(true);
        dimension.setCalculator(calculator);

        return dimension;
    }

    private Dimension getLeadSourceDimension() {
        Dimension dimension = new Dimension();
        dimension.setName(DIM_LEAD_SOURCE);
        dimension.setDisplayName(dimension.getName());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_OPP));
        dimension.setCatalog(catalogs.get(STREAM_OPP));

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(LeadSource.name());
        generator.setFromCatalog(false);
        generator.setOption(DimensionGeneratorOption.ENUM);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(LeadSource.name());
        calculator.setOption(DimensionCalculatorOption.EXACT_MATCH);
        dimension.setCalculator(calculator);

        return dimension;
    }

    private Dimension getIsClosedDimension() {
        Dimension dimension = new Dimension();
        dimension.setName(DIM_IS_CLOSED);
        dimension.setDisplayName(dimension.getName());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_OPP));
        dimension.setCatalog(catalogs.get(STREAM_OPP));

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(IsClosed.name());
        generator.setFromCatalog(false);
        generator.setOption(DimensionGeneratorOption.BOOLEAN);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(IsClosed.name());
        calculator.setOption(DimensionCalculatorOption.EXACT_MATCH);
        dimension.setCalculator(calculator);

        return dimension;
    }
}
