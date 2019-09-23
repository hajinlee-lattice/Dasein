package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
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
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class DimensionEntityMgrTestNG extends ActivityRelatedEntityMgrImplTestNGBase {
    private static final String CATALOG_WEBVISIT = "WebVisitPathPatterns";
    private static final List<String> CATALOG_NAMES = Arrays.asList(CATALOG_WEBVISIT);

    private static final String STREAM_WEBVISIT = "WebVisit";
    private static final String STREAM_OPP = "Oppotunity";
    private static final List<String> STREAM_NAMES = Arrays.asList(STREAM_WEBVISIT, STREAM_OPP);

    private static final InterfaceName DIM_PATH_PATTERN_ID = InterfaceName.PathPatternId;
    private static final InterfaceName DIM_LEAD_SOURCE = InterfaceName.LeadSource;
    private static final InterfaceName DIM_IS_CLOSED = InterfaceName.IsClosed;

    private Map<InterfaceName, Dimension> dimensions = new HashMap<>();
    private List<StreamAttributeDeriver> derivers = getAttributeDerivers();

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
            Dimension dimension = dimensionEntityMgr.findByNameAndTenantAndStream(d.getName(), mainTestTenant, d.getStream());
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
            Assert.assertTrue(CollectionUtils.isEmpty(dimension.getAttributeDerivers()));
        } else {
            Assert.assertNotNull(dimension.getCatalog());
            Assert.assertNotNull(dimension.getAttributeDerivers());
            Assert.assertEquals(dimension.getAttributeDerivers().size(), derivers.size());
        }
        Assert.assertNotNull(dimension.getGenerator());
        Assert.assertNotNull(dimension.getCalculator());
    }

    private Dimension getWebVisitDimension() {
        Dimension dimension = new Dimension();
        dimension.setName(DIM_PATH_PATTERN_ID);
        dimension.setDisplayName(dimension.getName().name());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_WEBVISIT));
        dimension.setCatalog(catalogs.get(CATALOG_WEBVISIT));

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.PathPatternName);
        generator.setFromCatalog(true);
        generator.setOption(DimensionGeneratorOption.HASH);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(InterfaceName.WebVisitPageUrl);
        calculator.setOption(DimensionCalculatorOption.REGEX);
        calculator.setPatternAttribute(InterfaceName.PathPattern);
        calculator.setPatternFromCatalog(true);
        dimension.setCalculator(calculator);

        dimension.setAttributeDerivers(derivers);
        return dimension;
    }

    private Dimension getLeadSourceDimension() {
        Dimension dimension = new Dimension();
        dimension.setName(DIM_LEAD_SOURCE);
        dimension.setDisplayName(dimension.getName().name());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_OPP));
        dimension.setCatalog(catalogs.get(STREAM_OPP));

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.LeadSource);
        generator.setFromCatalog(false);
        generator.setOption(DimensionGeneratorOption.ENUM);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(InterfaceName.LeadSource);
        calculator.setOption(DimensionCalculatorOption.EXACT_MATCH);
        dimension.setCalculator(calculator);

        return dimension;
    }

    private Dimension getIsClosedDimension() {
        Dimension dimension = new Dimension();
        dimension.setName(DIM_IS_CLOSED);
        dimension.setDisplayName(dimension.getName().name());
        dimension.setTenant(mainTestTenant);
        dimension.setStream(streams.get(STREAM_OPP));
        dimension.setCatalog(catalogs.get(STREAM_OPP));

        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.IsClosed);
        generator.setFromCatalog(false);
        generator.setOption(DimensionGeneratorOption.BOOLEAN);
        dimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(InterfaceName.IsClosed);
        calculator.setOption(DimensionCalculatorOption.EXACT_MATCH);
        dimension.setCalculator(calculator);

        return dimension;
    }

    // Fake some derived attributes
    private List<StreamAttributeDeriver> getAttributeDerivers() {
        StreamAttributeDeriver deriver1 = new StreamAttributeDeriver();
        deriver1.setCalculation(StreamAttributeDeriver.Calculation.COUNT);
        deriver1.setSourceAttributes(Arrays.asList(InterfaceName.ContactId));
        deriver1.setTargetAttribute(InterfaceName.NumberOfContacts);

        StreamAttributeDeriver deriver2 = new StreamAttributeDeriver();
        deriver2.setCalculation(StreamAttributeDeriver.Calculation.SUM);
        deriver2.setSourceAttributes(Arrays.asList(InterfaceName.Amount));
        deriver2.setTargetAttribute(InterfaceName.TotalAmount);

        return Arrays.asList(deriver1, deriver2);
    }
}
