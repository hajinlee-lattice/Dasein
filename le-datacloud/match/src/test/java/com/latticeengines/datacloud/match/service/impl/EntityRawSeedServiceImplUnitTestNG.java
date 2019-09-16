package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.equalsDisregardPriority;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.merge;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.newSeed;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_NETFLIX_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_NETFLIX_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_GOOGLE_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_NETFLIX_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_NETFLIX_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.Seed.EMPTY;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;

public class EntityRawSeedServiceImplUnitTestNG {

    private static final String TEST_SEED_ID = "entity_raw_seed_unit_test_seed_id";
    private static final int NUM_SHARDS = 3;
    private static final String TEST_EXTRA_ATTRIBUTE_KEY = "key";
    private static final String TEST_EXTRA_ATTRIBUTE_VALUE = "val";

    private EntityRawSeedServiceImpl service;

    @BeforeClass(groups = "unit")
    private void setup() {
        // instantiate required services
        EntityMatchConfigurationService configurationService = Mockito.mock(EntityMatchConfigurationService.class);
        Mockito.when(configurationService.getNumShards(Mockito.any())).thenReturn(NUM_SHARDS);
        service = new EntityRawSeedServiceImpl(null, configurationService);
    }

    /*
     * test the method to retrieve all dynamo attributes with string type (S) and string set type (SS)
     */
    @Test(groups = "unit", dataProvider = "seedAttributes")
    private void testGetStringAttributes(
            @NotNull EntityRawSeed seed, @NotNull Map<String, String> expectedStringAttributes,
            @NotNull Map<String, Set<String>> expectedStringSetAttributes) {
        // string attributes
        Map<String, String> strAttributes = service.getStringAttributes(seed);
        Assert.assertNotNull(strAttributes);
        Assert.assertEquals(strAttributes, expectedStringAttributes);
        // string set attributes
        Map<String, Set<String>> strSetAttributes = service.getStringSetAttributes(seed);
        Assert.assertNotNull(strSetAttributes);
        Assert.assertEquals(strSetAttributes, expectedStringSetAttributes);

        // add default extra attribute (will only affect string attributes)
        seed = addExtra(seed);
        expectedStringAttributes.put(service.buildSeedAttrName(TEST_EXTRA_ATTRIBUTE_KEY), TEST_EXTRA_ATTRIBUTE_VALUE);
        strAttributes = service.getStringAttributes(seed);
        Assert.assertNotNull(strAttributes);
        Assert.assertEquals(strAttributes, expectedStringAttributes);
        // string set attributes (not changed)
        strSetAttributes = service.getStringSetAttributes(seed);
        Assert.assertNotNull(strSetAttributes);
        Assert.assertEquals(strSetAttributes, expectedStringSetAttributes);
    }

    @Test(groups = "unit", dataProvider = "fromAttributeMap")
    private void testFromAttributeMap(@NotNull EntityRawSeed expectedSeed) {
        Map<String, AttributeValue> attrMap = toAttributeValueMap(expectedSeed);
        Assert.assertNotNull(attrMap);
        EntityRawSeed seed = service.fromAttributeMap(attrMap);
        Assert.assertNotNull(seed);
        Assert.assertTrue(equalsDisregardPriority(seed, expectedSeed));
    }

    @Test(groups = "unit", dataProvider = "fromItem")
    private void testFromItem(EntityRawSeed expectedSeed) {
        if (expectedSeed != null) {
            Item item = toDynamoItem(expectedSeed);
            Assert.assertNotNull(item);
            EntityRawSeed seed = service.fromItem(item);
            Assert.assertNotNull(seed);
            Assert.assertTrue(equalsDisregardPriority(seed, expectedSeed));
        } else {
            Assert.assertNull(service.fromItem(null));
        }
    }

    @DataProvider(name = "seedAttributes")
    private Object[][] provideStringAttributeTestData() {
        return new Object[][] {
                {
                        EMPTY,
                        strAttrMap(new EntityLookupEntry[0], new String[0]),
                        strSetAttrMap()
                },
                /*
                 * lookup entries
                 */
                // mixing string & string set entries
                {
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_FACEBOOK_1, DC_FACEBOOK_4,
                                NC_GOOGLE_1, NC_FACEBOOK_1, DUNS_1, SFDC_4, MKTO_3, ELOQUA_4),
                        strAttrMap(new EntityLookupEntry[] {
                                DUNS_1, SFDC_4, MKTO_3, ELOQUA_4
                        }, new String[0]),
                        strSetAttrMap(DC_GOOGLE_1, DC_GOOGLE_2, DC_FACEBOOK_1,
                                DC_FACEBOOK_4, NC_GOOGLE_1, NC_FACEBOOK_1)
                },
                {
                        newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_FACEBOOK_1, DC_NETFLIX_1, NC_NETFLIX_1),
                        strAttrMap(new EntityLookupEntry[0], new String[0]),
                        strSetAttrMap(DC_GOOGLE_1, DC_GOOGLE_2, DC_FACEBOOK_1, DC_NETFLIX_1, NC_NETFLIX_1)
                },
                {
                        newSeed(TEST_SEED_ID, DUNS_3, SFDC_2, DC_NETFLIX_2, DC_FACEBOOK_2),
                        strAttrMap(new EntityLookupEntry[] { DUNS_3, SFDC_2 }, new String[0]),
                        strSetAttrMap(DC_NETFLIX_2, DC_FACEBOOK_2)
                },
                // string set entries only
                {
                        newSeed(TEST_SEED_ID, DC_FACEBOOK_3),
                        strAttrMap(new EntityLookupEntry[0], new String[0]),
                        strSetAttrMap(DC_FACEBOOK_3)
                },
                {
                        newSeed(TEST_SEED_ID, NC_NETFLIX_2),
                        strAttrMap(new EntityLookupEntry[0], new String[0]),
                        strSetAttrMap(NC_NETFLIX_2)
                },
                // string entries only
                {
                        newSeed(TEST_SEED_ID, DUNS_5, SFDC_5, MKTO_1, ELOQUA_2),
                        strAttrMap(new EntityLookupEntry[] {
                                DUNS_5, SFDC_5, MKTO_1, ELOQUA_2
                        }, new String[0]),
                        strSetAttrMap()
                },
                {
                        newSeed(TEST_SEED_ID, DUNS_2),
                        strAttrMap(new EntityLookupEntry[] { DUNS_2 }, new String[0]),
                        strSetAttrMap()
                },
                {
                        newSeed(TEST_SEED_ID, SFDC_4),
                        strAttrMap(new EntityLookupEntry[] { SFDC_4 }, new String[0]),
                        strSetAttrMap()
                },
                {
                        newSeed(TEST_SEED_ID, ELOQUA_1),
                        strAttrMap(new EntityLookupEntry[] { ELOQUA_1 }, new String[0]),
                        strSetAttrMap()
                },
                {
                        newSeed(TEST_SEED_ID, ELOQUA_2, DUNS_3),
                        strAttrMap(new EntityLookupEntry[] { ELOQUA_2, DUNS_3 }, new String[0]),
                        strSetAttrMap()
                },
                {
                        newSeed(TEST_SEED_ID, ELOQUA_2, SFDC_5),
                        strAttrMap(new EntityLookupEntry[] { ELOQUA_2, SFDC_5 }, new String[0]),
                        strSetAttrMap()
                },
                /*
                 * extra attributes
                 */
                {
                        newSeed(TEST_SEED_ID, TEST_EXTRA_ATTRIBUTE_KEY, TEST_EXTRA_ATTRIBUTE_VALUE),
                        strAttrMap(new EntityLookupEntry[0], new String[] { TEST_EXTRA_ATTRIBUTE_KEY }),
                        strSetAttrMap()
                },
                {
                        newSeed(TEST_SEED_ID,
                                "key1", TEST_EXTRA_ATTRIBUTE_VALUE,
                                "key2", TEST_EXTRA_ATTRIBUTE_VALUE,
                                "key3", TEST_EXTRA_ATTRIBUTE_VALUE),
                        strAttrMap(new EntityLookupEntry[0], new String[] { "key1", "key2", "key3" }),
                        strSetAttrMap()
                },
        };
    }


    @DataProvider(name = "fromAttributeMap")
    private Object[][] provideFromAttributeMapTestData() {
        return combineLookupEntryAndAttributeSeeds();
    }

    @DataProvider(name = "fromItem")
    private Object[][] provideFromItemTestData() {
        Object[][] allCombination = combineLookupEntryAndAttributeSeeds();
        Object[][] res = new Object[allCombination.length + 1][];
        System.arraycopy(allCombination, 0, res, 0, allCombination.length);
        // null seed
        res[allCombination.length] = new Object[] { null };
        return res;
    }

    private Object[][] combineLookupEntryAndAttributeSeeds() {
        Object[][] lookupOnly = getLookupEntryOnlySeeds();
        Object[][] attributeOnly = getExtraAttributeOnlySeeds();
        Object[][] both = new Object[lookupOnly.length * attributeOnly.length][];
        // get every combination of lookup * attribute
        for (int i = 0; i < lookupOnly.length; i++) {
            for (int j = 0; j < attributeOnly.length; j++) {
                EntityRawSeed seed = merge((EntityRawSeed) lookupOnly[i][0], (EntityRawSeed) attributeOnly[j][0]);
                both[i * attributeOnly.length + j] = new Object[] { seed };
            }
        }
        Object[][] res = new Object[lookupOnly.length + attributeOnly.length + both.length][];
        System.arraycopy(lookupOnly, 0, res, 0, lookupOnly.length);
        System.arraycopy(attributeOnly, 0, res, lookupOnly.length, attributeOnly.length);
        System.arraycopy(both, 0, res, attributeOnly.length, both.length);
        return res;
    }

    private Object[][] getLookupEntryOnlySeeds() {
        return new Object[][] {
                { EMPTY },
                /*
                 * domain/country only
                 */
                // all with same domain
                { newSeed(TEST_SEED_ID, DC_FACEBOOK_1, DC_FACEBOOK_2, DC_FACEBOOK_3) },
                // multiple domains
                { newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_FACEBOOK_1, DC_FACEBOOK_4) },
                { newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_3, DC_FACEBOOK_1,
                        DC_FACEBOOK_2, DC_FACEBOOK_4, DC_NETFLIX_1) },
                /*
                 * name/country only
                 */
                // all with same company name
                { newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_GOOGLE_3, NC_GOOGLE_4) },
                // multiple companies
                { newSeed(TEST_SEED_ID, NC_GOOGLE_3, NC_GOOGLE_4, NC_FACEBOOK_3, NC_FACEBOOK_1, NC_FACEBOOK_2) },
                { newSeed(TEST_SEED_ID, NC_GOOGLE_1, NC_GOOGLE_2, NC_FACEBOOK_1, NC_NETFLIX_1, NC_NETFLIX_2) },
                /*
                 * duns only (should have at most one DUNS per seed)
                 */
                { newSeed(TEST_SEED_ID, DUNS_1) },
                { newSeed(TEST_SEED_ID, DUNS_2) },
                { newSeed(TEST_SEED_ID, DUNS_3) },
                /*
                 * external system only (should have at most one ID per system)
                 */
                { newSeed(TEST_SEED_ID, SFDC_1) },
                { newSeed(TEST_SEED_ID, SFDC_2) },
                { newSeed(TEST_SEED_ID, SFDC_1, MKTO_1) },
                { newSeed(TEST_SEED_ID, SFDC_3, MKTO_1, ELOQUA_1) },
                { newSeed(TEST_SEED_ID, MKTO_2, ELOQUA_4) },
                /*
                 * all types
                 */
                { newSeed(TEST_SEED_ID, DC_GOOGLE_1, DC_GOOGLE_2, DC_FACEBOOK_1, DC_FACEBOOK_4,
                        NC_GOOGLE_1, NC_FACEBOOK_1, DUNS_1, SFDC_4, MKTO_3, ELOQUA_4) },
        };
    }

    private Object[][] getExtraAttributeOnlySeeds() {
        return new Object[][] {
                { newSeed(TEST_SEED_ID, "key1", "val1") },
                { newSeed(TEST_SEED_ID, "key1", "val1", "key2", "val2", "key3", "val3") },
        };
    }

    /*
     * helper to copy input seed and add the testing extra attribute to the copy
     */
    private EntityRawSeed addExtra(@NotNull EntityRawSeed seed) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(TEST_EXTRA_ATTRIBUTE_KEY, TEST_EXTRA_ATTRIBUTE_VALUE);
        if (MapUtils.isNotEmpty(seed.getAttributes())) {
            attrs.putAll(seed.getAttributes());
        }
        return new EntityRawSeed(seed.getId(), seed.getEntity(), seed.getLookupEntries(), attrs);
    }

    /*
     * helpers to build dynamo related classes for testing
     *
     * always use getStringAttributes & getStringSetAttributes for building dynamo attribute name/value
     */

    private Item toDynamoItem(@NotNull EntityRawSeed seed) {
        Item item = new Item()
                .withString(DataCloudConstants.ENTITY_ATTR_SEED_ID, seed.getId())
                .withString(DataCloudConstants.ENTITY_ATTR_ENTITY, seed.getEntity())
                .withNumber(DataCloudConstants.ENTITY_ATTR_EXPIRED_AT, System.currentTimeMillis() / 1000)
                .withNumber(DataCloudConstants.ENTITY_ATTR_VERSION, 0L);

        Map<String, String> strings = service.getStringAttributes(seed);
        Map<String, Set<String>> stringSets = service.getStringSetAttributes(seed);
        Assert.assertNotNull(strings);
        Assert.assertNotNull(stringSets);
        strings.forEach(item::withString);
        stringSets.forEach(item::withStringSet);
        return item;
    }

    private Map<String, AttributeValue> toAttributeValueMap(@NotNull EntityRawSeed seed) {
        Map<String, String> strings = service.getStringAttributes(seed);
        Map<String, Set<String>> stringSets = service.getStringSetAttributes(seed);
        Assert.assertNotNull(strings);
        Assert.assertNotNull(stringSets);
        Stream<Pair<String, AttributeValue>> strAttrs = strings
                .entrySet()
                .stream()
                .map(entry -> {
                    AttributeValue value = new AttributeValue();
                    value.setS(entry.getValue());
                    return Pair.of(entry.getKey(), value);
                });
        Stream<Pair<String, AttributeValue>> strSetAttrs = stringSets
                .entrySet()
                .stream()
                .map(entry -> {
                    AttributeValue value = new AttributeValue();
                    value.setSS(entry.getValue());
                    return Pair.of(entry.getKey(), value);
                });
        // combine all
        return Stream.of(getBaseAttributeValues(seed), strAttrs, strSetAttrs)
                .reduce(Stream::concat)
                .get()
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private Stream<Pair<String, AttributeValue>> getBaseAttributeValues(@NotNull EntityRawSeed seed) {
        AttributeValue seedIdValue = new AttributeValue();
        seedIdValue.setS(seed.getId());
        AttributeValue entityValue = new AttributeValue();
        entityValue.setS(seed.getEntity());
        return Stream.of(
                Pair.of(DataCloudConstants.ENTITY_ATTR_SEED_ID, seedIdValue),
                Pair.of(DataCloudConstants.ENTITY_ATTR_ENTITY, entityValue));
    }

    /*
     * NOTE use buildAttrPairFromLookupEntry for building dynamo key/value for lookup entry
     * NOTE use buildSeedAttrName for building dynamo key/value for extra attributes
     */
    private Map<String, String> strAttrMap(
            @NotNull EntityLookupEntry[] lookupEntries, @NotNull String[] extraAttrNames) {
        Map<String, String> map = Arrays
                .stream(lookupEntries)
                .map(service::buildAttrPairFromLookupEntry)
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
        Arrays.stream(extraAttrNames)
                .map(service::buildSeedAttrName)
                .forEach(key -> map.put(key, TEST_EXTRA_ATTRIBUTE_VALUE));
        return map;
    }

    /*
     * use buildAttrPairFromLookupEntry for building dynamo key/value for lookup entry and aggregated as string set
     */
    private Map<String, Set<String>> strSetAttrMap(@NotNull EntityLookupEntry... lookupEntries) {
        return Arrays.stream(lookupEntries)
                .map(service::buildAttrPairFromLookupEntry)
                .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toSet())));
    }
}
