package com.latticeengines.datacloud.match.testframework;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDomainCountry;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDuns;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromExternalSystem;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromNameCountry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class TestEntityMatchUtils {

    /*
     * default testing variables
     */
    private static final String TEST_SEED_ID = TestEntityMatchUtils.class.getSimpleName();
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String EXTERNAL_SYSTEM_SFDC = "SFDC";
    private static final String EXTERNAL_SYSTEM_MKTO = "MKTO";
    private static final String EXTERNAL_SYSTEM_ELOQUA = "ELOQUA";


    /**
     * Determine whether two input {@link EntityRawSeed} are the same. The priority of {@link EntityLookupEntry} is
     * not considered in equality, the two seeds only need to have the same set of entries to be considered equals.
     *
     * @param seed1 one input seed
     * @param seed2 the other input seed
     * @return true if two seeds are considered equals
     */
    public static boolean equalsDisregardPriority(EntityRawSeed seed1, EntityRawSeed seed2) {
        if (seed1 == null && seed2 == null) {
            return true;
        } else if (seed1 == null || seed2 == null) {
            return false;
        }

        Set<EntityLookupEntry> set1 = new HashSet<>(seed1.getLookupEntries());
        Set<EntityLookupEntry> set2 = new HashSet<>(seed2.getLookupEntries());
        return Objects.equals(seed1.getId(), seed2.getId()) && Objects.equals(seed1.getEntity(), seed2.getEntity())
                && Objects.equals(seed1.getAttributes(), seed2.getAttributes())
                && Objects.equals(set1, set2);
    }

    /*
     * helpers to create seed
     */

    public static EntityRawSeed newSeed(@NotNull String seedId, EntityLookupEntry... lookupEntries) {
        return new EntityRawSeed(seedId, TEST_ENTITY, Arrays.asList(lookupEntries), null);
    }

    public static EntityRawSeed newSeed(@NotNull String seedId, boolean isNewlyAllocated,
            EntityLookupEntry... lookupEntries) {
        return new EntityRawSeed(seedId, TEST_ENTITY, isNewlyAllocated, -1, Arrays.asList(lookupEntries), null);
    }

    /**
     * Create an {@link EntityRawSeed} with given additional attributes and empty {@link EntityLookupEntry} list.
     * Entity will be {@link TestEntityMatchUtils#TEST_ENTITY}
     *
     * @param seedId seed ID
     * @param attributeNameValues attribute key/values, format is [ key1, val1, key2, val2 ... ]
     * @return generated seed
     */
    public static EntityRawSeed newSeed(@NotNull String seedId, String... attributeNameValues) {
        Preconditions.checkNotNull(seedId);
        Preconditions.checkArgument(attributeNameValues.length % 2 == 0);
        Map<String, String> attributes = IntStream.range(0, attributeNameValues.length / 2)
                .mapToObj(idx -> Pair.of(attributeNameValues[2 * idx], attributeNameValues[2 * idx + 1]))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
        return new EntityRawSeed(seedId, TEST_ENTITY, Collections.emptyList(), attributes);
    }

    /**
     * Merge lookup entries and extra attributes of two {@link EntityRawSeed} and return a newly created seed.
     * First seed's ID and entity will be used to create the new seed.
     *
     * @param seed1 first seed
     * @param seed2 second seed
     * @return newly created seed object that contains the merged lookup entries and extra attributes
     */
    public static EntityRawSeed merge(@NotNull EntityRawSeed seed1, @NotNull EntityRawSeed seed2) {
        List<EntityLookupEntry> entries = Stream
                .concat(seed1.getLookupEntries().stream(), seed2.getLookupEntries().stream())
                .collect(Collectors.toList());
        Map<String, String> attributes = Stream.concat(
                seed1.getAttributes().entrySet().stream(), seed2.getAttributes().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
        return new EntityRawSeed(seed1.getId(), seed1.getEntity(), entries, attributes);
    }

    public static EntityRawSeed changeId(@NotNull EntityRawSeed seed, @NotNull String seedId) {
        return new EntityRawSeed(seedId, seed.getEntity(), seed.getLookupEntries(), seed.getAttributes());
    }

    /*
     * Namespace for pre-generated seed test data
     */
    public static class Seed {
        public static final EntityRawSeed EMPTY =
                new EntityRawSeed(TEST_SEED_ID, TEST_ENTITY, Collections.emptyList(), null);
    }

    /*
     * Namespace for pre-generated lookup entry test data
     */
    public static class LookupEntry {
        public static final EntityLookupEntry DC_GOOGLE_1 =
                fromDomainCountry(TEST_ENTITY, "google.com", "USA");
        public static final EntityLookupEntry DC_GOOGLE_2 =
                fromDomainCountry(TEST_ENTITY, "google.com", "CN");
        public static final EntityLookupEntry DC_GOOGLE_3 =
                fromDomainCountry(TEST_ENTITY, "google.com", "TW");
        public static final EntityLookupEntry DC_GOOGLE_4 =
                fromDomainCountry(TEST_ENTITY, "google.com", "JP");
        public static final EntityLookupEntry DC_FACEBOOK_1 =
                fromDomainCountry(TEST_ENTITY, "facebook.com", "USA");
        public static final EntityLookupEntry DC_FACEBOOK_2 =
                fromDomainCountry(TEST_ENTITY, "facebook.com", "CN");
        public static final EntityLookupEntry DC_FACEBOOK_3 =
                fromDomainCountry(TEST_ENTITY, "facebook.com", "TW");
        public static final EntityLookupEntry DC_FACEBOOK_4 =
                fromDomainCountry(TEST_ENTITY, "facebook.com", "JP");
        public static final EntityLookupEntry DC_NETFLIX_1 =
                fromDomainCountry(TEST_ENTITY, "netflix.com", "USA");
        public static final EntityLookupEntry DC_NETFLIX_2 =
                fromDomainCountry(TEST_ENTITY, "netflix.com", "CN");

        public static final EntityLookupEntry NC_GOOGLE_1 = fromNameCountry(TEST_ENTITY, "Google", "USA");
        public static final EntityLookupEntry NC_GOOGLE_2 = fromNameCountry(TEST_ENTITY, "Google", "CN");
        public static final EntityLookupEntry NC_GOOGLE_3 = fromNameCountry(TEST_ENTITY, "Google", "TW");
        public static final EntityLookupEntry NC_GOOGLE_4 = fromNameCountry(TEST_ENTITY, "Google", "JP");
        public static final EntityLookupEntry NC_FACEBOOK_1 =
                fromNameCountry(TEST_ENTITY, "Facebook", "USA");
        public static final EntityLookupEntry NC_FACEBOOK_2 =
                fromNameCountry(TEST_ENTITY, "Facebook", "CN");
        public static final EntityLookupEntry NC_FACEBOOK_3 =
                fromNameCountry(TEST_ENTITY, "Facebook", "TW");
        public static final EntityLookupEntry NC_NETFLIX_1 =
                fromDomainCountry(TEST_ENTITY, "Netflix", "USA");
        public static final EntityLookupEntry NC_NETFLIX_2 =
                fromDomainCountry(TEST_ENTITY, "Netflix", "CN");

        public static final EntityLookupEntry DUNS_1 = fromDuns(TEST_ENTITY, "111111111");
        public static final EntityLookupEntry DUNS_2 = fromDuns(TEST_ENTITY, "222222222");
        public static final EntityLookupEntry DUNS_3 = fromDuns(TEST_ENTITY, "333333333");
        public static final EntityLookupEntry DUNS_4 = fromDuns(TEST_ENTITY, "444444444");
        public static final EntityLookupEntry DUNS_5 = fromDuns(TEST_ENTITY, "555555555");

        // external systems
        public static final EntityLookupEntry SFDC_1 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_SFDC, "s1");
        public static final EntityLookupEntry SFDC_2 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_SFDC, "s2");
        public static final EntityLookupEntry SFDC_3 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_SFDC, "s3");
        public static final EntityLookupEntry SFDC_4 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_SFDC, "s4");
        public static final EntityLookupEntry SFDC_5 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_SFDC, "s5");

        public static final EntityLookupEntry MKTO_1 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_MKTO, "m1");
        public static final EntityLookupEntry MKTO_2 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_MKTO, "m2");
        public static final EntityLookupEntry MKTO_3 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_MKTO, "m3");

        public static final EntityLookupEntry ELOQUA_1 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_ELOQUA, "e1");
        public static final EntityLookupEntry ELOQUA_2 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_ELOQUA, "e2");
        public static final EntityLookupEntry ELOQUA_3 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_ELOQUA, "e3");
        public static final EntityLookupEntry ELOQUA_4 =
                fromExternalSystem(TEST_ENTITY, EXTERNAL_SYSTEM_ELOQUA, "e4");
    }
}
