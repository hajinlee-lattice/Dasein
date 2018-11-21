package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Class to convert to and from other classes to {@link CDLLookupEntry}
 */
public class CDLLookupEntryConverter {

    public static CDLLookupEntry fromMatchKeyTuple(@NotNull MatchKeyTuple tuple) {
        // TODO
        return null;
    }

    public static MatchKeyTuple toMatchKeyTuple(@NotNull CDLLookupEntry entry) {
        // TODO
        return null;
    }

    public static CDLLookupEntry fromDomainCountry(
            @NotNull CDLMatchEntity entity, @NotNull String domain, String country) {
        return new CDLLookupEntry(
                CDLLookupEntry.Type.DOMAIN_COUNTRY, entity, new String[0], new String[] { domain, country });
    }

    /**
     * Parse domain/country from input entry.
     *
     * @param entry target entry
     * @return [ Domain, Country ], will not be {@literal null}
     */
    public static Pair<String, String> toDomainCountry(@NotNull CDLLookupEntry entry) {
        check(entry, CDLLookupEntry.Type.DOMAIN_COUNTRY);
        String[] values = entry.getValues();
        return Pair.of(values[0], values[1]);
    }

    public static CDLLookupEntry fromNameCountry(@NotNull CDLMatchEntity entity, @NotNull String name, String country) {
        return new CDLLookupEntry(
                CDLLookupEntry.Type.NAME_COUNTRY, entity, new String[0], new String[] { name, country });
    }

    /**
     * Parse name/country from input entry.
     *
     * @param entry target entry
     * @return [ Name, Country ], will not be {@literal null}
     */
    public static Pair<String, String> toNameCountry(@NotNull CDLLookupEntry entry) {
        check(entry, CDLLookupEntry.Type.NAME_COUNTRY);
        String[] values = entry.getValues();
        return Pair.of(values[0], values[1]);
    }
    public static CDLLookupEntry fromDuns(@NotNull CDLMatchEntity entity, @NotNull String duns) {
        return new CDLLookupEntry(
                CDLLookupEntry.Type.DUNS, entity, new String[0], new String[] { duns });
    }

    public static String toDuns(@NotNull CDLLookupEntry entry) {
        check(entry, CDLLookupEntry.Type.DUNS);
        return entry.getValues()[0];
    }

    /*
     * systemId is the ID in that external system. e.g., SFDC ID
     */
    public static CDLLookupEntry fromExternalSystem(
            @NotNull CDLMatchEntity entity, @NotNull String systemName, @NotNull String systemId) {
        return new CDLLookupEntry(
                CDLLookupEntry.Type.EXTERNAL_SYSTEM, entity, new String[] { systemName }, new String[] { systemId });
    }

    /**
     * Parse system name/ID from input entry.
     *
     * @param entry target entry
     * @return [ System name, System ID ], will not be {@literal null}
     */
    public static Pair<String, String> toExternalSystem(@NotNull CDLLookupEntry entry) {
        check(entry, CDLLookupEntry.Type.EXTERNAL_SYSTEM);
        return Pair.of(entry.getKeys()[0], entry.getValues()[0]);
    }

    private static void check(CDLLookupEntry entry, @NotNull CDLLookupEntry.Type type) {
        Preconditions.checkNotNull(entry);
        Preconditions.checkArgument(entry.getType() == type);
    }

}
