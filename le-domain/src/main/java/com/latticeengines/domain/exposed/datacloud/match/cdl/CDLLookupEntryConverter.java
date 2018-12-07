package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Class to convert to and from other classes to {@link CDLLookupEntry}
 */
public class CDLLookupEntryConverter {

    /**
     * Create {@link CDLLookupEntry} from {@link MatchKeyTuple}
     * @param entity input business entity
     * @param tuple input match key tuple
     * @return created entry, {@literal null} if not valid fields
     */
    public static CDLLookupEntry fromMatchKeyTuple(@NotNull String entity, @NotNull MatchKeyTuple tuple) {
        Preconditions.checkNotNull(tuple);
        // FIXME remove placeholder entity
        // TODO create from external system ID
        if (StringUtils.isNotBlank(tuple.getDuns())) {
            return fromDuns(entity, tuple.getDuns());
        } else if (StringUtils.isNotBlank(tuple.getDomain()) && StringUtils.isNotBlank(tuple.getCountry())) {
            return fromDomainCountry(entity, tuple.getDomain(), tuple.getCountry());
        } else if (StringUtils.isNotBlank(tuple.getName()) && StringUtils.isNotBlank(tuple.getCountry())) {
            return fromNameCountry(entity, tuple.getName(), tuple.getCountry());
        }
        return null;
    }

    /**
     * Create {@link MatchKeyTuple} from {@link CDLLookupEntry}
     * @param entry input lookup entry
     * @return created tuple, will not be {@literal null}
     * @throws UnsupportedOperationException if the entry type is not supported
     */
    public static MatchKeyTuple toMatchKeyTuple(@NotNull CDLLookupEntry entry) {
        Preconditions.checkNotNull(entry);
        Preconditions.checkNotNull(entry.getType());
        // TODO put entity into match key tuple or return a different object as response
        switch (entry.getType()) {
            case EXTERNAL_SYSTEM:
                // TODO create from external system
                break;
            case DOMAIN_COUNTRY:
                Pair<String, String> domainCountry = toDomainCountry(entry);
                return new MatchKeyTuple.Builder()
                        .withDomain(domainCountry.getLeft())
                        .withCountry(domainCountry.getValue())
                        .build();
            case NAME_COUNTRY:
                Pair<String, String> nameCountry = toNameCountry(entry);
                return new MatchKeyTuple.Builder()
                        .withName(nameCountry.getKey())
                        .withCountry(nameCountry.getValue())
                        .build();
            case DUNS:
                return new MatchKeyTuple.Builder().withDuns(toDuns(entry)).build();
            default:
        }
        throw new UnsupportedOperationException("Entry type " + entry.getType() + " is not supported");
    }

    public static CDLLookupEntry fromDomainCountry(
            @NotNull String entity, @NotNull String domain, String country) {
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

    public static CDLLookupEntry fromNameCountry(@NotNull String entity, @NotNull String name, String country) {
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
    public static CDLLookupEntry fromDuns(@NotNull String entity, @NotNull String duns) {
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
            @NotNull String entity, @NotNull String systemName, @NotNull String systemId) {
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
