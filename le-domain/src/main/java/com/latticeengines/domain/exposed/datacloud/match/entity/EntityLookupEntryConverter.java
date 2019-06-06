package com.latticeengines.domain.exposed.datacloud.match.entity;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_NAME_PHONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.C_ACCT_EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.C_ACCT_NAME_PHONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.NAME_PHONE;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

/**
 * Class to convert to and from other classes to {@link EntityLookupEntry}
 */
public class EntityLookupEntryConverter {

    /**
     * Create a list of {@link EntityLookupEntry} from {@link MatchKeyTuple}
     * @param entity input entity
     * @param tuple input match key tuple
     * @return created list of entries, empty list if no valid fields in tuple
     */
    public static List<EntityLookupEntry> fromMatchKeyTuple(@NotNull String entity, @NotNull MatchKeyTuple tuple) {
        Preconditions.checkNotNull(tuple);
        for (EntityLookupEntry.Type type : EntityLookupEntry.Type.values()) {
            if (type.canTransformToEntries(tuple)) {
                // use the first valid type, so the order in enum declaration matters
                return type.toEntries(entity, tuple);
            }
        }
        return Collections.emptyList();
    }

    /**
     * Create {@link MatchKeyTuple} from {@link EntityLookupEntry}
     * @param entry input lookup entry
     * @return created tuple, will not be {@literal null}
     * @throws UnsupportedOperationException if the entry type is not supported
     */
    public static MatchKeyTuple toMatchKeyTuple(@NotNull EntityLookupEntry entry) {
        Preconditions.checkNotNull(entry);
        Preconditions.checkNotNull(entry.getType());
        return entry.getType().toTuple(entry);
    }

    /**
     * Create an {@link EntityLookupEntry} from a pair of systemId name/value
     *
     * @param entity input entity
     * @param systemIdName system name
     * @param systemIdValue system id value
     * @return created lookup entry, will not be {@literal null}
     */
    public static EntityLookupEntry fromSystemId(
            @NotNull String entity, @NotNull String systemIdName, @NotNull String systemIdValue) {
       return new EntityLookupEntry(
               EntityLookupEntry.Type.EXTERNAL_SYSTEM, entity,
               new String[] { systemIdName }, new String[] { systemIdValue });
    }

    public static EntityLookupEntry fromDomainCountry(
            @NotNull String entity, @NotNull String domain, String country) {
        return new EntityLookupEntry(
                EntityLookupEntry.Type.DOMAIN_COUNTRY, entity, new String[0], new String[] { domain, country });
    }

    /**
     * Parse domain/country from input entry.
     *
     * @param entry target entry
     * @return [ Domain, Country ], will not be {@literal null}
     */
    public static Pair<String, String> toDomainCountry(@NotNull EntityLookupEntry entry) {
        check(entry, EntityLookupEntry.Type.DOMAIN_COUNTRY);
        String[] values = entry.getValues();
        return Pair.of(values[0], values[1]);
    }

    public static EntityLookupEntry fromNameCountry(@NotNull String entity, @NotNull String name, String country) {
        return new EntityLookupEntry(
                EntityLookupEntry.Type.NAME_COUNTRY, entity, new String[0], new String[] { name, country });
    }

    /**
     * Parse name/country from input entry.
     *
     * @param entry target entry
     * @return [ Name, Country ], will not be {@literal null}
     */
    public static Pair<String, String> toNameCountry(@NotNull EntityLookupEntry entry) {
        check(entry, EntityLookupEntry.Type.NAME_COUNTRY);
        String[] values = entry.getValues();
        return Pair.of(values[0], values[1]);
    }

    public static EntityLookupEntry fromDuns(@NotNull String entity, @NotNull String duns) {
        return new EntityLookupEntry(
                EntityLookupEntry.Type.DUNS, entity, new String[0], new String[] { duns });
    }

    public static String toDuns(@NotNull EntityLookupEntry entry) {
        check(entry, EntityLookupEntry.Type.DUNS);
        return entry.getValues()[0];
    }

    public static EntityLookupEntry fromAccountIdEmail(@NotNull String entity, @NotNull String accountId,
            @NotNull String email) {
        return new EntityLookupEntry(ACCT_EMAIL, entity, new String[0], new String[] { accountId, email });
    }

    public static EntityLookupEntry fromAccountIdNamePhoneNumber(@NotNull String entity, @NotNull String accountId,
            @NotNull String name, @NotNull String phoneNumber) {
        return new EntityLookupEntry(ACCT_NAME_PHONE, entity, new String[0],
                new String[] { accountId, name, phoneNumber });
    }

    public static EntityLookupEntry fromCustomerAccountIdEmail(@NotNull String entity,
            @NotNull String customerAccountId, @NotNull String email) {
        return new EntityLookupEntry(C_ACCT_EMAIL, entity, new String[0], new String[] { customerAccountId, email });
    }

    public static EntityLookupEntry fromCustomerAccountIdNamePhoneNumber(@NotNull String entity,
            @NotNull String customerAccountId, @NotNull String name, @NotNull String phoneNumber) {
        return new EntityLookupEntry(C_ACCT_NAME_PHONE, entity, new String[0],
                new String[] { customerAccountId, name, phoneNumber });
    }

    public static EntityLookupEntry fromEmail(@NotNull String entity, @NotNull String email) {
        return new EntityLookupEntry(EMAIL, entity, new String[0], new String[] { email });
    }

    public static EntityLookupEntry fromNamePhoneNumber(@NotNull String entity, @NotNull String name,
            @NotNull String phoneNumber) {
        return new EntityLookupEntry(NAME_PHONE, entity, new String[0], new String[] { name, phoneNumber });
    }

    /*
     * systemId is the ID in that external system. e.g., SFDC ID
     */
    public static EntityLookupEntry fromExternalSystem(
            @NotNull String entity, @NotNull String systemName, @NotNull String systemId) {
        return new EntityLookupEntry(
                EntityLookupEntry.Type.EXTERNAL_SYSTEM, entity, new String[] { systemName }, new String[] { systemId });
    }

    /**
     * Parse system name/ID from input entry.
     *
     * @param entry target entry
     * @return [ System name, System ID ], will not be {@literal null}
     */
    public static Pair<String, String> toExternalSystem(@NotNull EntityLookupEntry entry) {
        check(entry, EntityLookupEntry.Type.EXTERNAL_SYSTEM);
        return Pair.of(entry.getKeys()[0], entry.getValues()[0]);
    }

    private static void check(EntityLookupEntry entry, @NotNull EntityLookupEntry.Type type) {
        Preconditions.checkNotNull(entry);
        Preconditions.checkArgument(entry.getType() == type);
    }
}
