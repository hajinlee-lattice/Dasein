package com.latticeengines.domain.exposed.datacloud.match.entity;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_NAME_PHONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.C_ACCT_EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.C_ACCT_NAME_PHONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.NAME_PHONE;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

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
        // TODO refactor this logic into EntityLookupEntry
        // in the future
        Preconditions.checkNotNull(tuple);
        if (isNotBlank(tuple.getDuns())) {
            return singletonList(fromDuns(entity, tuple.getDuns()));
        } else if (isNotBlank(tuple.getDomain()) && isNotBlank(tuple.getCountry())) {
            return singletonList(fromDomainCountry(entity, tuple.getDomain(), tuple.getCountry()));
        } else if (isNotBlank(tuple.getName()) && isNotBlank(tuple.getCountry())) {
            return singletonList(fromNameCountry(entity, tuple.getName(), tuple.getCountry()));
        } else if (isNotBlank(getAccountId(tuple))) {
            if (isNotBlank(tuple.getEmail())) {
                return singletonList(fromAccountIdEmail(entity, getAccountId(tuple), tuple.getEmail()));
            } else if (isNotBlank(tuple.getName()) && isNotBlank(tuple.getPhoneNumber())) {
                return singletonList(fromAccountIdNamePhoneNumber(entity, getAccountId(tuple), tuple.getName(),
                        tuple.getPhoneNumber()));
            }
        } else if (isNotBlank(getCustomerAccountId(tuple))) {
            if (isNotBlank(tuple.getEmail())) {
                return singletonList(fromCustomerAccountIdEmail(entity, getAccountId(tuple), tuple.getEmail()));
            } else if (isNotBlank(tuple.getName()) && isNotBlank(tuple.getPhoneNumber())) {
                return singletonList(fromCustomerAccountIdNamePhoneNumber(entity, getAccountId(tuple), tuple.getName(),
                        tuple.getPhoneNumber()));
            }
        } else if (isNotBlank(tuple.getEmail())) {
            return singletonList(fromEmail(entity, tuple.getEmail()));
        } else if (isNotBlank(tuple.getName()) && isNotBlank(tuple.getPhoneNumber())) {
            return singletonList(fromNamePhoneNumber(entity, tuple.getName(), tuple.getPhoneNumber()));
        } else if (CollectionUtils.isNotEmpty(tuple.getSystemIds())) {
            return tuple.getSystemIds()
                    .stream()
                    .map(pair -> fromExternalSystem(entity, pair.getKey(), pair.getValue()))
                    .collect(Collectors.toList());
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
        String[] values = entry.getValues();
        MatchKeyTuple.Builder builder = new MatchKeyTuple.Builder();
        switch (entry.getType()) {
        case EXTERNAL_SYSTEM:
            return builder.withSystemIds(singletonList(toSystemId(entry))).build();
        case DOMAIN_COUNTRY:
            Pair<String, String> domainCountry = toDomainCountry(entry);
            return builder //
                    .withDomain(domainCountry.getLeft()) //
                    .withCountry(domainCountry.getValue()) //
                    .build();
        case NAME_COUNTRY:
            Pair<String, String> nameCountry = toNameCountry(entry);
            return builder //
                    .withName(nameCountry.getKey()) //
                    .withCountry(nameCountry.getValue()) //
                    .build();
        case DUNS:
            return builder.withDuns(toDuns(entry)).build();
        case ACCT_EMAIL:
            return builder //
                    .withSystemIds(singletonList(toAccountIdPair(values[0]))) //
                    .withEmail(values[1]) //
                    .build();
        case ACCT_NAME_PHONE:
            return builder //
                    .withSystemIds(singletonList(toAccountIdPair(values[0]))) //
                    .withName(values[1]) //
                    .withPhoneNumber(values[2]) //
                    .build();
        case C_ACCT_EMAIL:
            return builder //
                    .withSystemIds(singletonList(toCustomerAccountIdPair(values[0]))) //
                    .withEmail(values[1]) //
                    .build();
        case C_ACCT_NAME_PHONE:
            return builder //
                    .withSystemIds(singletonList(toCustomerAccountIdPair(values[0]))) //
                    .withName(values[1]) //
                    .withPhoneNumber(values[2]) //
                    .build();
        case EMAIL:
            return builder.withEmail(values[0]).build();
        case NAME_PHONE:
            return builder.withName(values[0]).withPhoneNumber(values[1]).build();
        default:
        }
        throw new UnsupportedOperationException("Entry type " + entry.getType() + " is not supported");
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

    /**
     * Parse a pair of systemId name/value from input entry
     *
     * @param entry input lookup entry
     * @return [ systemId name, systemId value ], will not be {@literal null}
     */
    public static Pair<String, String> toSystemId(@NotNull EntityLookupEntry entry) {
        check(entry, EntityLookupEntry.Type.EXTERNAL_SYSTEM);
        return Pair.of(entry.getSerializedKeys(), entry.getSerializedValues());
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

    /*
     * get the target system's ID from input tuple, return null if there is no such
     * system
     */
    private static String getExternalSystemId(MatchKeyTuple tuple, @NotNull String systemName) {
        if (tuple == null || tuple.getSystemIds() == null) {
            return null;
        }

        return tuple.getSystemIds() //
                .stream() //
                .filter(pair -> pair != null && systemName.equals(pair.getKey())) //
                .map(Pair::getValue) //
                .findAny() //
                .orElse(null);
    }

    /*
     * get account entity ID from tuple
     */
    private static String getAccountId(@NotNull MatchKeyTuple tuple) {
        return getExternalSystemId(tuple, InterfaceName.AccountId.name());
    }

    /*
     * generate (systemName, systemId) tuple for account entity ID
     */
    private static Pair<String, String> toAccountIdPair(@NotNull String accountId) {
        return Pair.of(InterfaceName.AccountId.name(), accountId);
    }

    /*
     * get customer account ID from tuple
     */
    private static String getCustomerAccountId(@NotNull MatchKeyTuple tuple) {
        return getExternalSystemId(tuple, InterfaceName.CustomerAccountId.name());
    }

    /*
     * generate (systemName, systemId) tuple for customer account ID
     */
    private static Pair<String, String> toCustomerAccountIdPair(@NotNull String accountId) {
        return Pair.of(InterfaceName.CustomerAccountId.name(), accountId);
    }
}
