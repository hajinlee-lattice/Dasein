package com.latticeengines.domain.exposed.datacloud.match.entity;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.MANY_TO_MANY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.ONE_TO_ONE;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

/**
 * Lookup entry for a specific entity
 */
public class EntityLookupEntry {
    private final Type type;
    private final String entity;
    // store serialized form since internal operation only need these
    private final String serializedKeys;
    private final String serializedValues;

    public EntityLookupEntry(
            @NotNull Type type, @NotNull String entity, @NotNull String[] keys, @NotNull String[] values) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(entity);
        type.checkKeys(keys);
        type.checkValues(values);
        this.type = type;
        this.entity = entity;
        this.serializedKeys = type.serializeKeys(keys);
        this.serializedValues = type.serializeValues(values);
    }

    /*
     * serializedKeys and serializedValues must have the correct format
     *
     * NOTE not checking here for performance
     */
    public EntityLookupEntry(
            @NotNull Type type, @NotNull String entity,
            @NotNull String serializedKeys, @NotNull String serializedValues) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(serializedKeys);
        Preconditions.checkNotNull(serializedValues);
        this.type = type;
        this.entity = entity;
        this.serializedKeys = serializedKeys;
        this.serializedValues = serializedValues;
    }

    public Type getType() {
        return type;
    }

    public String getEntity() {
        return entity;
    }

    public String getSerializedKeys() {
        return serializedKeys;
    }

    public String getSerializedValues() {
        return serializedValues;
    }

    public String[] getKeys() {
        // TODO cache this if it is accessed frequently in the future
        return type.deserializeKeys(serializedKeys);
    }

    public String[] getValues() {
        // TODO cache this if it is accessed frequently in the future
        return type.deserializeValues(serializedValues);
    }

    /**
     * Lookup type. whenever new item is added here, remember to update {@link EntityLookupEntryConverter} as well.
     *
     * 1. Keys are used to determine the attribute name. E.g., in external system, system name will be used in the
     *    attribute name so different system will have different attribute name. while in DUNS, we only have one
     *    attribute of this type.
     */
    public enum Type {
        // types for account match only
        DOMAIN_COUNTRY(0, 2, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(tuple.getDomain())
                        && isNotBlank(tuple.getCountry());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(new EntityLookupEntry(this, entity, new String[0],
                        new String[] { tuple.getDomain(), tuple.getCountry() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                String[] values = entry.getValues();
                return new MatchKeyTuple.Builder().withDomain(values[0]).withCountry(values[1]).build();
            }
        }, //
        DUNS(0, 1, Mapping.MANY_TO_ONE) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(tuple.getDuns());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(
                        new EntityLookupEntry(this, entity, new String[0], new String[] { tuple.getDuns() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                return new MatchKeyTuple.Builder().withDuns(entry.getSerializedValues()).build();
            }
        }, //
        NAME_COUNTRY(0, 2, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(tuple.getName())
                        && isNotBlank(tuple.getCountry());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(new EntityLookupEntry(this, entity, new String[0],
                        new String[] { tuple.getName(), tuple.getCountry() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                String[] values = entry.getValues();
                return new MatchKeyTuple.Builder().withName(values[0]).withCountry(values[1]).build();
            }
        }, //
        // types for contact match only
        ACCT_EMAIL(0, 2, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(getSystemId(tuple, AccountId.name()))
                        && isNotBlank(tuple.getEmail());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(new EntityLookupEntry(this, entity, new String[0],
                        new String[] { getSystemId(tuple, AccountId.name()), tuple.getEmail() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                String[] values = entry.getValues();
                return new MatchKeyTuple.Builder() //
                        .withSystemIds(singletonList(Pair.of(AccountId.name(), values[0]))) //
                        .withEmail(values[1]) //
                        .build();
            }
        }, // Account entity ID + Email
        ACCT_NAME_PHONE(0, 3, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(getSystemId(tuple, AccountId.name()))
                        && isNotBlank(tuple.getName()) && isNotBlank(tuple.getPhoneNumber());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(new EntityLookupEntry(this, entity, new String[0], new String[] {
                        getSystemId(tuple, AccountId.name()), tuple.getName(), tuple.getPhoneNumber() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                String[] values = entry.getValues();
                return new MatchKeyTuple.Builder() //
                        .withSystemIds(singletonList(Pair.of(AccountId.name(), values[0]))) //
                        .withName(values[1]) //
                        .withPhoneNumber(values[2]).build();
            }
        }, // Account entity ID + Contact Full Name + Phone Number
        C_ACCT_EMAIL(0, 2, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(getSystemId(tuple, CustomerAccountId.name()))
                        && isNotBlank(tuple.getEmail());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(new EntityLookupEntry(this, entity, new String[0],
                        new String[] { getSystemId(tuple, CustomerAccountId.name()), tuple.getEmail() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                String[] values = entry.getValues();
                return new MatchKeyTuple.Builder() //
                        .withSystemIds(singletonList(Pair.of(CustomerAccountId.name(), values[0]))) //
                        .withEmail(values[1]) //
                        .build();
            }
        }, // Customer Account ID + Email
        C_ACCT_NAME_PHONE(0, 3, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(getSystemId(tuple, CustomerAccountId.name()))
                        && isNotBlank(tuple.getName()) && isNotBlank(tuple.getPhoneNumber());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(new EntityLookupEntry(this, entity, new String[0], new String[] {
                        getSystemId(tuple, CustomerAccountId.name()), tuple.getName(), tuple.getPhoneNumber() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                String[] values = entry.getValues();
                return new MatchKeyTuple.Builder() //
                        .withSystemIds(singletonList(Pair.of(CustomerAccountId.name(), values[0]))) //
                        .withName(values[1]) //
                        .withPhoneNumber(values[2]).build();
            }
        }, // Customer Account ID + Contact Full Name + Phone Number
        EMAIL(0, 1, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(tuple.getEmail());
            }

            @Override
            public List<EntityLookupEntry> toEntries(String entity, MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(
                        new EntityLookupEntry(this, entity, new String[0], new String[] { tuple.getEmail() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                return new MatchKeyTuple.Builder().withEmail(entry.getSerializedValues()).build();
            }
        }, // Email only
        NAME_PHONE(0, 2, MANY_TO_MANY) {
            @Override
            public boolean canTransformToEntries(@NotNull MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && isNotBlank(tuple.getName())
                        && isNotBlank(tuple.getPhoneNumber());
            }

            @Override
            public List<EntityLookupEntry> toEntries(@NotNull String entity, @NotNull MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return singletonList(new EntityLookupEntry(this, entity, new String[0],
                        new String[] { tuple.getName(), tuple.getPhoneNumber() }));
            }

            @Override
            public MatchKeyTuple toTuple(EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                String[] values = entry.getValues();
                return new MatchKeyTuple.Builder().withName(values[0]).withPhoneNumber(values[1]).build();
            }
        }, // Contact Full Name + Phone Number
           // general types
        EXTERNAL_SYSTEM(1, 1, ONE_TO_ONE) {
            @Override
            public boolean canTransformToEntries(MatchKeyTuple tuple) {
                return super.canTransformToEntries(tuple) && CollectionUtils.isNotEmpty(tuple.getSystemIds());
            }

            @Override
            public List<EntityLookupEntry> toEntries(@NotNull String entity, @NotNull MatchKeyTuple tuple) {
                checkToEntriesArgument(entity, tuple);
                return tuple.getSystemIds() //
                        .stream() //
                        .filter(java.util.Objects::nonNull) //
                        // using serialized value directly since we only have 1 key & value
                        // use empty string to represent null value
                        .map(pair -> new EntityLookupEntry(this, entity, pair.getKey(),
                                pair.getValue() == null ? StringUtils.EMPTY : pair.getValue())) //
                        .collect(Collectors.toList());
            }

            @Override
            public MatchKeyTuple toTuple(@NotNull EntityLookupEntry entry) {
                checkToTupleArgument(entry);
                return new MatchKeyTuple.Builder() //
                        .withSystemIds(singletonList(Pair.of(entry.getSerializedKeys(), entry.getSerializedValues()))) //
                        .build();
            }
        }; //

        private static final String DELIMITER = "||";
        private static final String DELIMITER_REGEX = "\\|\\|";

        public final Mapping mapping;
        private final int nKeys;
        private final int nValues;

        Type(int nKeys, int nValues, @NotNull Mapping mapping) {
            Preconditions.checkArgument(nKeys >= 0 && nValues > 0);
            Preconditions.checkNotNull(mapping);
            this.nKeys = nKeys;
            this.nValues = nValues;
            this.mapping = mapping;
        }

        public String serializeKeys(@NotNull String... keys) {
            return serialize(nKeys, keys);
        }

        public String[] deserializeKeys(@NotNull String serializedKeys) {
            return deserialize(nKeys, serializedKeys);
        }

        public String serializeValues(@NotNull String... values) {
            return serialize(nValues, values);
        }

        public String[] deserializeValues(@NotNull String serializedValues) {
            return deserialize(nValues, serializedValues);
        }

        public void checkKeys(@NotNull String[] keys) {
            check(nKeys, keys);
        }

        public void checkValues(@NotNull String[] values) {
            check(nValues, values);
        }

        /**
         * Check whether the input {@link MatchKeyTuple} can be transformed to lookup
         * entry of this specific type.
         *
         * @param tuple
         *            input tuple
         * @return true if can transform
         */
        public boolean canTransformToEntries(MatchKeyTuple tuple) {
            return tuple != null;
        }

        /**
         * Transform input {@link MatchKeyTuple} to a list of {@link EntityLookupEntry}
         * of this specific type.
         *
         * @param entity
         *            target entity
         * @param tuple
         *            input tuple
         * @throws IllegalArgumentException
         *             if {@link this#canTransformToEntries(MatchKeyTuple)} returns
         *             false
         * @return list of {@link EntityLookupEntry}, will not be {@literal null}
         */
        public List<EntityLookupEntry> toEntries(@NotNull String entity, @NotNull MatchKeyTuple tuple) {
            String msg = String.format("Transforming to list of EntityLookupEntry for type %s is not supported.",
                    name());
            throw new UnsupportedOperationException(msg);
        }

        /**
         * Transform input {@link EntityLookupEntry} to {@link MatchKeyTuple}.
         *
         * @param entry
         *            input entry, should have the same type as the type instance this
         *            method is invoked on
         * @return generated tuple, will not be {@literal null}
         */
        public MatchKeyTuple toTuple(@NotNull EntityLookupEntry entry) {
            String msg = String.format("Transforming to MatchKeyTuple for type %s is not supported.", name());
            throw new UnsupportedOperationException(msg);
        }

        private String serialize(int nArgs, String... args) {
            check(nArgs, args);
            if (nArgs == 0) {
                return "";
            } else if (nArgs == 1) {
                return args[0];
            }
            return String.join(DELIMITER, args);
        }

        private String[] deserialize(int nArgs, String serializedArgs) {
            Preconditions.checkNotNull(serializedArgs);
            if (nArgs == 0) {
                return new String[0];
            } else if (nArgs == 1) {
                return new String[] { serializedArgs };
            }
            String[] args = serializedArgs.split(DELIMITER_REGEX);
            check(nArgs, args);
            return args;
        }

        private void check(int nArgs, String... args) {
            Preconditions.checkArgument(args != null && args.length == nArgs);
        }

        protected String getSystemId(@NotNull MatchKeyTuple tuple, @NotNull String systemName) {
            if (tuple == null || CollectionUtils.isEmpty(tuple.getSystemIds())) {
                return null;
            }

            return tuple.getSystemIds().stream() //
                    .filter(pair -> pair != null && systemName.equals(pair.getKey())) //
                    .map(Pair::getValue) //
                    .filter(StringUtils::isNotBlank) //
                    .findFirst() //
                    .orElse(null);
        }

        protected void checkToEntriesArgument(String entity, MatchKeyTuple tuple) {
            Preconditions.checkArgument(isNotBlank(entity), "Target entity should not be null or blank");
            Preconditions.checkArgument(canTransformToEntries(tuple),
                    String.format("Input MatchKeyTuple=%s cannot be transformed to type %s", tuple, this.name()));
        }

        protected void checkToTupleArgument(EntityLookupEntry entry) {
            Preconditions.checkNotNull(entry);
            Preconditions.checkArgument(entry.type == this,
                    String.format("Input entry should have type %s, got %s instead", this.name(), entry.type));
        }
    }

    /**
     * determine the relationship between this type of lookup entry and entity.
     */
    public enum Mapping {
        ONE_TO_ONE, MANY_TO_ONE, MANY_TO_MANY
    }

    /*
     * Generated equals and hashCode
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EntityLookupEntry that = (EntityLookupEntry) o;
        return type == that.type && Objects.equal(this.entity, that.entity) &&
                Objects.equal(serializedKeys, that.serializedKeys) &&
                Objects.equal(serializedValues, that.serializedValues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type, entity, serializedKeys, serializedValues);
    }

    @Override
    public String toString() {
        return "EntityLookupEntry{" +
                "type=" + type +
                ", entity='" + entity + '\'' +
                ", serializedKeys='" + serializedKeys + '\'' +
                ", serializedValues='" + serializedValues + '\'' +
                '}';
    }
}
