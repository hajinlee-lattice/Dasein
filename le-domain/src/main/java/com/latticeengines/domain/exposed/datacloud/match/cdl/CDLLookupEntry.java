package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Lookup entry for a specific entity
 */
public class CDLLookupEntry {
    private final Type type;
    private final BusinessEntity entity;
    // store serialized form since internal operation only need these
    private final String serializedKeys;
    private final String serializedValues;

    public CDLLookupEntry(
            @NotNull Type type, @NotNull BusinessEntity entity, @NotNull String[] keys, @NotNull String[] values) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(entity);
        type.checkKeys(keys);
        type.checkValues(values);
        this.type = type;
        this.entity = entity;
        this.serializedKeys = type.serializeKeys(keys);
        this.serializedValues = type.serializeValues(values);
    }

    public Type getType() {
        return type;
    }

    public BusinessEntity getEntity() {
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
     * Lookup type. whenever new item is added here, remember to update {@link CDLLookupEntryConverter} as well.
     *
     * 1. Keys are used to determine the attribute name. E.g., in external system, system name will be used in the
     *    attribute name so different system will have different attribute name. while in DUNS, we only have one
     *    attribute of this type.
     */
    public enum Type {
        EXTERNAL_SYSTEM(1, 1, Mapping.ONE_TO_ONE),
        DOMAIN_COUNTRY(0, 2, Mapping.MANY_TO_MANY),
        DUNS(0, 1, Mapping.MANY_TO_ONE),
        NAME_COUNTRY(0, 2, Mapping.MANY_TO_MANY);

        private static final String DELIMITER = "_";

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
            String[] args = serializedArgs.split(DELIMITER);
            check(nArgs, args);
            return args;
        }

        private void check(int nArgs, String... args) {
            Preconditions.checkArgument(args != null && args.length == nArgs);
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
        CDLLookupEntry that = (CDLLookupEntry) o;
        return type == that.type &&
                Objects.equal(serializedKeys, that.serializedKeys) &&
                Objects.equal(serializedValues, that.serializedValues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type, serializedKeys, serializedValues);
    }
}
