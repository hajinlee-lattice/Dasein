package com.latticeengines.domain.exposed.datafabric;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;

public abstract class BaseFabricEntity<T> implements FabricEntity<T> {

    private final Map<String, Object> tags = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <U> U getTag(String tagName, Class<U> tagClass) {
        Object tag = tags.get(tagName);
        if (tag == null) {
            return null;
        }
        if (tagClass.equals(Boolean.class)) {
            return (U) Boolean.valueOf(String.valueOf(tag));
        }
        return (U) tag;
    }

    @Override
    public void setTag(String tagName, Object value) {
        tags.put(tagName, value);
    }

    @Override
    public Map<String, Object> getTags() {
        return tags;
    }

    protected String replaceSpacialChars(@NotNull String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        return recordType.replace('.', '_');
    }

}
