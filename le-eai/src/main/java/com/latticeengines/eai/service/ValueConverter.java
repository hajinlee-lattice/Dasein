package com.latticeengines.eai.service;

public interface ValueConverter {

    <T> T convertTo(Class<T> targetType, Object value);
}
