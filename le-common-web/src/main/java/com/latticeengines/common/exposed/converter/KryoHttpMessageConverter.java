package com.latticeengines.common.exposed.converter;

import java.io.IOException;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;

import com.latticeengines.common.exposed.util.KryoUtils;

public class KryoHttpMessageConverter extends AbstractHttpMessageConverter<Object> {

    public static final MediaType KRYO = new MediaType("application", "x-kryo");
    public static final String KRYO_VALUE = "application/x-kryo";

    public KryoHttpMessageConverter() {
        super(KRYO);
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        return Object.class.isAssignableFrom(clazz);
    }

    @Override
    protected Object readInternal(Class<? extends Object> clazz, HttpInputMessage inputMessage) throws IOException {
        return KryoUtils.read(inputMessage.getBody());
    }

    @Override
    protected void writeInternal(Object object, HttpOutputMessage outputMessage) throws IOException {
        KryoUtils.write(outputMessage.getBody(), object);
    }

    @Override
    protected MediaType getDefaultContentType(Object object) {
        return KRYO;
    }
}
