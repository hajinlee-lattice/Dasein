package com.latticeengines.eai.service.impl.file.converter;

import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.eai.service.impl.AvroTypeConverter;

@Component("fileToAvroTypeConverter")
public class FileToAvroTypeConverter extends AvroTypeConverter {

    @Override
    public Type convertTypeToAvro(String type) {
        type = "java.lang." + type;
        Class<?> javaType = null;
        try {
            javaType = Class.forName(type);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return AvroUtils.getAvroType(javaType);
    }

}
