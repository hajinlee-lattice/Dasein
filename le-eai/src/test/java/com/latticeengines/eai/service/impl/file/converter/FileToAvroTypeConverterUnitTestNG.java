package com.latticeengines.eai.service.impl.file.converter;

import static org.testng.Assert.assertEquals;

import org.apache.avro.Schema.Type;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FileToAvroTypeConverterUnitTestNG {

    private FileToAvroTypeConverter fileToAvroTypeConverter = new FileToAvroTypeConverter();

    @Test(groups = "unit", dataProvider = "fileTypeToAvroTypeProvider")
    public void convertTypeToAvro(String fileType, Type avroType) {
        assertEquals(fileToAvroTypeConverter.convertTypeToAvro(fileType), avroType);
    }

    @DataProvider(name = "fileTypeToAvroTypeProvider")
    public static Object[][] getDataFileProvider() {
        return new Object[][] { { "Int", Type.INT }, //
                { "Double", Type.DOUBLE }, //
                { "Boolean", Type.BOOLEAN }, //
                { "Float", Type.FLOAT }, //
                { "String", Type.STRING }, //
                { "Date", Type.LONG }, //
                { "Timestamp", Type.LONG }, //
                { "Long", Type.LONG } };
    }

}
