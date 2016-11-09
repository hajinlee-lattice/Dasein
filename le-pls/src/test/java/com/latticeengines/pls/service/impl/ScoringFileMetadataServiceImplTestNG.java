package com.latticeengines.pls.service.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class ScoringFileMetadataServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file2.csv";
    private File csvFile;
    private CloseableResourcePool closeableResourcePool;
    private String displayName;

    @Autowired
    private ScoringFileMetadataService scoringFileMetadataService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        URL csvFileUrl = ClassLoader.getSystemResource(PATH);
        csvFile = new File(csvFileUrl.getFile());
        closeableResourcePool = new CloseableResourcePool();
        displayName = "file2.csv";
    }

    @Test(groups = "functional")
    public void testValidateHeaderFieldsWithEmptyHeaders() throws FileNotFoundException {
        InputStream stream = new FileInputStream(csvFile);
        boolean exceptionThrown = false;
        try {
            scoringFileMetadataService.validateHeaderFields(stream, closeableResourcePool,
                    displayName);
            assertTrue(exceptionThrown, "should have thrown exception");
        } catch (Exception e) {
            exceptionThrown = true;
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18096);
        }
        assertTrue(exceptionThrown);
    }

    @Test(groups = "functional")
    public void testSaveFieldMappingDocument() {
        List<Attribute> attrs = new ArrayList<>();
        Attribute a = new Attribute();
        a.setName("A");
        a.setDisplayName("AD");
        attrs.add(a);

        Attribute b = new Attribute();
        b.setName("B");
        b.setDisplayName("BD");
        attrs.add(b);

        ModelMetadataService modelMetadataService = Mockito.mock(ModelMetadataService.class);
        when(modelMetadataService.getRequiredColumns(anyString())).thenReturn(attrs);

        Set<String> attributeNames = new HashSet<>();
        attributeNames.add("C");
        attributeNames.add("C_1");
        when(modelMetadataService.getLatticeAttributeNames(anyString())).thenReturn(attributeNames);

        SourceFileService sourceFileService = Mockito.mock(SourceFileService.class);
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName("file2");
        when(sourceFileService.findByName(anyString())).thenReturn(sourceFile);
        Mockito.doNothing().when(sourceFileService).update(any(SourceFile.class));

        MetadataProxy metadataProxy = Mockito.mock(MetadataProxy.class);
        Mockito.doNothing().when(metadataProxy).createTable(anyString(), anyString(),
                any(Table.class));

        ReflectionTestUtils.setField(scoringFileMetadataService, "modelMetadataService",
                modelMetadataService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "sourceFileService",
                sourceFileService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "metadataProxy", metadataProxy);

        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setRequiredFields(Arrays.asList(new String[] { "A" }));

        FieldMapping mapping = new FieldMapping();
        mapping.setUserField("SomeId");
        mapping.setMappedField("A");
        mapping.setMappedToLatticeField(true);
        mapping.setFieldType(UserDefinedType.NUMBER);

        FieldMapping mapping2 = new FieldMapping();
        mapping2.setUserField("SomeStr");
        mapping2.setMappedField("B");
        mapping2.setMappedToLatticeField(true);
        mapping2.setFieldType(UserDefinedType.TEXT);

        FieldMapping mapping3 = new FieldMapping();
        mapping3.setUserField("C_1");
        mapping3.setMappedToLatticeField(false);
        mapping3.setFieldType(UserDefinedType.TEXT);

        FieldMapping mapping4 = new FieldMapping();
        mapping4.setUserField("C");
        mapping4.setMappedToLatticeField(false);
        mapping4.setFieldType(UserDefinedType.TEXT);

        fieldMappingDocument.setRequiredFields(Arrays.asList(new String[] { "A" }));
        fieldMappingDocument.setFieldMappings(
                Arrays.asList(new FieldMapping[] { mapping, mapping2, mapping3, mapping4 }));

        Tenant t = new Tenant();
        t.setId("t1");
        MultiTenantContext.setTenant(t);
        Table table = scoringFileMetadataService.saveFieldMappingDocument("csvfilename", "modelid",
                fieldMappingDocument);

        assertEquals(table.getAttributes().size(), 4);
        assertNotNull(table.getAttribute("C_2"));
        assertNotNull(table.getAttribute("C_1_1"));
    }

}
