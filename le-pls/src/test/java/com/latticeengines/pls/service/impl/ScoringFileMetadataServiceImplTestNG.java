package com.latticeengines.pls.service.impl;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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

import org.junit.Assert;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
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
        displayName = "file1.csv";
    }

    @Test(groups = "functional")
    public void testValidateHeaderFieldsWithSufficientColumns() throws FileNotFoundException {
        boolean noException = false;
        InputStream stream = new FileInputStream(csvFile);
        try {
            scoringFileMetadataService.validateHeaderFields(stream, createTableWithSufficientColumns().getAttributes(),
                    closeableResourcePool, displayName);
            noException = true;
        } catch (Exception e) {
            Assert.fail("Should not throw exception");
            e.printStackTrace();
        }
        Assert.assertTrue(noException);
    }

    @Test(groups = "functional")
    public void testValidateHeaderFieldsWithExtraColumns() throws FileNotFoundException {
        boolean exception = false;
        InputStream stream = new FileInputStream(csvFile);
        try {
            scoringFileMetadataService.validateHeaderFields(stream, createTableWithExtraColumns().getAttributes(),
                    closeableResourcePool, displayName);
            exception = true;
        } catch (Exception e) {
            Assert.assertFalse(exception);
        }
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
        sourceFile.setName("file1");
        when(sourceFileService.findByName(anyString())).thenReturn(sourceFile);
        Mockito.doNothing().when(sourceFileService).update(any(SourceFile.class));

        MetadataProxy metadataProxy = Mockito.mock(MetadataProxy.class);
        Mockito.doNothing().when(metadataProxy).createTable(anyString(), anyString(), any(Table.class));

        ReflectionTestUtils.setField(scoringFileMetadataService, "modelMetadataService", modelMetadataService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "sourceFileService", sourceFileService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "metadataProxy", metadataProxy);

        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setRequiredFields(Arrays.asList(new String[] { "A" }));

        FieldMapping mapping = new FieldMapping();
        mapping.setUserField("SomeId");
        mapping.setMappedField("A");
        mapping.setMappedToLatticeField(true);
        mapping.setFieldType(UserDefinedType.NUMBER);

        FieldMapping mapping2 = new FieldMapping();
        mapping.setUserField("SomeStr");
        mapping.setMappedField("B");
        mapping.setMappedToLatticeField(false);
        mapping.setFieldType(UserDefinedType.TEXT);

        fieldMappingDocument.setRequiredFields(Arrays.asList(new String[] { "A" }));
        fieldMappingDocument.setFieldMappings(Arrays.asList(new FieldMapping[] { mapping, mapping2 }));
        fieldMappingDocument.setIgnoredFields(Arrays.asList(new String[] { "C", "C_1" }));

        Tenant t = new Tenant();
        t.setId("t1");
        MultiTenantContext.setTenant(t);
        Table table = scoringFileMetadataService.saveFieldMappingDocument("csvfilename", "modelid", fieldMappingDocument);

        assertEquals(table.getAttributes().size(), 4);
        assertNotNull(table.getAttribute("C_2") != null );
        assertNotNull(table.getAttribute("C_1_1") != null );
    }

    private Table createTableWithSufficientColumns() {
        Table table = new Table();
        table.setName("TableWithSufficientColumns");

        Attribute att = new Attribute();
        att.setName(InterfaceName.Id.name());
        att.setDisplayName("Account ID");
        table.addAttribute(att);

        att = new Attribute();
        att.setName(InterfaceName.Website.name());
        att.setDisplayName("Website");
        table.addAttribute(att);

        att = new Attribute();
        att.setName(InterfaceName.Event.name());
        att.setDisplayName("Event");
        table.addAttribute(att);

        return table;
    }

    private Table createTableWithExtraColumns() {
        Table table = createTableWithSufficientColumns();
        table.setName("TableWithSufficientColumns");

        Attribute att = new Attribute();
        att.setName("ExtraColumn");
        att.setDisplayName("ExtraColumn");
        table.addAttribute(att);
        return table;
    }

}
