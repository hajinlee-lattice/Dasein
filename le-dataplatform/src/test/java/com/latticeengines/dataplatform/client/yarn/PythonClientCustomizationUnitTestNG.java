package com.latticeengines.dataplatform.client.yarn;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class PythonClientCustomizationUnitTestNG {

    private PythonClientCustomization customization = new PythonClientCustomization();

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        List<SoftwarePackage> packages = new ArrayList<>();
        packages.add(createSoftwarePackage("le-serviceflows-prospectdiscovery"));
        packages.add(createSoftwarePackage("le-serviceflows-leadprioritization"));
        packages.add(createSoftwarePackage("le-serviceflows-propdata"));
        SoftwareLibraryService swlibService = mock(SoftwareLibraryService.class);
        swlibService.setStackName("");
        when(swlibService.getTopLevelPath()).thenReturn("/app/swlib");
        when(swlibService.getInstalledPackagesByVersion(anyString(), anyString())).thenReturn(
                packages);
        ReflectionTestUtils.setField(customization, "softwareLibraryService", swlibService);

        VersionManager versionManager = mock(VersionManager.class);
        when(versionManager.getCurrentVersionInStack(anyString())).thenReturn("2.0.22-SNAPSHOT");
        ReflectionTestUtils.setField(customization, "versionManager", versionManager);
        ReflectionTestUtils.setField(customization, "stackName", "");
    }

    private SoftwarePackage createSoftwarePackage(String artifactId) {
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setArtifactId(artifactId);
        pkg.setModule("dataflowapi");
        pkg.setVersion("2.0.22-SNAPSHOT");
        pkg.setGroupId("com.latticeengines");
        return pkg;
    }

    @Test(groups = "unit", dataProvider = "provenancePropertyProvider")
    public void getSwlibArtifactHdfsPath(String provenanceProperties, String expectedValue) {
        Classifier classifier = mock(Classifier.class);
        when(classifier.getProvenanceProperties()).thenReturn(provenanceProperties);
        when(classifier.getPythonPipelineLibHdfsPath()).thenReturn("pipelinepath");
        assertEquals(customization.getSwlibArtifactHdfsPath(classifier), expectedValue);
    }

    @Test(groups = "unit")
    public void getSwlibArtifactHdfsPathNullClassifier() {
        assertEquals(customization.getSwlibArtifactHdfsPath(null), "");
    }

    @Test(groups = "unit")
    public void getSwlibArtifactHdfsPathNullProvenanceProperties() {
        Classifier classifier = mock(Classifier.class);
        when(classifier.getProvenanceProperties()).thenReturn(null);
        when(classifier.getPythonPipelineLibHdfsPath()).thenReturn("pipelinepath");
        assertEquals(customization.getSwlibArtifactHdfsPath(classifier), "pipelinepath");
    }

    @Test(groups = "unit")
    public void getSwlibArtifactHdfsPathEmptyProvenanceProperties() {
        Classifier classifier = mock(Classifier.class);
        when(classifier.getProvenanceProperties()).thenReturn("");
        when(classifier.getPythonPipelineLibHdfsPath()).thenReturn("pipelinepath");
        assertEquals(customization.getSwlibArtifactHdfsPath(classifier), "pipelinepath");
    }

    @DataProvider(name = "provenancePropertyProvider")
    public Object[][] provenancePropertyProvider() {
        return new Object[][] { //
                {
                        StringUtils.join(
                                new String[] { //
                                "swlib.module=dataflowapi", //
                                        "swlib.group_id=com.latticeengines", //
                                        "swlib.version=2.0.22-SNAPSHOT", //
                                        "swlib.artifact_id=le-serviceflows-leadprioritization" },
                                " "), //
                        "/app/swlib/dataflowapi/com/latticeengines/le-serviceflows-leadprioritization/2.0.22-SNAPSHOT/le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.jar" }, //
                {
                        StringUtils.join(new String[] { //
                                "swlib.module=dataflowapi", //
                                        "swlib.group_id=com.latticeengines", //
                                        "swlib.version=2.0.22-SNAPSHOT" }, " "), //
                        "/app/swlib/dataflowapi/com/latticeengines/le-serviceflows-prospectdiscovery/2.0.22-SNAPSHOT/le-serviceflows-prospectdiscovery-2.0.22-SNAPSHOT.jar" }

        };
    }
    
    @Test(groups = "unit")
    public void resetClassifierWithProperProvenanceProperties() {
        Classifier classifier = new Classifier();
        String provenanceProperties = StringUtils.join(
                new String[] { //
                        "DataLoader_TenantName=XYZ", //
                        "swlib.module=dataflowapi", //
                        "swlib.group_id=com.latticeengines", //
                        "swlib.version=latest", //
                        "swlib.artifact_id=le-serviceflows-leadprioritization" },
                " ");
        classifier.setProvenanceProperties(provenanceProperties);
        SoftwarePackage pkg = createSoftwarePackage("le-serviceflows-prospectdiscovery");
        customization.resetClassifierWithProperProvenanceProperties(classifier, pkg);
        String[] properties = classifier.getProvenanceProperties().split("\\s");
        assertEquals(properties.length, 5);
        for (String property : properties) {
            if (property.startsWith("swlib.version")) {
                assertEquals(property.split("=")[1], "2.0.22-SNAPSHOT");
            }
        }
    }
}
