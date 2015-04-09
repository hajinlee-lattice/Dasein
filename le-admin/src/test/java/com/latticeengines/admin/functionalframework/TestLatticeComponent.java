package com.latticeengines.admin.functionalframework;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Component;

import com.google.common.io.Files;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

@Component("testLatticeComponent")
public class TestLatticeComponent extends LatticeComponent {

    private CustomerSpaceServiceInstaller installer = new TestInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new TestUpgrader();

    private static File componentConfigSourceDir;

    private CustomerSpaceServiceScope scope = null;

    public TestLatticeComponent() throws Exception {
        componentConfigSourceDir = Files.createTempDir();

        createTextFile(componentConfigSourceDir + "/fc.json", "{ \"PROP1\": \"value1\" }");
        createDirectory(componentConfigSourceDir + "/1");
        createTextFile(componentConfigSourceDir + "/1/fc_1.json", "{ \"PROP2\": \"value2\" }");
        createTextFile(componentConfigSourceDir + "/metadata.xml", "<metadata><property name=\"PROP1\" type=\"String\"/></metadata>");

        Map<String, String> overrideProps = new HashMap<>();
        overrideProps.put("PROP1", "abc");

        scope = new CustomerSpaceServiceScope("CONTRACT1", //
                "TENANT1", //
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, //
                getName(), //
                1, //
                overrideProps);
    }

    public CustomerSpaceServiceScope getScope() {
        return scope;
    }

    private static void createDirectory(String path) {
        File dir = new File(path);
        dir.mkdir();
        dir.deleteOnExit();
    }

    private static void createTextFile(String path, String contents) throws FileNotFoundException {
        try (PrintWriter w = new PrintWriter(path)) {
            w.print(contents);
        }
        new File(path).deleteOnExit();
    }

    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(componentConfigSourceDir);
    }

    @Override
    public String getName() {
        return "TestComponent";
    }

    @Override
    public void setName(String name) {
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    public static class TestInstaller implements CustomerSpaceServiceInstaller {
        
        private DocumentDirectory getConvertedLocalDir(File localDir) {
            FileSystemGetChildrenFunction func;
            try {
                func = new FileSystemGetChildrenFunction(componentConfigSourceDir);
                return new DocumentDirectory(new Path("/"), func);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion,
                Map<String, String> properties) {
            assertTrue(properties.containsKey("PROP1"));
            return getConvertedLocalDir(componentConfigSourceDir);
        }

        @Override
        public DocumentDirectory getDefaultConfiguration(String serviceName) {
            return getConvertedLocalDir(componentConfigSourceDir);
        }
    }

    public static class TestUpgrader implements CustomerSpaceServiceUpgrader {

        @Override
        public DocumentDirectory upgrade(CustomerSpace space, String serviceName, int sourceVersion, int targetVersion,
                DocumentDirectory source, Map<String, String> properties) {
            return null;
        }
    }

}
