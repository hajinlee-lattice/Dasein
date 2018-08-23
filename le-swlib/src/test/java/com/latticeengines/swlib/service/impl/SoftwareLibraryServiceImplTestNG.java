package com.latticeengines.swlib.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.functionalframework.SWLibFunctionalTestNGBase;

public class SoftwareLibraryServiceImplTestNG extends SWLibFunctionalTestNGBase {

    @Inject
    private SoftwareLibraryServiceImpl softwareLibraryService;

    @Inject
    private Configuration yarnConfiguration;

    private SoftwarePackage pkgVersion1;

    private String jarFile;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        jarFile = ClassLoader.getSystemResource("com/latticeengines/swlib/service/impl/a.jar").getPath();
        pkgVersion1 = new SoftwarePackage();
        pkgVersion1.setGroupId("com.latticeengines");
        pkgVersion1.setArtifactId("le-serviceflows");
        pkgVersion1.setModule("dataflow");
        pkgVersion1.setInitializerClass("xyz");

        SoftwarePackage pkgVersion2 = new SoftwarePackage();
        pkgVersion2.setGroupId("com.latticeengines");
        pkgVersion2.setArtifactId("le-serviceflows");
        pkgVersion2.setModule("dataflow");
        pkgVersion2.setInitializerClass("abc");
    }

    @Test(groups = "functional")
    public void createSoftwareLibDirExpectedToFail() {
        boolean exception = false;
        try {
            softwareLibraryService.createSoftwareLibDir("/app/swlib1:/");
        } catch (Exception e) {
            exception = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_27000);
        }
        assertTrue(exception);
    }

    @Test(groups = "functional")
    public void validateInitialSetup() throws Exception {
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, softwareLibraryService.getTopLevelPath()));
    }

    @Test(groups = "functional", dependsOnMethods = { "validateInitialSetup" })
    public void installPackage() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, softwareLibraryService.getTopLevelPath() + "/dataflow");
        softwareLibraryService.installPackage(pkgVersion1, new File(jarFile));
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration,
                String.format("%s/%s", softwareLibraryService.getTopLevelPath(), pkgVersion1.getHdfsPath("json")));
        SoftwarePackage deserializedPkg = JsonUtils.deserialize(contents, SoftwarePackage.class);

        assertEquals(deserializedPkg.getGroupId(), pkgVersion1.getGroupId());
        assertEquals(deserializedPkg.getArtifactId(), pkgVersion1.getArtifactId());
        assertEquals(deserializedPkg.getClassifier(), pkgVersion1.getClassifier());
    }

    @Test(groups = "functional", dependsOnMethods = { "installPackage" })
    public void installPackageThatAlreadyExists() {
        boolean exception = false;
        try {
            softwareLibraryService.installPackage(pkgVersion1, new File(jarFile));
        } catch (Exception e) {
            exception = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_27002);
        }
        assertTrue(exception);
    }

    @Test(groups = "functional", dependsOnMethods = { "installPackage" })
    public void getInstalledPackages() {
        List<SoftwarePackage> packages = softwareLibraryService.getInstalledPackages("dataflow");

        assertEquals(packages.size(), 1);
        SoftwarePackage deserializedPkg = packages.get(0);

        assertEquals(deserializedPkg.getGroupId(), pkgVersion1.getGroupId());
        assertEquals(deserializedPkg.getArtifactId(), pkgVersion1.getArtifactId());
        assertEquals(deserializedPkg.getClassifier(), pkgVersion1.getClassifier());

    }

    @Test(groups = "functional")
    public void getInstalledPackagesMissingModule() {
        List<SoftwarePackage> packages = softwareLibraryService.getInstalledPackages("xyz");
        assertEquals(packages.size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "getInstalledPackages" })
    public void getInstalledPackagesNonSWPackageJsonFile() throws Exception {
        String[] pkgTokens = pkgVersion1.getHdfsPath("json").split("/");
        pkgTokens[pkgTokens.length - 1] = "a.json";
        String filePath = String.format("%s/%s", softwareLibraryService.getTopLevelPath(), StringUtils.join(pkgTokens, "/"));
        HdfsUtils.writeToFile(yarnConfiguration, filePath, "xyz");
        List<SoftwarePackage> packages = softwareLibraryService.getInstalledPackages("dataflow");
        assertEquals(packages.size(), 1);
    }

}
