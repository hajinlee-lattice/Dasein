package com.latticeengines.objectapi.functionalframework;

import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_DIR;
import static com.latticeengines.query.functionalframework.QueryTestUtils.ATTR_REPO_S3_FILENAME;
import static com.latticeengines.query.functionalframework.QueryTestUtils.TABLEJSONS_S3_FILENAME;
import static com.latticeengines.query.functionalframework.QueryTestUtils.TABLES_S3_FILENAME;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;
import com.latticeengines.query.functionalframework.QueryTestUtils;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-objectapi-context.xml" })
public class ObjectApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    protected QueryEvaluator queryEvaluator;

    @Autowired
    private TestArtifactService testArtifactService;

    @Inject
    protected Configuration yarnConfiguration;

    @Value("${camille.zk.pod.id}")
    private String podId;

    protected AttributeRepository attrRepo;
    protected CustomerSpace customerSpace;
    protected Map<String, String> tblPathMap;

    protected void initializeAttributeRepo(int version, boolean uploadHdfs) {
        InputStream is = testArtifactService.readTestArtifactAsStream(ATTR_REPO_S3_DIR,
                String.valueOf(version), ATTR_REPO_S3_FILENAME);
        attrRepo = QueryTestUtils.getCustomerAttributeRepo(is);
        if (version >= 3) {
            if (uploadHdfs) {
                tblPathMap = new HashMap<>();
                Map<TableRoleInCollection, String> pathMap = readTablePaths(version);
                for (TableRoleInCollection role : QueryTestUtils.getRolesInAttrRepo()) {
                    String tblName = QueryTestUtils.getServingStoreName(role, version);
                    String path = pathMap.get(role);
                    tblPathMap.put(tblName, path);
                }
                uploadTablesToHdfs(attrRepo.getCustomerSpace(), version);
            }
            insertPurchaseHistory(attrRepo);
            for (TableRoleInCollection role : QueryTestUtils.getRolesInAttrRepo()) {
                String tblName = QueryTestUtils.getServingStoreName(role, version);
                attrRepo.changeServingStoreTableName(role, tblName);
            }
        }
        customerSpace = attrRepo.getCustomerSpace();
    }

    private Map<TableRoleInCollection, String> readTablePaths(int version) {
        String downloadsDir = "downloads";
        File downloadedFile = testArtifactService.downloadTestArtifact(ATTR_REPO_S3_DIR, //
                String.valueOf(version), TABLEJSONS_S3_FILENAME);
        String zipFilePath = downloadedFile.getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(downloadsDir);
        } catch (ZipException e) {
            throw new RuntimeException("Failed to unzip tables archive " + zipFilePath, e);
        }
        Map<TableRoleInCollection, String> pathMap = new HashMap<>();
        QueryTestUtils.getRolesInAttrRepo().forEach(role -> {
            try {
                File tableJsonFile = new File(downloadsDir + File.separator + "TableJsons/" + role + ".json");
                Table table = JsonUtils.deserialize(FileUtils.openInputStream(tableJsonFile), Table.class);
                String path = table.getExtracts().get(0).getPath();
                path = path.replace("/Pods/QA/", "/Pods/" + podId + "/");
                pathMap.put(role, path);
            } catch (IOException e) {
                throw new RuntimeException("Cannot open table json file for " + role);
            }
        });
        return pathMap;
    }

    private void insertPurchaseHistory(AttributeRepository attrRepo) {
        String downloadsDir = "downloads";
        TableRoleInCollection role = TableRoleInCollection.CalculatedPurchaseHistory;
        File tableJsonFile = new File(downloadsDir + File.separator + "TableJsons/" + role + ".json");
        try {
            Table table = JsonUtils.deserialize(FileUtils.openInputStream(tableJsonFile), Table.class);
            attrRepo.appendServingStore(BusinessEntity.PurchaseHistory, table);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open table json file for " + role, e);
        }
    }

    private void uploadTablesToHdfs(CustomerSpace customerSpace, int version) {
        String downloadsDir = "downloads";
        File downloadedFile = testArtifactService.downloadTestArtifact(ATTR_REPO_S3_DIR, //
                String.valueOf(version), TABLES_S3_FILENAME);
        String zipFilePath = downloadedFile.getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(downloadsDir);
        } catch (ZipException e) {
            throw new RuntimeException("Failed to unzip tables archive " + zipFilePath, e);
        }
        String targetPath = PathBuilder.buildCustomerSpacePath(podId, customerSpace).append("Data").toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, downloadsDir, targetPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy local dir to hdfs.", e);
        }
    }

    protected EventFrontEndQuery loadEventFrontEndQueryFromResource(String resourceName) {
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
            return JsonUtils.deserialize(inputStream, EventFrontEndQuery.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load json resource" + resourceName, e);
        }

    }

    protected FrontEndQuery loadFrontEndQueryFromResource(String resourceName) {
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
            return JsonUtils.deserialize(inputStream, FrontEndQuery.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load json resource" + resourceName, e);
        }

    }

}
