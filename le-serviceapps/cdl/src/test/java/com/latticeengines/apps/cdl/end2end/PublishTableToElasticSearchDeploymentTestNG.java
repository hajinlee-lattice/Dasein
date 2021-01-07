package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.apps.cdl.end2end.ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.elasticsearch.PublishTableToESRequest;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.ElasticSearchDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PublishTableProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;

public class PublishTableToElasticSearchDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PublishTableToElasticSearchDeploymentTestNG.class);


    @Inject
    private PublishTableProxy publishTableProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private ElasticSearchService elasticSearchService;

    @BeforeClass(groups = {"end2end"})
    @Override
    public void setup() throws Exception {
        super.setup();
        // account/contact in check point, not time line profile
        resumeCheckpoint(CHECK_POINT);
    }

    @Test(groups = "end2end")
    public void testTimelineProfile() throws Exception {

        String tableName = setupTables();
        PublishTableToESRequest request = new PublishTableToESRequest();
        ElasticSearchExportConfig config = new ElasticSearchExportConfig();
        config.setSignature(ElasticSearchUtils.generateNewVersion());
        config.setTableRoleInCollection(TableRoleInCollection.TimelineProfile);
        config.setTableName(tableName);
        List<ElasticSearchExportConfig> configs = Collections.singletonList(config);
        request.setExportConfigs(configs);

        String appId = publishTableProxy.publishTableToES(mainCustomerSpace, request);

        JobStatus status = waitForWorkflowStatus(appId, false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        ElasticSearchDataUnit unit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(mainCustomerSpace,
                TableRoleInCollection.TimelineProfile.name(),
                DataUnit.StorageType.ElasticSearch);
        Assert.assertNotNull(unit);
        Assert.assertEquals(unit.getTableRole(), TableRoleInCollection.TimelineProfile);
        Assert.assertTrue(StringUtils.isNotBlank(unit.getSignature()));
        String entity = ElasticSearchUtils.getEntityFromTableRole(TableRoleInCollection.TimelineProfile);
        String signature = unit.getSignature();
        String indexName = ElasticSearchUtils.constructIndexName(CustomerSpace.shortenCustomerSpace(mainCustomerSpace),
                entity, signature);
        boolean exists = elasticSearchService.indexExists(indexName);
        Assert.assertTrue(exists);

    }

    private String setupTables() throws IOException {
        Table dunsCountTable = JsonUtils
                .deserialize(IOUtils.toString(ClassLoader.getSystemResourceAsStream(
                        "end2end/role/timelineprofile.json"), "UTF-8"), Table.class);
        String dunsCountTableName = NamingUtils.timestamp("dunsCount");
        dunsCountTable.setName(dunsCountTableName);
        Extract extract = dunsCountTable.getExtracts().get(0);
        extract.setPath(PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(),
                        CustomerSpace.parse(mainCustomerSpace))
                .append(dunsCountTableName).toString()
                + "/*.avro");
        dunsCountTable.setExtracts(Collections.singletonList(extract));
        metadataProxy.createTable(mainCustomerSpace, dunsCountTableName, dunsCountTable);

        String path = ClassLoader
                .getSystemResource("end2end/role").getPath();
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, //
                path + "/timelineprofile.avro", //
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(),
                                CustomerSpace.parse(mainCustomerSpace))
                        .append(dunsCountTableName).append("part1.avro").toString());
        return dunsCountTableName;
    }


}
