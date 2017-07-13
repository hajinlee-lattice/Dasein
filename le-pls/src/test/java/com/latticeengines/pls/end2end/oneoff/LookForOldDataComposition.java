package com.latticeengines.pls.end2end.oneoff;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class LookForOldDataComposition extends PlsFunctionalTestNGBase {

    private static final String EVENT_METADATA = "-Event-Metadata";

    private static final Logger log = LoggerFactory.getLogger(LookForOldDataComposition.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
    }

    @Test(groups = "manual", enabled = false)
    public void analyze() throws Exception {
        List<MSHolder> models = new ArrayList<>();

        @SuppressWarnings("unchecked")
        List<String> paths = JsonUtils.deserialize(discoveredDeprecatedDCPaths, ArrayList.class);
        for (String path : paths) {
            String[] splits = path.split("/");
            String lookup = splits[8];
            if (lookup.endsWith(EVENT_METADATA)) {
                String prefix = lookup.substring(0, lookup.length() - EVENT_METADATA.length());
                MSHolder msHolder = lookup(prefix);
                if (msHolder != null && msHolder.status != 2) {
                    models.add(msHolder);
                }
            }
        }
        for (MSHolder model : models) {
            MultiTenantContext.setTenant(model.tenant);
            modelSummaryEntityMgr.deleteByModelId(model.id);
        }
        log.info(JsonUtils.serialize(models));
    }

    private MSHolder lookup(String prefix) {
        MSHolder msHolder = null;
        try {
            msHolder = jdbcTemplate.queryForObject(
                    "select ID,NAME,STATUS,TENANT_ID,CONSTRUCTION_TIME from MODEL_SUMMARY where LOOKUP_ID LIKE ?",
                    new Object[] { "%" + prefix + "%" }, new RowMapper<MSHolder>() {
                        @Override
                        public MSHolder mapRow(ResultSet rs, int rowNumber) throws SQLException {
                            MSHolder modelSummary = new MSHolder();
                            modelSummary.id = (rs.getString("ID"));
                            modelSummary.name = (rs.getString("NAME"));
                            modelSummary.status = (rs.getInt("STATUS"));
                            int tenantPid = (rs.getInt("TENANT_ID"));
                            Tenant tenant = tenantEntityMgr.findByField("TENANT_PID", tenantPid);
                            modelSummary.tenant = tenant;
                            modelSummary.tenantId = tenant.getId();
                            long time = (rs.getLong("CONSTRUCTION_TIME"));
                            modelSummary.constructionTime = new Date(time).toString();

                            return modelSummary;
                        }
                    });
        } catch (EmptyResultDataAccessException e) {
            log.info("No results found for " + prefix);
        }
        return msHolder;
    }

    @SuppressWarnings("unused")
    private static class MSHolder {
        public String id;        
        public String name;
        public String tenantId;
        public Tenant tenant;
        public int status;
        public String constructionTime;
    }

    @Test(groups = "manual", enabled = false)
    public void discover() throws Exception {
        log.info("Begin operation");
        yarnConfiguration.setInt("dfs.replication", 3);

        List<String> customerPaths = HdfsUtils.getFilesForDir(yarnConfiguration, "/user/s-analytics/customers/",
                new HdfsFileFilter() {
                    @Override
                    public boolean accept(FileStatus file) {
                        return true;
                    }

                });

        List<String> allDataCompositionPaths = new ArrayList<>();

        for (String customerPath : customerPaths) {
            log.info("Analyzing customer: " + customerPath);
            try {
                List<String> customerDataCompositionPaths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration,
                        customerPath + "/data", new HdfsFileFilter() {
                            @Override
                            public boolean accept(FileStatus file) {
                                return file.getPath().getName().equals("datacomposition.json");
                            }
                        });
                allDataCompositionPaths.addAll(customerDataCompositionPaths);
            } catch (java.io.FileNotFoundException e) {
                log.info("No data dir for customer " + customerPath);
            }
        }

        List<String> notReadablePaths = new ArrayList<>();
        List<String> deprecatedDCPaths = new ArrayList<>();

        for (String dataCompositionPath : allDataCompositionPaths) {
            log.info("Processing " + dataCompositionPath);

            String contents = null;
            try {
                contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, dataCompositionPath);
            } catch (Exception e) {
                log.error("Not able to read contents for " + dataCompositionPath);
                notReadablePaths.add(dataCompositionPath);
                continue;
            }
            try {
                JsonUtils.deserialize(contents, DataComposition.class);
            } catch (Exception e) {
                boolean featureCase = false;
                if (contents.contains("\"FEATURE\"")) {
                    featureCase = true;
                }
                log.error(String.format("Deprecated datacomposition found with featureCase %s at %s\n%s", featureCase,
                        dataCompositionPath, contents));
                deprecatedDCPaths.add(dataCompositionPath);
            }
        }

        log.info("notReadablePaths --- " + JsonUtils.serialize(notReadablePaths));
        log.info("deprecatedDCPaths --- " + JsonUtils.serialize(deprecatedDCPaths));
    }

    private static String discoveredDeprecatedDCPaths = "[\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Adobe_LP3.Adobe_LP3.Production/data/367a9d8e-6fc3-4091-a579-a2d553f8fd3b-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Adobe_LP3.Adobe_LP3.Production/data/RunMatchWithLEUniverse_35020_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Adobe_LP3.Adobe_LP3.Production/data/RunMatchWithLEUniverse_35010_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Adobe_LP3.Adobe_LP3.Production/data/RunMatchWithLEUniverse_35009_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Adobe_LP3.Adobe_LP3.Production/data/66cc2ac4-aba5-42a5-9318-111a4088c459-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Adobe_LP3.Adobe_LP3.Production/data/RunMatchWithLEUniverse_34870_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Demo_LP3.Demo_LP3.Production/data/RunMatchWithLEUniverse_29778_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Demo_LP3.Demo_LP3.Production/data/RunMatchWithLEUniverse_36038_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Demo_LP3.Demo_LP3.Production/data/3a754f7c-66ae-4fcc-9f69-7969fadca015-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/LP3ModelQuality02.LP3ModelQuality02.Production/data/8d925982-9956-46ce-82fa-69b6107c0479-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/LP3ModelQuality02.LP3ModelQuality02.Production/data/RunMatchWithLEUniverse_35243_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/LP3ModelQuality02.LP3ModelQuality02.Production/data/RunMatchWithLEUniverse_36184_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/LP3ModelQuality02.LP3ModelQuality02.Production/data/RunMatchWithLEUniverse_36192_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/LP3ModelQuality02.LP3ModelQuality02.Production/data/RunMatchWithLEUniverse_36490_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Lattice_LP3.Lattice_LP3.Production/data/RunMatchWithLEUniverse_29803_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/Malwarebytes_LP3.Malwarebytes_LP3.Production/data/RunMatchWithLEUniverse_35903_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/SureShot_Test_Tenant1.SureShot_Test_Tenant1.Production/data/RunMatchWithLEUniverse_152699_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/SureShot_Test_Tenant1.SureShot_Test_Tenant1.Production/data/RunMatchWithLEUniverse_152700_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/SureShot_Test_Tenant2.SureShot_Test_Tenant2.Production/data/RunMatchWithLEUniverse_152706_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/SureShot_Test_Tenant2.SureShot_Test_Tenant2.Production/data/RunMatchWithLEUniverse_152701_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/SureShot_Test_Tenant3.SureShot_Test_Tenant3.Production/data/RunMatchWithLEUniverse_152708_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/SureShot_Test_Tenant3.SureShot_Test_Tenant3.Production/data/RunMatchWithLEUniverse_152707_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_29797_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_29794_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_152544_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_152579_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_152625_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/860e5331-3f18-439e-898d-5ea24525e4b2-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_152551_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_152571_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/YSong_DplTest.YSong_DplTest.Production/data/RunMatchWithLEUniverse_63843_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_33651_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_152592_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/b3b299d1-9fee-4dfb-bae5-078c89546d78-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_33830_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/311e8406-488d-455c-9b08-8b0a45502530-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_152590_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_152617_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_34642_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_33811_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_34653_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_35236_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/RunMatchWithLEUniverse_34873_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/dbf4dd04-046d-4997-9b11-3a70a6b3940f-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/demo_jeff.demo_jeff.Production/data/c31b2b6a-e9c5-4d3a-ad3f-33f00e86ca49-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/dae48db0-f24b-40f9-81e1-f5abaef1a587-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/4e531143-185f-492e-b7d0-d7d31f1806b4-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_34386_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_34444_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_35225_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/6477c016-b559-4693-9641-0e2ed7fdc16f-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/b120b299-ea7c-411c-9a25-23c0300d95c4-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/e3019e17-e2a2-494e-9a4b-c186aa34ddaf-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_152698_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/08aa2a48-d579-4b4b-90dc-8c79edbd2c80-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_63822_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_152705_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/72a16336-09f9-46b4-8046-a3952b3ef725-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/3b31acf9-f382-468a-8099-5a7e82b2d6aa-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_33825_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_29801_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_152618_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_29798_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_29789_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/b25e1651-d416-49d4-bb46-466229aa6e20-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_35007_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_29799_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_35208_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/e5a02373-f234-48b2-a02b-0b60c9b5a418-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_34472_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/fmeng_lp3_demo.fmeng_lp3_demo.Production/data/RunMatchWithLEUniverse_36218_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_35222_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152671_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152657_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152598_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152679_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152584_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152642_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152662_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152616_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152665_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152685_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152710_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_152574_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/ad6c51cc-9329-419e-860e-bdc63f06f495-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash1.lp3_bugbash1.Production/data/RunMatchWithLEUniverse_63876_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152581_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152686_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152667_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152677_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152656_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152641_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152664_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash3.lp3_bugbash3.Production/data/RunMatchWithLEUniverse_152589_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash6.lp3_bugbash6.Production/data/RunMatchWithLEUniverse_34214_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbash6.lp3_bugbash6.Production/data/RunMatchWithLEUniverse_152690_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152570_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152650_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152660_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152586_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152653_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152643_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152663_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush2.lp3_bugbush2.Production/data/RunMatchWithLEUniverse_152678_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush4.lp3_bugbush4.Production/data/RunMatchWithLEUniverse_152672_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush4.lp3_bugbush4.Production/data/RunMatchWithLEUniverse_152668_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush4.lp3_bugbush4.Production/data/RunMatchWithLEUniverse_33654_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush4.lp3_bugbush4.Production/data/RunMatchWithLEUniverse_152687_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush4.lp3_bugbush4.Production/data/RunMatchWithLEUniverse_152676_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush4.lp3_bugbush4.Production/data/RunMatchWithLEUniverse_152661_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152673_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152655_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152669_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152654_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152591_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152644_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152652_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_33657_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152666_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\",\n"
            + "    \"hdfs://ledpprod2/user/s-analytics/customers/lp3_bugbush5.lp3_bugbush5.Production/data/RunMatchWithLEUniverse_152688_DerivedColumnsCache_with_std_attrib-Event-Metadata/datacomposition.json\"\n"
            + "]";
}
