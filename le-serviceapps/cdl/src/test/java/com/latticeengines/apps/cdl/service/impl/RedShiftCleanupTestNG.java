package com.latticeengines.apps.cdl.service.impl;

import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class RedShiftCleanupTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private RedShiftCleanupService redshiftCleanupService;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    private String desTableName;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testCreateTable() {
        Tenant tenant = MultiTenantContext.getTenant();
        this.desTableName = tenant.getName() + "_" + System.currentTimeMillis();
        Table dataTable = new Table();
        dataTable.setName(this.desTableName);
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(tenant);
        PrimaryKey pk = createPrimaryKey();
        LastModifiedKey lk = createLastModifiedKey();
        dataTable.setPrimaryKey(pk);
        dataTable.setLastModifiedKey(lk);
        Extract e1 = createExtract("e1");
        Extract e2 = createExtract("e2");
        Extract e3 = createExtract("e3");
        dataTable.addExtract(e1);
        dataTable.addExtract(e2);
        dataTable.addExtract(e3);
        Attribute pkAttr = new Attribute();
        pkAttr.setName("ID");
        pkAttr.setDisplayName("Id");
        pkAttr.setLength(10);
        pkAttr.setPrecision(10);
        pkAttr.setScale(10);
        pkAttr.setPhysicalDataType(Schema.Type.INT.toString());
        pkAttr.setSourceLogicalDataType("Identity");
        pkAttr.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        dataTable.addAttribute(pkAttr);
        Attribute lkAttr = new Attribute();
        lkAttr.setName("LID");
        lkAttr.setDisplayName("LastUpdatedDate");
        lkAttr.setLength(20);
        lkAttr.setPrecision(20);
        lkAttr.setScale(20);
        lkAttr.setPhysicalDataType(Schema.Type.LONG.toString());
        lkAttr.setSourceLogicalDataType("Date");
        lkAttr.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        dataTable.addAttribute(lkAttr);
        Attribute attr = new Attribute();
        attr.setName("ExtraAttribute");
        attr.setDisplayName("ExtraAttribute");
        attr.setSourceLogicalDataType("");
        attr.setPhysicalDataType(Type.STRING.name());
        dataTable.addAttribute(attr);
        Schema schema = TableUtils.createSchema(dataTable.getName(), dataTable);
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setTableName(this.desTableName);
        redshiftTableConfig.setSortKeyType(SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(Arrays.asList("id"));
        redshiftTableConfig.setS3Bucket(s3Bucket);
        redshiftService.createTable(redshiftTableConfig, schema);
        assertTrue(redshiftService.hasTable(this.desTableName));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateTable")
    public void testRedshiftCleanup() {
        redshiftCleanupService.removeUnusedTableByTenant(MultiTenantContext.getCustomerSpace().toString());
        assertFalse(redshiftService.hasTable(this.desTableName));
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        if (redshiftService.hasTable(this.desTableName))
            redshiftService.dropTable(this.desTableName);
    }

    protected PrimaryKey createPrimaryKey() {
        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.setDisplayName("Primary Key for ID column");
        pk.addAttribute("ID");

        return pk;
    }

    protected LastModifiedKey createLastModifiedKey() {
        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LUD");
        lk.setDisplayName("Last Modified Key for LastUpdatedDate column");
        lk.addAttribute("LID");

        return lk;
    }

    protected Extract createExtract(String name) {
        Extract e = new Extract();
        e.setName(name);
        e.setPath("/" + name);
        e.setExtractionTimestamp(System.currentTimeMillis());
        return e;
    }
}
