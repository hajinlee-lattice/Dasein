package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.persistence.criteria.CriteriaBuilder.Case;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.hibernate.cfg.EJB3DTDEntityResolver;
import org.opensaml.ws.wsfed.RequestSecurityTokenResponse;
import org.opensaml.ws.wssecurity.impl.AbstractWSSecurityObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.metadata.ListenableMetadataStore;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Tables;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.impl.DataCollectionServiceImpl;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace.TableSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.mchange.io.IOStringObjectMap;
import com.mysql.fabric.xmlrpc.base.Data;
import com.sun.tools.classfile.InnerClasses_attribute.Info;



public class DataCollectionResourceDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataCollectionResourceDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataCollectionService dataCollectionService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        cdlTestDataService.populateData(mainTestTenant.getId());
    }

    @Test(groups = "deployment")
    public void testGetCDLDataSpace() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        CDLDataSpace cdlDataSpace = dataCollectionProxy.getCDLDataSpace(customerSpace);
        Assert.assertNotNull(cdlDataSpace);
        Assert.assertEquals(cdlDataSpace.getEntities().size(), 5);
        Assert.assertEquals(cdlDataSpace.getActiveVersion(), Version.Blue);
        for(Map.Entry<BusinessEntity, Map<BusinessEntity.DataStore, List<TableSpace>>> entitiesEntry:cdlDataSpace.getEntities().entrySet()) {
            Assert.assertNotNull(entitiesEntry.getValue());
            Assert.assertTrue(entitiesEntry.getValue().size() <= 2);
            for (Map.Entry<BusinessEntity.DataStore, List<TableSpace>> entry:entitiesEntry.getValue().entrySet()) {
                Assert.assertNotNull(entry.getValue());
                List<TableSpace> testTableSpaceList = entry.getValue();
                switch(entry.getKey()) {
                    case Serving:
                        for (TableSpace testTableSpace:testTableSpaceList) {
                            Assert.assertNotNull(testTableSpace.getTables());
                        }
                        break;
                    case Batch:
                        for (TableSpace testTableSpace:testTableSpaceList) {
                            Assert.assertNotNull(testTableSpace.getHdfsPath());
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        Assert.assertEquals(cdlDataSpace.getEntities().get(BusinessEntity.PeriodTransaction).get(BusinessEntity.DataStore.Serving).get(0).getTableRole(), TableRoleInCollection.AggregatedPeriodTransaction);
        Assert.assertNotNull(cdlDataSpace.getOthers());
    }
}
