package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class DataSetEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {
    
    private DataSet dataSet;
    
    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        dataSetEntityMgr.deleteAll();
        
        dataSet = new DataSet();
        dataSet.setName("DataSet1");
        dataSet.setIndustry("Industry1");
        dataSet.setTenant(new Tenant("Tenant1"));
        dataSet.setDataSetType(DataSetType.FILE);
        dataSet.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);
        
        ScoringDataSet scoringDataSet = new ScoringDataSet();
        scoringDataSet.setName("ScoringDataSet1");
        scoringDataSet.setDataHdfsPath("ScoringDataSetPath1");
        dataSet.addScoringDataSet(scoringDataSet);
    }

    @Test(groups = "functional")
    public void create() {
        dataSetEntityMgr.create(dataSet);
        
        List<DataSet> dataSets = dataSetEntityMgr.findAll();
        assertEquals(dataSets.size(), 1);
        DataSet retrievedDataSet = dataSets.get(0);
        
        assertEquals(retrievedDataSet.getName(), dataSet.getName());
        assertEquals(retrievedDataSet.getIndustry(), dataSet.getIndustry());
        assertEquals(retrievedDataSet.getTenant().getId(), dataSet.getTenant().getId());
        assertEquals(retrievedDataSet.getDataSetType(), DataSetType.FILE);
        
        List<ScoringDataSet> scoringDataSets = dataSet.getScoringDataSets();
        List<ScoringDataSet> retrievedScoringDataSets = retrievedDataSet.getScoringDataSets();
        assertEquals(scoringDataSets.size(), 1);
        assertEquals(scoringDataSets.size(), retrievedScoringDataSets.size());
        
        ScoringDataSet scoringDataSet = scoringDataSets.get(0);
        ScoringDataSet retrievedScoringDataSet = retrievedScoringDataSets.get(0);
        
        assertEquals(scoringDataSet.getName(), retrievedScoringDataSet.getName());
        assertEquals(scoringDataSet.getDataHdfsPath(), retrievedScoringDataSet.getDataHdfsPath());
        
        
        
    }
}
