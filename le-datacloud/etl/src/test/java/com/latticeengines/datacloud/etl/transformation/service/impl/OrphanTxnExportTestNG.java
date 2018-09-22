package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.OrphanTxnExport;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class OrphanTxnExportTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration>{
    private static final Logger log = LoggerFactory.getLogger(OrphanTxnExportTestNG.class);
    GeneralSource source = new GeneralSource("OrphanTxnExportSeedClean");
    GeneralSource baseSourceAccount = new GeneralSource("OrphanTxnExportAccount");
    GeneralSource baseSourceProdcut = new GeneralSource("OrphanTxnExportProduct");
    GeneralSource baseSourceTxn = new GeneralSource("OrphanTxnExportTxn");

    private Object[][] accountData = new Object[][]{
            //"AccountId", "Name"
            {"A001", "Husky"},
            {"A002", "Alaskan Malamute"},
            {"A003", "Collie"},
            {"A004", "Chihuahua"},
            {"A005", "Labrador Retriever"},
    };

    private Object[][] productData = new Object[][]{
            //"ProductId", "ProductName"
            {"P0001", "test_product_1"},
            {"P0002", "test_product_2"},
            {"P0003", "test_product_3"},
            {"P0004", "test_product_4"},
            {"P0005", "test_product_5"}
    };

    private Object[][] transactionData = new String[][]{
            //"TransactionId", "AccountId", "ProductId", "TransactionCount"
            {"T00200","A001","P0002","200"},
            {"T01234","A005","P0010","200"},
            {"T06666","A010","P0088","300"},
            {"T08080","A004","P0003","150"},
            {"T18888","A006","P0004","998"}
    };

    private Object[][] expectedData = new Object[][]{
            //"TransactionId", "AccountId", "ProductId", "TransactionCount"
            {"A006","P0004","T18888","998"},
            {"A010","P0088","T06666","300"},
            {"A005","P0010","T01234","200"}
    };

    @Test(groups = "pipeline1")
    public void testOrphanTxnExport(){
        prepareAccountData();
        prepareProductData();
        prepareTxnData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected  PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("OrphanTxnExport");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(baseSourceAccount.getSourceName());
        baseSources.add(baseSourceProdcut.getSourceName());
        baseSources.add(baseSourceTxn.getSourceName());

        step1.setBaseSources(baseSources);
        step1.setTransformer(OrphanTxnExport.TRANSFORMER_NAME);
        step1.setTargetSource(source.getSourceName());
        step1.setConfiguration("{}");

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        configuration.setSteps(steps);
        return configuration;
    }

    private void prepareAccountData(){
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.Name.name(), String.class));
        uploadBaseSourceData(baseSourceAccount.getSourceName(), baseSourceVersion, columns, accountData);
    }

    private void prepareProductData(){
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductName.name(), String.class));
        uploadBaseSourceData(baseSourceProdcut.getSourceName(),baseSourceVersion,columns, productData);
    }

    private void prepareTxnData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.TransactionId.name(), String.class));
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        columns.add(Pair.of(InterfaceName.TransactionCount.name(), String.class));
        uploadBaseSourceData(baseSourceTxn.getSourceName(),baseSourceVersion,columns, transactionData);
    }


    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }


    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertTrue(isObjEquals(record.get(InterfaceName.AccountId.name()), expectedData[rowNum][0]));
            Assert.assertTrue(isObjEquals(record.get(InterfaceName.ProductId.name()), expectedData[rowNum][1]));
            Assert.assertTrue(isObjEquals(record.get(InterfaceName.TransactionId.name()), expectedData[rowNum][2]));
            Assert.assertTrue(isObjEquals(record.get(InterfaceName.TransactionCount.name()), expectedData[rowNum][3]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedData.length);
    }

}
