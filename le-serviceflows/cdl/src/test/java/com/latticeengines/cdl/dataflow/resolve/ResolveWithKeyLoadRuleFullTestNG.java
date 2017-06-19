package com.latticeengines.cdl.dataflow.resolve;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.cdl.KeyLoadStrategy;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.ResolveStagingAndRuntimeTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class ResolveWithKeyLoadRuleFullTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    /**
     * Runtime: {u'Timestamp': 1488386040509, u'Attr4': True, u'Attr5': None,
     * u'Attr2': 1.0, u'Attr3': 1, u'RowId': 1, u'Attr1': '1'} {u'Timestamp':
     * 1488386040509, u'Attr4': None, u'Attr5': None, u'Attr2': 1.0, u'Attr3':
     * 1, u'RowId': 2, u'Attr1': '2'} {u'Timestamp': 1488386040509, u'Attr4':
     * True, u'Attr5': None, u'Attr2': 1.0, u'Attr3': None, u'RowId': 3,
     * u'Attr1': '3'} {u'Timestamp': 1488386040509, u'Attr4': True, u'Attr5':
     * None, u'Attr2': None, u'Attr3': 1, u'RowId': 4, u'Attr1': '4'}
     * {u'Timestamp': 1488386040509, u'Attr4': True, u'Attr5': 'only me',
     * u'Attr2': 1.0, u'Attr3': 1, u'RowId': 5, u'Attr1': None}
     * 
     * 
     * Staging: {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': None,
     * u'Attr3_added': 1, u'RowId': 1, u'Attr1': '1', u'Attr2': 1.0}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': None,
     * u'Attr3_added': None, u'RowId': 3, u'Attr1': '3', u'Attr2': 1.0}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': None,
     * u'Attr3_added': 1, u'RowId': 4, u'Attr1': '4', u'Attr2': None}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': 'only me but
     * better', u'Attr3_added': 1, u'RowId': 5, u'Attr1': None, u'Attr2': None}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': 'extra1',
     * u'Attr3_added': 1, u'RowId': 6, u'Attr1': None, u'Attr2': 1.0}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': 'extra2',
     * u'Attr3_added': 1, u'RowId': 7, u'Attr1': None, u'Attr2': 1.0}
     * 
     * Expected Result: {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5':
     * None, u'Attr3_added': 1, u'RowId': 1, u'Attr1': '1', u'Attr2': 1.0}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': None,
     * u'Attr3_added': None, u'RowId': 3, u'Attr1': '3', u'Attr2': 1.0}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': None,
     * u'Attr3_added': 1, u'RowId': 4, u'Attr1': '4', u'Attr2': None}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': 'only me but
     * better', u'Attr3_added': 1, u'RowId': 5, u'Attr1': None, u'Attr2': None}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': 'extra1',
     * u'Attr3_added': 1, u'RowId': 6, u'Attr1': None, u'Attr2': 1.0}
     * {u'Timestamp': 1488391998461, u'Attr4': True, u'Attr5': 'extra2',
     * u'Attr3_added': 1, u'RowId': 7, u'Attr1': None, u'Attr2': 1.0}
     * 
     */
    @Test(groups = "functional")
    public void test() {
        ResolveStagingAndRuntimeTableParameters parameters = new ResolveStagingAndRuntimeTableParameters();
        parameters.keyLoadStrategy = KeyLoadStrategy.Full;
        parameters.stageTableName = "AccountStaging";
        parameters.runtimeTableName = "Account";
        executeDataFlow(parameters);

        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 6);
        for (GenericRecord record : outputRecords) {
            if (record.get("RowId").equals(5)) {
                assertEquals(record.get("Attr5"), "only me but better");
                assertNull(record.get("Attr2"));
            }
        }

    }

    @Override
    protected String getFlowBeanName() {
        return "resolveStagingAndRuntimeTable";
    }

    @Override
    protected String getScenarioName() {
        return "nonEmptyRuntimeTable";
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return "RowId";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }

}
