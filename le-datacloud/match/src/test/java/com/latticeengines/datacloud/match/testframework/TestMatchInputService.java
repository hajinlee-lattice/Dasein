package com.latticeengines.datacloud.match.testframework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.service.impl.AccountMasterColumnServiceImpl;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component("testMatchInputService")
public class TestMatchInputService {

    @Autowired
    private AccountMasterColumnServiceImpl accountMasterColumnService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    public MatchInput prepareSimpleRTSMatchInput(Object[][] data) {
        return TestMatchInputUtils.prepareSimpleMatchInput(data, null);
    }

    public MatchInput prepareSimpleAMMatchInput(Object[][] data) {
        MatchInput input = prepareSimpleRTSMatchInput(data);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        return input;
    }

    public MatchInput prepareSimpleAMMatchInput(Object[][] data, String[] fields) {
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, fields);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        return input;
    }

    public ColumnSelection enrichmentSelection() {
        return constructColumnSelection(Arrays.asList( //
                "TechIndicator_AddThis", //
                "TechIndicator_AdobeCreativeSuite", //
                InterfaceName.IsMatched.name()));
    }

    public ColumnSelection companyProfileSelection() {
        return constructColumnSelection(Arrays.asList( //
                "LDC_Domain", //
                "LDC_Name", //
                "LDC_State", //
                "LDC_Country", //
                "LE_EMPLOYEE_RANGE", //
                "LE_REVENUE_RANGE"));
    }

    private ColumnSelection constructColumnSelection(Collection<String> columnIds) {
        List<AccountMasterColumn> columns = new ArrayList<>();
        columnIds.forEach(id -> columns.add(accountMasterColumnService.getMetadataColumn(id,
                versionEntityMgr.currentApprovedVersion().getVersion())));
        ColumnSelection cs = new ColumnSelection();
        cs.createAccountMasterColumnSelection(columns);
        return cs;
    }

}
