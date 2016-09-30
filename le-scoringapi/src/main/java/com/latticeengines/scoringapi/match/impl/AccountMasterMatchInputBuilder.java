package com.latticeengines.scoringapi.match.impl;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component
public class AccountMasterMatchInputBuilder extends AbstractMatchInputBuilder {
    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    public void setDataCloudVersion(ModelSummary modelSummary, MatchInput matchInput,
            Map<Predefined, String> predefinedSelections) {
        super.setDataCloudVersion(modelSummary, matchInput, predefinedSelections);

        // TODO - work with Lei to fis RTS version to 2.0 during AM
        // based model creation
        predefinedSelections.put(modelSummary.getPredefinedSelection(), "2.0");
    }
}
