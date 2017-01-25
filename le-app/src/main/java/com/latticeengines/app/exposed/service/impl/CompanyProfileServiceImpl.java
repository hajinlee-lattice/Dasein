package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.CompanyProfileService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.mysql.jdbc.StringUtils;

@Component("companyProfileService")
public class CompanyProfileServiceImpl implements CompanyProfileService {

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Value("${ulysses.companyprofile.datacloud.version:}")
    private String dataCloudVersion;

    @Override
    public CompanyProfile getProfile(CustomerSpace customerSpace, Map<String, String> values, boolean enforceFuzzyMatch) {
        MatchInput matchInput = new MatchInput();

        List<List<Object>> data = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            FieldInterpretation interpretation = getFieldInterpretation(entry.getKey());
            List<Object> datum = new ArrayList<>();
            fields.add(interpretation.name());
            datum.add(entry.getValue());
            data.add(datum);
        }

        Tenant tenant = new Tenant(customerSpace.toString());
        matchInput.setTenant(tenant);
        matchInput.setFields(fields);
        matchInput.setData(data);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.RTS);
        if (StringUtils.isNullOrEmpty(dataCloudVersion)) {
            dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        }
        matchInput.setUseRemoteDnB(enforceFuzzyMatch);
        matchInput.setDataCloudVersion(dataCloudVersion);

        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        return createCompanyProfile(matchOutput);
    }

    private FieldInterpretation getFieldInterpretation(String fieldName) {
        try {
            FieldInterpretation field = Enum.valueOf(FieldInterpretation.class, fieldName);
            return field;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36001, new String[] { fieldName });
        }
    }

    private CompanyProfile createCompanyProfile(MatchOutput matchOutput) {
        CompanyProfile profile = new CompanyProfile();
        List<String> outputFields = matchOutput.getOutputFields();
        List<Object> outputRecords = matchOutput.getResult().get(0).getOutput();
        for (int i = 0; i < outputRecords.size(); i++) {
            profile.getAttributes().put(outputFields.get(i), String.valueOf(outputRecords.get(i)));
        }
        return profile;
    }
}
