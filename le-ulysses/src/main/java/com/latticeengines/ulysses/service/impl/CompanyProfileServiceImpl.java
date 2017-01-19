package com.latticeengines.ulysses.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.ulysses.service.CompanyProfileService;

@Component("companyProfileService")
public class CompanyProfileServiceImpl implements CompanyProfileService {

    @Autowired
    private MatchProxy matchProxy;

    @Value("${ulysses.companyprofile.datacloud.version:}")
    private String dataCloudVersion;

    @Override
    public CompanyProfile getProfile(CustomerSpace customerSpace, Map<FieldInterpretation, String> values) {
        MatchInput matchInput = new MatchInput();

        List<List<Object>> data = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        for (Map.Entry<FieldInterpretation, String> entry : values.entrySet()) {
            List<Object> datum = new ArrayList<>();
            fields.add(entry.getKey().name());
            datum.add(entry.getValue());
            data.add(datum);
        }

        Tenant tenant = new Tenant(customerSpace.toString());
        matchInput.setTenant(tenant);
        matchInput.setFields(fields);
        matchInput.setData(data);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.RTS);
        matchInput.setDataCloudVersion(dataCloudVersion);

        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        return createCompanyProfile(matchOutput);
    }

    private CompanyProfile createCompanyProfile(MatchOutput matchOutput) {
        CompanyProfile profile = new CompanyProfile();
        List<String> outputFields = matchOutput.getOutputFields();
        List<Object> outputRecords = matchOutput.getResult().get(0).getOutput();
        for (int i = 0; i < outputRecords.size(); i++) {
            profile.attributes.put(outputFields.get(i), String.valueOf(outputRecords.get(i)));
        }
        return profile;
    }
}
