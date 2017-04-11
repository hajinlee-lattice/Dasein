package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.CompanyProfileService;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.CompanyProfileRequest;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.mysql.jdbc.StringUtils;

@Component("companyProfileService")
public class CompanyProfileServiceImpl implements CompanyProfileService {
    private static final Log log = LogFactory.getLog(CompanyProfileServiceImpl.class);

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Value("${ulysses.companyprofile.datacloud.version:}")
    private String dataCloudVersion;

    @Override
    public CompanyProfile getProfile(CustomerSpace customerSpace, CompanyProfileRequest request,
            boolean enforceFuzzyMatch) {
        if (!FeatureFlagClient.isEnabled(customerSpace, LatticeFeatureFlag.LATTICE_INSIGHTS.getName())) {
            throw new RuntimeException(String.format("LATTICE_INSIGHTS feature flag is not enabled for customer %s",
                    customerSpace));
        }

        MatchInput matchInput = new MatchInput();

        List<List<Object>> data = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        List<Object> datum = new ArrayList<>();
        for (Map.Entry<String, Object> entry : request.getRecord().entrySet()) {
            FieldInterpretation interpretation = getFieldInterpretation(entry.getKey());
            fields.add(interpretation.name());
            datum.add(entry.getValue());
        }
        data.add(datum);
        Boolean considerInternalAttributes = FeatureFlagClient.isEnabled(customerSpace,
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES.getName());

        considerInternalAttributes = considerInternalAttributes == null //
        ? Boolean.FALSE //
                : considerInternalAttributes;

        Tenant tenant = new Tenant(customerSpace.toString());
        matchInput.setTenant(tenant);
        matchInput.setFields(fields);
        matchInput.setData(data);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.Enrichment);
        if (StringUtils.isNullOrEmpty(dataCloudVersion)) {
            dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        }
        matchInput.setUseRemoteDnB(enforceFuzzyMatch);
        matchInput.setDataCloudVersion(dataCloudVersion);
        matchInput.setLogLevel(Level.DEBUG);

        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        return createCompanyProfile(matchOutput, dataCloudVersion, considerInternalAttributes);
    }

    private FieldInterpretation getFieldInterpretation(String fieldName) {
        try {
            FieldInterpretation field = Enum.valueOf(FieldInterpretation.class, fieldName);
            return field;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_36001, new String[] { fieldName });
        }
    }

    private CompanyProfile createCompanyProfile(MatchOutput matchOutput, String dataCloudVersion,
            Boolean considerInternalAttributes) {
        CompanyProfile profile = new CompanyProfile();

        Map<String, Object> enrichValueMap = new HashMap<String, Object>();
        Map<String, Object> nonNullEnrichValueMap = new HashMap<>();
        Map<String, Object> nonNullInternalEnrichValueMap = new HashMap<>();
        Map<String, Object> firmographicAttrValueMap = new HashMap<>();

        List<String> outputFields = matchOutput.getOutputFields();
        List<Object> outputRecords = matchOutput.getResult().get(0).getOutput();
        for (int i = 0; i < outputRecords.size(); i++) {
            enrichValueMap.put(outputFields.get(i), String.valueOf(outputRecords.get(i)));
        }

        if (!MapUtils.isEmpty(enrichValueMap)) {
            for (String enrichKey : enrichValueMap.keySet()) {
                Object value = enrichValueMap.get(enrichKey);
                if (value != null && !value.equals("null")) {
                    nonNullEnrichValueMap.put(enrichKey, value);
                }
            }

            List<ColumnMetadata> enrichmentColumns = columnMetadataProxy.columnSelection(Predefined.Enrichment,
                    dataCloudVersion);

            List<ColumnMetadata> requiredEnrichmentMetadataList = new ArrayList<>();

            for (ColumnMetadata attr : enrichmentColumns) {
                if (nonNullEnrichValueMap.containsKey(attr.getColumnName())) {
                    requiredEnrichmentMetadataList.add(attr);
                    if (attr.isCanInternalEnrich()) {
                        nonNullInternalEnrichValueMap.put(attr.getColumnName(),
                                nonNullEnrichValueMap.get(attr.getColumnName()));
                    }
                    if (attr.getCategory() == Category.FIRMOGRAPHICS) {
                        firmographicAttrValueMap.put(attr.getColumnName(),
                                nonNullEnrichValueMap.get(attr.getColumnName()));
                    }
                }
            }
            profile.setCompanyInfo(firmographicAttrValueMap);

            if (!considerInternalAttributes) {
                for (String key : nonNullInternalEnrichValueMap.keySet()) {
                    nonNullEnrichValueMap.remove(key);
                }
            }

        }

        profile.setCompanyInfo(firmographicAttrValueMap);
        profile.setAttributes(nonNullEnrichValueMap);
        profile.setTimestamp(matchOutput.getFinishedAt().toString());
        profile.setMatchLogs(matchOutput.getResult().get(0).getMatchLogs());
        profile.setMatchErrorMessages(matchOutput.getResult().get(0).getErrorMessages());

        return profile;
    }
}
