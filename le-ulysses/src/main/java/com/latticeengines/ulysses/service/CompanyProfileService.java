package com.latticeengines.ulysses.service;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;

public interface CompanyProfileService {

    CompanyProfile getProfile(CustomerSpace customerSpace, Map<FieldInterpretation, String> fields);
}
