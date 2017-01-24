package com.latticeengines.app.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;

public interface CompanyProfileService {

    CompanyProfile getProfile(CustomerSpace customerSpace, Map<String, String> fields, boolean enforceFuzzyMatch);
}
