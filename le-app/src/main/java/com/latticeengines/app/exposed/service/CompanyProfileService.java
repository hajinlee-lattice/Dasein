package com.latticeengines.app.exposed.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.CompanyProfileRequest;

public interface CompanyProfileService {

    CompanyProfile getProfile(CustomerSpace customerSpace, CompanyProfileRequest request, boolean enforceFuzzyMatch);
}
