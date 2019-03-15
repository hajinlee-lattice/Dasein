package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.match.service.MatchStandardizationService;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.metadata.InterfaceName;


@Component("matchStandardizationService")
public class MatchStandardizationServiceImpl implements MatchStandardizationService {

    private static Logger log = LoggerFactory.getLogger(MatchStandardizationServiceImpl.class);

    @Inject
    private PublicDomainService publicDomainService;

    @Inject
    private NameLocationService nameLocationService;

    // Customer standard attr name -> Internal standard attr name
    // Current use case is to standardize SystemId fields:
    // AccountId from customer is named as CustomerAccountId in file import (In
    // future, might extend this naming convention to other match key fields too
    // to avoid naming conflicts). But SystemId field names managed in
    // seed/lookup should use standard name, eg. AccountId. So need to do
    // standardization for SystemId field names too.
    private static final Map<String, String> STANDARD_ATTR_DICT = new HashMap<>();
    static {
        STANDARD_ATTR_DICT.put(InterfaceName.CustomerAccountId.name().toLowerCase(), InterfaceName.AccountId.name());
    }


    @Override
    public void parseRecordForDomain(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                     Set<String> domainSet, boolean treatPublicDomainAsNormal,
                                     EntityMatchKeyRecord record) {
        if (keyPositionMap.containsKey(MatchKey.Domain)) {
            boolean relaxPublicDomainCheck = isPublicDomainCheckRelaxed(record.getParsedNameLocation().getName(),
                    record.getParsedDuns());
            List<Integer> domainPosList = keyPositionMap.get(MatchKey.Domain);
            try {
                // Iterate through all positions matching fields in the list of Domain MatchKeys finding the first
                // valid non-public domain to use as the parsed domain.
                String cleanDomain = null;
                boolean foundPublicDomain = false;
                for (Integer domainPos : domainPosList) {
                    String originalDomain = (String) inputRecord.get(domainPos);
                    record.setOrigDomain(originalDomain);
                    cleanDomain = DomainUtils.parseDomain(originalDomain);
                    if (StringUtils.isEmpty(cleanDomain)) {
                        continue;
                    }
                    if (publicDomainService.isPublicDomain(cleanDomain)) {
                        // For match input with domain, but without name and duns,
                        // and domain is not in email format, public domain is
                        // treated as normal domain
                        if (treatPublicDomainAsNormal
                                || (relaxPublicDomainCheck && !DomainUtils.isEmail(record.getOrigDomain()))) {
                            record.setMatchEvenIsPublicDomain(true);
                            record.setPublicDomain(true);
                            record.addErrorMessages("Parsed to a public domain: " + cleanDomain
                                    + ", but treat it as normal domain in match");
                            record.setParsedDomain(cleanDomain);
                            if (domainSet != null) {
                                domainSet.add(cleanDomain);
                            }
                            break;
                        } else {
                            record.addErrorMessages("Found a public domain: " + cleanDomain);
                            // public domain is not used for match
                            cleanDomain = null;
                            foundPublicDomain = true;
                        }
                    } else if (StringUtils.isNotEmpty(cleanDomain)) {
                        record.setPublicDomain(false);
                        record.setParsedDomain(cleanDomain);
                        if (domainSet != null) {
                            domainSet.add(cleanDomain);
                        }
                        break;
                    }
                    if (StringUtils.isEmpty(cleanDomain)) {
                        record.setParsedDomain(null);
                        record.addErrorMessages("Did not find a valid non-public domain");
                        if (foundPublicDomain) {
                            record.setPublicDomain(true);
                        }
                    }
                }
                // TODO(dzheng): Move catch inside loop so that we can skip unparsable domains rather than error out?
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup domain field: " + e.getMessage());
            }
        }
    }

    // No need to check ZK setting as ldc match will remove zk check too
    private boolean isPublicDomainCheckRelaxed(String name, String duns) {
        return StringUtils.isBlank(name) && StringUtils.isBlank(duns);
    }

    @Override
    public void parseRecordForNameLocation(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                           Set<NameLocation> nameLocationSet, EntityMatchKeyRecord record) {
        try {
            String originalName = null;
            if (keyPositionMap.containsKey(MatchKey.Name)) {
                List<Integer> namePosList = keyPositionMap.get(MatchKey.Name);
                for (Integer namePos : namePosList) {
                    originalName = (String) inputRecord.get(namePos);
                }
            }
            String originalCountry = null;
            if (keyPositionMap.containsKey(MatchKey.Country)) {
                List<Integer> countryPosList = keyPositionMap.get(MatchKey.Country);
                for (Integer countryPos : countryPosList) {
                    originalCountry = (String) inputRecord.get(countryPos);
                }
            }
            String originalState = null;
            if (keyPositionMap.containsKey(MatchKey.State)) {
                List<Integer> statePosList = keyPositionMap.get(MatchKey.State);
                for (Integer statePos : statePosList) {
                    originalState = (String) inputRecord.get(statePos);
                }
            }
            String originalCity = null;
            if (keyPositionMap.containsKey(MatchKey.City)) {
                for (Integer cityPos : keyPositionMap.get(MatchKey.City)) {
                    originalCity = (String) inputRecord.get(cityPos);
                }
            }
            String originalZipCode = null;
            if (keyPositionMap.containsKey(MatchKey.Zipcode)) {
                for (Integer zipPos : keyPositionMap.get(MatchKey.Zipcode)) {
                    if (inputRecord.get(zipPos) != null) {
                        if (inputRecord.get(zipPos) instanceof String) {
                            originalZipCode = (String) inputRecord.get(zipPos);
                        } else if (inputRecord.get(zipPos) instanceof Utf8 || inputRecord.get(zipPos) instanceof Long
                                || inputRecord.get(zipPos) instanceof Integer) {
                            originalZipCode = inputRecord.get(zipPos).toString();
                        }
                    }
                }
            }
            String originalPhoneNumber = null;
            if (keyPositionMap.containsKey(MatchKey.PhoneNumber)) {
                for (Integer phonePos : keyPositionMap.get(MatchKey.PhoneNumber)) {
                    if (inputRecord.get(phonePos) != null) {
                        if (inputRecord.get(phonePos) instanceof String) {
                            originalPhoneNumber = (String) inputRecord.get(phonePos);
                        } else if (inputRecord.get(phonePos) instanceof Utf8 || inputRecord.get(phonePos) instanceof Long
                                || inputRecord.get(phonePos) instanceof Integer) {
                            originalPhoneNumber = inputRecord.get(phonePos).toString();
                        }
                    }
                }
            }

            NameLocation origNameLocation = getNameLocation(originalName, originalCountry, originalState, originalCity,
                    originalZipCode, originalPhoneNumber);
            record.setOrigNameLocation(origNameLocation);

            NameLocation nameLocation = getNameLocation(originalName, originalCountry, originalState, originalCity,
                    originalZipCode, originalPhoneNumber);
            nameLocationService.normalize(nameLocation);
            record.setParsedNameLocation(nameLocation);
            if (nameLocationSet != null && isValidNameLocation(nameLocation)) {
                nameLocationSet.add(nameLocation);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            record.setFailed(true);
            record.addErrorMessages("Error when cleanup name and location fields: " + e.getMessage());
        }
    }

    private static boolean isValidNameLocation(NameLocation nameLocation) {
        return (StringUtils.isNotBlank(nameLocation.getName()) || StringUtils.isNotBlank(nameLocation.getPhoneNumber()))
                && StringUtils.isNotBlank(nameLocation.getCountryCode());
    }

    private NameLocation getNameLocation(String originalName, String originalCountry, String originalState,
                                         String originalCity, String originalZipCode, String originalPhoneNumber) {
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName(originalName);
        nameLocation.setState(originalState);
        nameLocation.setCountry(originalCountry);
        nameLocation.setCity(originalCity);
        nameLocation.setZipcode(originalZipCode);
        nameLocation.setPhoneNumber(originalPhoneNumber);
        return nameLocation;
    }

    @Override
    public void parseRecordForDuns(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                   EntityMatchKeyRecord record) {
        if (keyPositionMap.containsKey(MatchKey.DUNS)) {
            List<Integer> dunsPosList = keyPositionMap.get(MatchKey.DUNS);
            try {
                String cleanDuns = null;
                for (Integer dunsPos : dunsPosList) {
                    String originalDuns = inputRecord.get(dunsPos) == null ? null
                            : String.valueOf(inputRecord.get(dunsPos));
                    record.setOrigDuns(originalDuns);
                    if (StringUtils.isNotEmpty(originalDuns)) {
                        cleanDuns = StringStandardizationUtils.getStandardDuns(originalDuns);
                        break;
                    }
                }
                record.setParsedDuns(cleanDuns);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup duns field: " + e.getMessage());
            }
        }
    }

    @Override
    public void parseRecordForSystemIds(List<Object> inputRecord, Map<MatchKey, List<String>> keyMap,
            Map<MatchKey, List<Integer>> keyPositionMap, MatchKeyTuple matchKeyTuple, EntityMatchKeyRecord record) {
        List<Pair<String, String>> systemIds = new ArrayList<>();
        if (keyMap.containsKey(MatchKey.SystemId) && keyPositionMap.containsKey(MatchKey.SystemId)) {
            List<String> systemIdNames = keyMap.get(MatchKey.SystemId);
            List<Integer> systemIdPositions = keyPositionMap.get(MatchKey.SystemId);

            for (int i = 0; i < systemIdNames.size(); i++) {
                String cleanSystemIdName = getStandardizedAttrName(systemIdNames.get(i));
                String cleanSystemId = null;
                Integer systemIdPos = systemIdPositions.get(i);
                if (inputRecord.get(systemIdPos) != null) {
                    String systemId = null;
                    if (inputRecord.get(systemIdPos) instanceof String) {
                        systemId = (String) inputRecord.get(systemIdPos);
                    } else if (inputRecord.get(systemIdPos) instanceof Utf8
                            || inputRecord.get(systemIdPos) instanceof Long
                            || inputRecord.get(systemIdPos) instanceof Integer) {
                        systemId = inputRecord.get(systemIdPos).toString();
                    }
                    // TODO(jwinter): Complete work to clean up System IDs.
                    cleanSystemId = StringStandardizationUtils.getStandardizedSystemId(systemId);
                    record.addOrigSystemId(cleanSystemIdName, systemId);
                    record.addParsedSystemId(cleanSystemIdName, cleanSystemId);
                }
                systemIds.add(Pair.of(cleanSystemIdName, cleanSystemId));
            }
        }
        matchKeyTuple.setSystemIds(systemIds);
    }

    // TODO: If predefined dictionary doesn't have the attr, just do trim(),
    // don't standardize attr name case for now. Not very sure about use case
    // yet. Need to revisit in the future
    private String getStandardizedAttrName(@NotNull String attrName) {
        Preconditions.checkNotNull(attrName);
        attrName = attrName.trim();
        if (STANDARD_ATTR_DICT.containsKey(attrName.toLowerCase())) {
            return STANDARD_ATTR_DICT.get(attrName.toLowerCase());
        }
        return attrName;
    }

    // TODO(jwinter): The two methods below are not used right now but I'm not deleting them in case they are needed
    //     later.
    /*
    public void parseRecordForLatticeAccountId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                               EntityMatchKeyRecord record) {
        if (keyPositionMap.containsKey(MatchKey.LatticeAccountID)) {
            List<Integer> idPosList = keyPositionMap.get(MatchKey.LatticeAccountID);
            try {
                String cleanId = null;
                for (Integer idPos : idPosList) {
                    String originalId = inputRecord.get(idPos) == null ? null : String.valueOf(inputRecord.get(idPos));
                    if (StringUtils.isNotEmpty(originalId)) {
                        cleanId = StringStandardizationUtils.getStandardizedInputLatticeID(originalId);
                        break;
                    }
                }
                record.setLatticeAccountId(cleanId);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup lattice account id field: " + e.getMessage());
            }
        }
    }

    public void parseRecordForLookupId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                       EntityMatchKeyRecord record) {
        if (keyPositionMap.containsKey(MatchKey.LookupId)) {
            List<Integer> idPosList = keyPositionMap.get(MatchKey.LookupId);
            Integer idPos = idPosList.get(0);
            try {
                String lookupId = inputRecord.get(idPos) == null ? null : String.valueOf(inputRecord.get(idPos));
                record.setLookupIdValue(lookupId);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup lookup id field: " + e.getMessage());
            }
        }
    }
    */
}
