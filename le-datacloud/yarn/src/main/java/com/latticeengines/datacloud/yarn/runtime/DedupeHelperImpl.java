package com.latticeengines.datacloud.yarn.runtime;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

@Component("dedupeHelper")
public class DedupeHelperImpl implements DedupeHelper {

    private static final Logger log = LoggerFactory.getLogger(DedupeHelperImpl.class);

    private static Pattern junkNamePattern = Pattern
            .compile("(^|\\s+)[\\[]*(none|no|not|delete|asd|sdf|unknown|undisclosed|null|dont|don't|n\\/a|n\\.a|abc|xyz|noname|nocompany)($|\\s+)");

    @Autowired
    PublicDomainService publicDomainService;

    @Override
    public void appendDedupeValues(ProcessorContext processorContext, List<Object> allValues, OutputRecord outputRecord) {

        String latticeAccountId = outputRecord.getMatchedLatticeAccountId();
        allValues.add(latticeAccountId);

        String dedupeId = null;
        Integer isRemoved = 0;
        String domain = outputRecord.getPreMatchDomain();

        /*** matched ***/
        boolean isMatched = StringUtils.isNotEmpty(latticeAccountId);
        if (isMatched) {
            if (StringUtils.isNotEmpty(outputRecord.getMatchedDduns())) {
                dedupeId = outputRecord.getMatchedDduns();
            } else if (StringUtils.isNotEmpty(outputRecord.getMatchedDuns())) {
                dedupeId = outputRecord.getMatchedDuns();
            } else {
                dedupeId = domain;
            }
            log.debug("Matched, domain=" + domain);
            addDedupeValues(allValues, dedupeId, isRemoved);
            return;
        }

        /*** un-matched ***/
        boolean isPublicDomain = !processorContext.getOriginalInput().isPublicDomainAsNormalDomain()
                && publicDomainService.isPublicDomain(domain);
        int numFeatureValue = outputRecord.getNumFeatureValue();
        NameLocation nameLocation = outputRecord.getPreMatchNameLocation();
        String name = nameLocation != null ? nameLocation.getName() : null;
        String country = nameLocation != null ? nameLocation.getCountry() : null;
        boolean hasNoNameLocation = StringUtils.isEmpty(name) && StringUtils.isEmpty(country);

        log.debug("No matched, domain=" + domain + " name=" + name + " country=" + country + ", has nameloation="
                + hasNoNameLocation + " is public=" + isPublicDomain + " Feature num=" + numFeatureValue
                + " name location=" + nameLocation);

        // public domain
        if (isPublicDomain || StringUtils.isEmpty(domain)) {
            if (hasNoNameLocation || isJunkCompany(name)) {
                isRemoved = 1;
                addDedupeValues(allValues, dedupeId, isRemoved);
                return;
            } else if (numFeatureValue > 0) {
                if (StringUtils.isEmpty(name)) {
                    name = "";
                }
                if (StringUtils.isEmpty(country)) {
                    country = "USA";
                }
                dedupeId = Base64Utils.encodeBase64(DigestUtils.md5(name + country));
                addDedupeValues(allValues, dedupeId, isRemoved);
                return;
            }
        }

        // non-public domain
        if (!isPublicDomain && StringUtils.isNotEmpty(domain) && numFeatureValue > 0) {
            if (StringUtils.isEmpty(domain) && StringUtils.isNotEmpty(name)) {
                if (StringUtils.isEmpty(country)) {
                    country = "USA";
                }
            } else {
                if (StringUtils.isEmpty(country)) {
                    country = "";
                }
            }
            dedupeId = Base64Utils.encodeBase64(DigestUtils.md5(domain + country));
            addDedupeValues(allValues, dedupeId, isRemoved);
            return;
        }
        addDedupeValues(allValues, dedupeId, isRemoved);
    }

    private boolean isJunkCompany(String name) {
        if (StringUtils.isNotEmpty(name)) {
            name = name.toLowerCase();
            Matcher matcher = junkNamePattern.matcher(name);
            return matcher.matches();
        }
        return false;
    }

    private void addDedupeValues(List<Object> allValues, String dedupeId, Integer isRemoved) {
        allValues.add(dedupeId);
        allValues.add(isRemoved);
    }

}
