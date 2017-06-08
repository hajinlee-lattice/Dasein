package com.latticeengines.datacloud.yarn.runtime;

import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

@Component("dedupeHelper")
public class DedupeHelperImpl implements DedupeHelper {

    private static final Log log = LogFactory.getLog(DedupeHelperImpl.class);

    @Autowired
    PublicDomainService publicDomainService;

    @Override
    public void appendDedupeValues(ProcessorContext processorContext, List<Object> allValues, OutputRecord outputRecord) {

        String latticeAccountId = outputRecord.getMatchedLatticeAccountId();
        allValues.add(latticeAccountId);

        String dedupeId = null;
        Integer isRemoved = 0;
        String domain = outputRecord.getPreMatchDomain();

        // matched
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

        // un-matched
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

        // removed
        if (isPublicDomain && hasNoNameLocation) {
            isRemoved = 1;
            addDedupeValues(allValues, dedupeId, isRemoved);
            return;
        }

        // non-public domain
        if (!isPublicDomain && numFeatureValue > 0) {
            if (StringUtils.isEmpty(domain) && StringUtils.isNotEmpty(name)) {
                if (StringUtils.isEmpty(country)) {
                    country = "USA";
                }
                dedupeId = Base64Utils.encodeBase64(DigestUtils.md5(name + country));
            }
            if (StringUtils.isNotEmpty(domain) && StringUtils.isEmpty(name)) {
                if (StringUtils.isEmpty(country)) {
                    country = "";
                }
                dedupeId = Base64Utils.encodeBase64(DigestUtils.md5(domain + country));
            }
            if (StringUtils.isNotEmpty(domain) && StringUtils.isNotEmpty(name)) {
                if (StringUtils.isEmpty(country)) {
                    country = "";
                }
                dedupeId = Base64Utils.encodeBase64(DigestUtils.md5(domain + name + country));
            }
            addDedupeValues(allValues, dedupeId, isRemoved);
            return;
        }

        // public domain
        if (isPublicDomain && numFeatureValue > 0 && StringUtils.isNotEmpty(name)) {
            if (StringUtils.isEmpty(country)) {
                country = "USA";
            }
            dedupeId = Base64Utils.encodeBase64(DigestUtils.md5(name + country));
        }
        addDedupeValues(allValues, dedupeId, isRemoved);
    }

    private void addDedupeValues(List<Object> allValues, String dedupeId, Integer isRemoved) {
        allValues.add(dedupeId);
        allValues.add(isRemoved);
    }

}
