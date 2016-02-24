package com.latticeengines.propdata.match.service.impl;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.propdata.match.service.PublicDomainService;

@Component
public class PublicDomainServiceImpl implements PublicDomainService {

    private static Set<String> publicDomains;

    @PostConstruct
    private void postConstruct() {
        loadPublicDomains();
    }

    @Override
    public Boolean isPublicDomain(String domain) {
        return publicDomains.contains(domain);
    }

    private void loadPublicDomains() {
        publicDomains = new HashSet<>();

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/propdata/match/PublicDomains.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource PublicDomains.txt");
        }
        Scanner scanner = new Scanner(is);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String domain = DomainUtils.parseDomain(line);
            if (StringUtils.isNotEmpty(domain)) {
                publicDomains.add(domain);
            }
        }
        scanner.close();
    }

}
