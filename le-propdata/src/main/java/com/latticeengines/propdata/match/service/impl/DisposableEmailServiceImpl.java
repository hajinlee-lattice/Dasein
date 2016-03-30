package com.latticeengines.propdata.match.service.impl;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.propdata.match.service.DisposableEmailService;

@Component("disposableEmailServiceImpl")
public class DisposableEmailServiceImpl implements DisposableEmailService {

    private static Set<String> disposableDomains;

    @PostConstruct
    private void postConstruct() {
        loadDomains();
    }

    @Override
    public Boolean isDisposableEmailDomain(String domain) {
        return disposableDomains.contains(domain);
    }

    private void loadDomains() {
        disposableDomains = new HashSet<>();

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/propdata/match/DisposableEmailDomains.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource PublicDomains.txt");
        }
        Scanner scanner = new Scanner(is);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String domain = DomainUtils.parseDomain(line);
            if (StringUtils.isNotEmpty(domain)) {
                disposableDomains.add(domain);
            }
        }
        scanner.close();
    }

}
