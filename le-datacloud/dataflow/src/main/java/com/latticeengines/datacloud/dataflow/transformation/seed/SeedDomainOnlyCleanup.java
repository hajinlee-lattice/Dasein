package com.latticeengines.datacloud.dataflow.transformation.seed;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.seed.SeedDomainOnlyCleanupConfig;

@Component(SeedDomainOnlyCleanup.DATAFLOW_BEAN_NAME)
public class SeedDomainOnlyCleanup extends ConfigurableFlowBase<SeedDomainOnlyCleanupConfig> {

    public static final String DATAFLOW_BEAN_NAME = "SeedDomainOnlyCleanupFlow";
    public static final String TRANSFORMER_NAME = "SeedDomainOnlyCleanup";

    private SeedDomainOnlyCleanupConfig config;

    private static final String TEMP_DOMAIN = "__DomainFromSource__";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        List<String> baseSrcs = parameters.getBaseTables();
        validateConfig(baseSrcs);

        // 0th base source is seed, other base sources are domain based sources
        Node seed = addSource(baseSrcs.get(0));
        List<String> seedFields = new ArrayList<>(seed.getFieldNames());
        // [pair<domain source node, domain field>]
        List<Pair<Node, String>> domainSrcs = IntStream.range(1, baseSrcs.size()).boxed()
                .map(i -> Pair.of(addSource(baseSrcs.get(i)), config.getSrcDomains().get(baseSrcs.get(i))))
                .collect(Collectors.toList());

        Node seedWithDuns = seed.filter(config.getSeedDuns() + "!=null", new FieldList(config.getSeedDuns()));
        Node seedDomainOnly = seed.filter(config.getSeedDuns() + "==null", new FieldList(config.getSeedDuns()));

        Node domains = convergeDomains(domainSrcs);
        seedDomainOnly = seedDomainOnly
                .join(new FieldList(config.getSeedDomain()), domains, new FieldList(TEMP_DOMAIN), JoinType.LEFT) //
                .filter(TEMP_DOMAIN + "!= null", new FieldList(TEMP_DOMAIN)) //
                .retain(new FieldList(seedFields));

        return seedWithDuns.merge(seedDomainOnly);
    }

    private void validateConfig(List<String> baseSrcs) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(baseSrcs), "Base sources are empty");
        Preconditions
                .checkArgument(baseSrcs.stream().distinct().collect(Collectors.toList()).size() == baseSrcs.size(),
                        "Has duplicate base source");
        Preconditions.checkArgument(MapUtils.isNotEmpty(config.getSrcDomains()), "Source domain fields are missing");
        Preconditions.checkArgument(StringUtils.isNotBlank(config.getSeedDomain()), "Seed domain field is missing");
        Preconditions.checkArgument(StringUtils.isNotBlank(config.getSeedDuns()), "Seed duns field is missing");
        // 0th base source is seed, other base sources are domain based sources
        Preconditions.checkArgument(baseSrcs.size() == config.getSrcDomains().size() + 1,
                "Base sources should contain one seed and all the domain based sources to join");
        IntStream.range(1, baseSrcs.size()).forEach(i -> {
            Preconditions.checkArgument(StringUtils.isNotBlank(config.getSrcDomains().get(baseSrcs.get(i))),
                    String.format("Domain field of source %s is blank", baseSrcs.get(i)));
        });
    }

    /**
     * Solution 1: Merge all domains from domain sources then dedup
     * 
     * Solution 2: Outer join all domain sources then converge domains
     * 
     * Not sure which way is faster. Since each domain sources is not large
     * (2-15M), try with solution 1 and monitor performance first
     * 
     * @param domainSrcs
     * @return Node with only 1 attr: TEMP_DOMAIN
     */
    private Node convergeDomains(List<Pair<Node, String>> domainSrcs) {
        List<Node> domains = domainSrcs.stream()
                .map(pair -> pair.getLeft().retain(pair.getRight()).rename(new FieldList(pair.getRight()),
                        new FieldList(TEMP_DOMAIN))) //
                .collect(Collectors.toList());
        Node toMerge = domains.remove(0);
        if (CollectionUtils.isEmpty(domains)) {
            // Although domain based source is guaranteed to be unique in
            // domain, just for safeguard
            return toMerge.groupByAndLimit(new FieldList(TEMP_DOMAIN), 1);
        } else {
            return toMerge.merge(domains) //
                    .groupByAndLimit(new FieldList(TEMP_DOMAIN), 1);
        }
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SeedDomainOnlyCleanupConfig.class;
    }
}
