package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.OrbCacheSeedRebuildBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedRebuildConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("orbCacheSeedRebuildFlow")
public class OrbCacheSeedRebuildFlow extends ConfigurableFlowBase<OrbCacheSeedRebuildConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        OrbCacheSeedRebuildConfig config = getTransformerConfig(parameters);
        Node orbCompany = addSource(parameters.getBaseTables().get(0));
        Node orbDomain = addSource(parameters.getBaseTables().get(1));
        
        orbCompany = removeBranches(orbCompany, config);
        orbDomain = renameOrbDomain(orbDomain);
        Node orbCacheSeed = generateOrbCacheSeed(orbCompany, orbDomain, config);
        return orbCacheSeed;
    }

    private Node removeBranches(Node source, OrbCacheSeedRebuildConfig config) {
        source = source.filter(
                String.format("%s == null || !(%s.equalsIgnoreCase(\"branch\"))",
                config.getCompanyFileEntityTypeField(), config.getCompanyFileEntityTypeField()),
                new FieldList(config.getCompanyFileEntityTypeField()));
        return source;
    }

    private Node renameOrbDomain(Node source) {
        List<String> fieldNames = source.getFieldNames();
        for (String fieldName : fieldNames) {
            source = source.rename(new FieldList(fieldName), new FieldList(renameColumnForOrbDomain(fieldName)));
        }
        return source;
    }

    private Node generateOrbCacheSeed(Node orbCompany, Node orbDomain, OrbCacheSeedRebuildConfig config) {
        Node orbCacheSeedJoined = orbCompany.join(new FieldList(config.getCompanyFileOrbNumField()), orbDomain,
                new FieldList(renameColumnForOrbDomain(config.getDomainFileOrbNumField())), JoinType.LEFT);
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.addAll(orbCacheSeedJoined.getFieldNames());
        fieldNames.add(config.getOrbCacheSeedDomainField());
        fieldNames.add(config.getOrbCacheSeedPrimaryDomainField());
        fieldNames.add(config.getOrbCacheSeedIsSecondaryDomainField());
        fieldNames.add(config.getOrbCacheSeedDomainHasEmailField());
        List<FieldMetadata> fieldMetadatas = new ArrayList<FieldMetadata>();
        fieldMetadatas.addAll(orbCacheSeedJoined.getSchema());
        fieldMetadatas.add(new FieldMetadata(config.getOrbCacheSeedDomainField(), String.class));
        fieldMetadatas.add(new FieldMetadata(config.getOrbCacheSeedPrimaryDomainField(), String.class));
        fieldMetadatas.add(new FieldMetadata(config.getOrbCacheSeedIsSecondaryDomainField(), Boolean.class));
        fieldMetadatas.add(new FieldMetadata(config.getOrbCacheSeedDomainHasEmailField(), Boolean.class));
        Node orbCacheSeedGroup = orbCacheSeedJoined.groupByAndBuffer(new FieldList(config.getCompanyFileOrbNumField()),
                new OrbCacheSeedRebuildBuffer(new Fields(fieldNames.toArray(new String[fieldNames.size()])),
                        config.getCompanyFileDomainField(), config.getCompanyFileWebDomainsField(),
                        renameColumnForOrbDomain(config.getDomainFileDomainField()),
                        renameColumnForOrbDomain(config.getDomainFileHasEmailField()),
                        config.getOrbCacheSeedDomainField(), config.getOrbCacheSeedPrimaryDomainField(),
                        config.getOrbCacheSeedIsSecondaryDomainField(), config.getOrbCacheSeedDomainHasEmailField()),
                fieldMetadatas);
        orbCacheSeedGroup = orbCacheSeedGroup.retain(new FieldList(config.getCompanyFileOrbNumField(),
                config.getOrbCacheSeedDomainField(), config.getOrbCacheSeedPrimaryDomainField(),
                config.getOrbCacheSeedIsSecondaryDomainField(), config.getOrbCacheSeedDomainHasEmailField()));
        orbCacheSeedGroup = orbCacheSeedGroup.renamePipe("OrbCacheSeedGroup");
        Node orbCacheSeed = orbCompany.join(new FieldList(config.getCompanyFileOrbNumField()), orbCacheSeedGroup,
                new FieldList(config.getCompanyFileOrbNumField()), JoinType.LEFT);
        orbCacheSeed = orbCacheSeed.discard(new FieldList(config.getCompanyFileDomainField(),
                "OrbCacheSeedGroup__" + config.getCompanyFileOrbNumField()));
        return orbCacheSeed;
    }
    
    private String renameColumnForOrbDomain(String fieldName) {
        return "OrbDomain_" + fieldName;
    }

    @Override
    public String getDataFlowBeanName() {
        return "orbCacheSeedRebuildFlow";
    }

    @Override
    public String getTransformerName() {
        return "orbCacheSeedRebuildTransformer";
    }

    @Override
    public Class<? extends OrbCacheSeedRebuildConfig> getTransformerConfigClass() {
        return OrbCacheSeedRebuildConfig.class;
    }


}
