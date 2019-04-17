package com.latticeengines.datacloud.dataflow.transformation.atlas;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_ID_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_NAME_FIELD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.util.Preconditions;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ExtractEmbeddedEntityTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * This transformer is only used in PA match step. If there is an entity
 * embedded in another entity match job (eg. Account embedded in Contact match),
 * we want to extract the newly created embedded entity (eg. Account), setup a
 * table/source with necessary match key fields and merge to the entity's batch
 * store
 *
 * InputSource1: EntityIds table (EntityName, EntityId)
 * eg. In Contact match, we create this table source with EntityName = 'Account'
 * and EntityId = EntityId of all the newly created Accounts in Contact match
 * Prerequisites: EntityId is guaranteed to be unique
 *
 * InputSource2: EmbeddedEntity table
 * eg. Result table of Contact match with AllocateId mode.
 * Contact input/match result fields + AccountId (Account EntityId) + Account input fields
 *
 * Join InputSource1 with InputSource2 by entity's EntityId,
 * then retain necessary match key fields
 */
@Component(ExtractEmbeddedEntityTable.DATAFLOW_BEAN_NAME)
public class ExtractEmbeddedEntityTable extends ConfigurableFlowBase<ExtractEmbeddedEntityTableConfig> {
    public static final String DATAFLOW_BEAN_NAME = "ExtractEmbeddedEntityTableFlow";
    public static final String TRANSFORMER_NAME = "ExtractEmbeddedEntityTable";

    private ExtractEmbeddedEntityTableConfig config;

    // Entity -> Required fields from embedded entity table (Configurable
    // EntityId fields are not included)
    private static final Map<String, List<String>> REQUIRED_FLDS = ImmutableMap.of(
            BusinessEntity.Account.name(),
            Collections.singletonList(InterfaceName.LatticeAccountId.name())
            );

    // Entity -> Optional fields from embedded entity table (Configurable
    // SystemId fields are not included)
    private static final Map<String, List<String>> OPTIONAL_FLDS = ImmutableMap.of(
            BusinessEntity.Account.name(),
            new ArrayList<>(MatchKey.LDC_MATCH_KEY_STD_FLDS.values())
            );

    private static final String ENTITYID_JOIN = "EntityId_Join";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        validateConfig();
        Node entityIds = addSource(parameters.getBaseTables().get(0));
        Node embeddedEntities = addSource(parameters.getBaseTables().get(1));

        entityIds = validatePrepareEntityIdsNode(entityIds);
        embeddedEntities = validatePrepareEmbeddedEntitiesNode(embeddedEntities);

        return entityIds
                .join(new FieldList(ENTITY_ID_FIELD), embeddedEntities, new FieldList(ENTITYID_JOIN), JoinType.INNER)
                .discard(ENTITYID_JOIN);
    }

    private void validateConfig() {
        // Currently only support Account entity
        Preconditions.checkArgument(BusinessEntity.Account.name().equals(config.getEntity()),
                "Currently only support Account entity, but asked for entity " + config.getEntity());
        if (config.getSystemIdFlds() == null) {
            config.setSystemIdFlds(new ArrayList<>());
        }
        config.getSystemIdFlds().forEach(fld -> {
            Preconditions.checkArgument(StringUtils.isNotBlank(fld),
                    "SystemId field in config cannot be null/blank string. If no SystemId available, don't set it.");
        });
        Preconditions.checkArgument(StringUtils.isNotBlank(config.getEntityIdFld()),
                "EntityId field in config cannot be null/blank string");
    }

    /**
     * For Account entity, retain EntityId and add AccountId with same value as
     * EntityId
     *
     * @param entityIds
     * @return
     */
    private Node validatePrepareEntityIdsNode(Node entityIds) {
        Preconditions.checkNotNull(entityIds.getSchema(ENTITY_ID_FIELD),
                "Input source of EntityIds doesn't have EntityId field");
        entityIds = entityIds.retain(ENTITY_ID_FIELD);
        if (BusinessEntity.Account.name().equals(config.getEntity())) {
            entityIds = entityIds.apply(ENTITY_ID_FIELD, //
                    new FieldList(entityIds.getFieldNames()), //
                    new FieldMetadata(InterfaceName.AccountId.name(), String.class));
        }
        if (config.isFilterByEntity()) {
            entityIds = entityIds.filter(
                    String.format("\"%s\".equals(%s)", config.getEntity(), ENTITY_NAME_FIELD),
                    new FieldList(ENTITY_NAME_FIELD));
        }
        return entityIds;
    }

    /**
     * Retain all required fields + existed optional fields
     *
     * @param embeddedEntities
     * @return
     */
    private Node validatePrepareEmbeddedEntitiesNode(Node embeddedEntities) {
        List<String> requiredFlds = REQUIRED_FLDS.get(config.getEntity());
        List<String> optionalFlds = OPTIONAL_FLDS.get(config.getEntity());
        // Validation
        final Node origin = embeddedEntities;
        requiredFlds.forEach(field -> {
            Preconditions.checkNotNull(origin.getSchema(field), String.format(
                    "Required field %s doesn't exist in input source %s", field, origin.getPipeName()));
        });
        Preconditions.checkNotNull(origin.getSchema(config.getEntityIdFld()), String.format(
                "EntityId field %s doesn't exist in input source %s", config.getEntityIdFld(),
                origin.getPipeName()));

        // Retain required fields + existed optional fields
        List<String> toRetainFlds = new ArrayList<>(requiredFlds);
        toRetainFlds.addAll(optionalFlds.stream().filter(field -> origin.getSchema(field) != null)
                .collect(Collectors.toList()));
        if (CollectionUtils.isNotEmpty(config.getSystemIdFlds())) {
            toRetainFlds.addAll(config.getSystemIdFlds().stream()
                    .filter(field -> origin.getSchema(field) != null).collect(Collectors.toList()));
        }
        toRetainFlds.add(config.getEntityIdFld());
        embeddedEntities = embeddedEntities.retain(new FieldList(toRetainFlds));

        embeddedEntities = embeddedEntities.rename(new FieldList(config.getEntityIdFld()),
                new FieldList(ENTITYID_JOIN));
        // To solve column mis-alignment issue after rename operation
        return embeddedEntities.retain(new FieldList(embeddedEntities.getFieldNames()));
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
        return ExtractEmbeddedEntityTableConfig.class;
    }
}
