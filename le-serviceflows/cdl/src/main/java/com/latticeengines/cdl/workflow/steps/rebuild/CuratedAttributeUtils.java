package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLCreatedTemplate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLCreatedTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CDLUpdatedTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityCreatedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityCreatedSource;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityCreatedType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityLastUpdatedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfContacts;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.spark.exposed.job.cdl.GenerateCuratedAttributes;

public final class CuratedAttributeUtils {

    private static final Logger log = LoggerFactory.getLogger(CuratedAttributeUtils.class);

    public static final String NUMBER_OF_CONTACTS_DISPLAY_NAME = "Number of Contacts";
    private static final String LAST_ACTIVITY_DATE_DISPLAY_NAME = "Lattice - Last Activity Date";
    private static final String ENTITY_CREATED_DATE_DISPLAY_NAME = "Lattice - Created Date";
    private static final String ENTITY_MODIFIED_DATE_DISPLAY_NAME = "Lattice - Last Modified Date";
    private static final String ENTITY_SYS_MODIFIED_DATE_NAME_FMT = "Lattice - Last Modified Date by %s";
    private static final String ENTITY_CREATED_SOURCE_DISPLAY_NAME = "Lattice - Created Source";
    private static final String ENTITY_CREATED_ENTITY_TYPE_DISPLAY_NAME = "Lattice - Created Entity Type";

    private static final String NUMBER_OF_CONTACTS_DESCRIPTION = "This curated attribute is calculated by counting the "
            + "number of contacts matching each account";
    private static final String LAST_ACTIVITY_DATE_DESCRIPTION = "Most recent activity date among "
            + "any of the time series activity data(excluding transactions)";
    private static final String ENTITY_CREATED_DATE_DESC_FMT = "The date when the %s is created";
    private static final String ENTITY_MODIFIED_DATE_DESC_FMT = "Most recent date when the %s is updated";
    private static final String ENTITY_SYS_MODIFIED_DATE_DESC_FMT = "Most recent date when the %s is updated by %s";
    private static final String ENTITY_CREATED_SOURCE_DESC_FMT = "System that created this %s";
    private static final String ENTITY_CREATED_ENTITY_TYPE_DESC_FMT = "Entity type of the template that created this %s";

    // <TEMPLATE_NAME>__<ATTR>
    private static final String SYSTEM_ATTR_FORMAT = "%s__%s";
    // <ENTITY>__<ATTR>
    private static final String ENTITY_ATTR_PREFIX_FORMAT = "%s__";

    protected CuratedAttributeUtils() {
        throw new UnsupportedOperationException();
    }

    /*-
     * src attr name in batch store -> dst attr name in curated attribute table
     */
    public static Map<String, String> attrsMergeFromMasterStore(@NotNull String entity) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CDLCreatedTime.name(), formatEntityAttribute(entity, EntityCreatedDate.name()));
        attrs.put(CDLUpdatedTime.name(), formatEntityAttribute(entity, EntityLastUpdatedDate.name()));
        attrs.put(CDLCreatedTemplate.name(), String.format(ENTITY_ATTR_PREFIX_FORMAT, entity));
        return attrs;
    }

    /**
     * Configure {@link GenerateCuratedAttributes} to copy attributes from existing
     * curated attribute table
     *
     * @param config
     *            target job config to configure
     * @param entity
     *            curated entity
     * @param templates
     *            import template names for this entity
     * @param inputIdx
     *            master store's location in input
     * @param currAttrs
     *            all attributes exist in current curate attribute table (that are
     *            not being recalculated)
     */
    public static void copySystemLastUpdateTimeAttrs(@NotNull GenerateCuratedAttributesConfig config,
            @NotNull String entity, @NotNull List<String> templates, int inputIdx, @NotNull Set<String> currAttrs) {
        Map<String, String> lastSystemUpdateAttrs = systemLastUpdateTimeAttrs(entity, templates);
        log.info("Last updated date attributes from systems = {}", lastSystemUpdateAttrs);
        config.attrsToMerge.put(inputIdx, lastSystemUpdateAttrs);
        // only EntityId field guaranteed to exist
        config.joinKeys.put(inputIdx, EntityId.name());
        // not copying from exist store
        currAttrs.removeAll(lastSystemUpdateAttrs.values());
    }

    /*-
     * generate src attr -> dst attr map for system last update time attributes
     */
    public static Map<String, String> systemLastUpdateTimeAttrs(@NotNull String entity, List<String> templates) {
        if (CollectionUtils.isEmpty(templates)) {
            return Collections.emptyMap();
        }
        return templates.stream().distinct().map(template -> {
            // copy last update time to entity from this system/template
            String srcAttr = formatSystemAttribute(template, CDLUpdatedTime.name());
            String tgtAttr = formatSystemEntityAttribute(template, entity, EntityLastUpdatedDate.name());
            return Pair.of(srcAttr, tgtAttr);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    /**
     * Get all curated attributes in current table
     *
     * @param table
     *            current curated attribute table
     * @param entity
     *            target entity
     * @param templates
     *            import template names for target entity
     * @param inactive
     *            inactive data collection version
     * @return set of curated attributes exist in current table
     */
    public static Set<String> currentCuratedAttributes(Table table, @NotNull String entity,
            @NotNull List<String> templates,
            @NotNull DataCollection.Version inactive) {
        if (table == null) {
            log.info("No existing table found for curated {} attributes in inactive version {}", entity.toLowerCase(),
                    inactive);
            return Collections.emptySet();
        }

        return Arrays.stream(table.getAttributeNames()) //
                .filter(attr -> {
                    // optional curated attrs might not be re-calculated everytime (need to be
                    // copied from old table)
                    if (NumberOfContacts.name().equals(attr) || LastActivityDate.name().equals(attr)) {
                        return true;
                    }

                    // check if this is a system last modified date attribute
                    return templates.stream()
                            .map(tmpl -> formatSystemEntityAttribute(tmpl, entity, EntityLastUpdatedDate.name()))
                            .anyMatch(attr::equals);
                }) //
                .collect(Collectors.toSet());
    }

    /**
     * generate template -> entity type (of that template) map
     *
     * @param taskMap
     *            template name -> data feed task reference map
     * @return non null map
     */
    public static Map<String, String> templateEntityTypeMap(Map<String, DataFeedTask> taskMap) {
        if (MapUtils.isEmpty(taskMap)) {
            return Collections.emptyMap();
        }

        return taskMap.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), getEntity(entry.getValue()))) //
                .filter(entry -> StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    /**
     * generate template name -> source value map
     *
     * @param templateSystemMap
     *            template name -> system name map
     * @param systemMap
     *            system name -> system reference map
     * @return non null map
     */
    public static Map<String, String> templateSourceMap(@NotNull Map<String, String> templateSystemMap,
            @NotNull Map<String, S3ImportSystem> systemMap) {
        Map<String, String> templateValues = new HashMap<>();
        if (MapUtils.isEmpty(templateSystemMap)) {
            return templateValues;
        }

        templateSystemMap.forEach((tmpl, systemName) -> {
            if (!systemMap.containsKey(systemName)) {
                log.warn("System {} for template {} does not exist in system map", systemName, tmpl);
                return;
            }

            String systemDisplayName = systemMap.get(systemName).getDisplayName();
            if (StringUtils.isNotBlank(systemDisplayName)) {
                templateValues.put(tmpl, systemDisplayName.trim());
            } else {
                log.warn("Template {} has blank system display name ({})", tmpl, systemDisplayName);
            }
        });

        log.info("template -> source map = {}", templateValues);
        return templateValues;
    }

    /**
     * Add display name, description and other metadata for curated attributes
     *
     * @param servingStoreTable
     *            generated curated attribute table
     * @param category
     *            target category
     * @param entity
     *            target entity
     * @param templateSystemMap
     *            template name -> system name map
     * @param systemMap
     *            system name -> system reference map
     */
    public static void enrichTableSchema(@NotNull Table servingStoreTable, @NotNull Category category,
            @NotNull String entity, @NotNull Map<String, String> templateSystemMap,
            @NotNull Map<String, S3ImportSystem> systemMap) {
        List<Attribute> attrs = servingStoreTable.getAttributes();
        attrs.forEach(attr -> {
            if (NumberOfContacts.name().equals(attr.getName())) {
                attr.setCategory(category);
                attr.setSubcategory(null);
                attr.setDisplayName(NUMBER_OF_CONTACTS_DISPLAY_NAME);
                attr.setDescription(NUMBER_OF_CONTACTS_DESCRIPTION);
                attr.setFundamentalType(FundamentalType.NUMERIC.getName());
            } else if (LastActivityDate.name().equals(attr.getName())) {
                // not prefix last activity date since it's already consumed in other places
                enrichDateAttribute(attr, category, LAST_ACTIVITY_DATE_DISPLAY_NAME, LAST_ACTIVITY_DATE_DESCRIPTION);
            } else if (formatEntityAttribute(entity, EntityLastUpdatedDate.name()).equals(attr.getName())) {
                enrichDateAttribute(attr, category, ENTITY_MODIFIED_DATE_DISPLAY_NAME,
                        String.format(ENTITY_MODIFIED_DATE_DESC_FMT, entity.toLowerCase()));
            } else if (formatEntityAttribute(entity, EntityCreatedDate.name()).equals(attr.getName())) {
                enrichDateAttribute(attr, category, ENTITY_CREATED_DATE_DISPLAY_NAME,
                        String.format(ENTITY_CREATED_DATE_DESC_FMT, entity.toLowerCase()));
            } else if (formatEntityAttribute(entity, EntityCreatedSource.name()).equals(attr.getName())) {
                attr.setCategory(category);
                attr.setSubcategory(null);
                attr.setDisplayName(ENTITY_CREATED_SOURCE_DISPLAY_NAME);
                attr.setDescription(String.format(ENTITY_CREATED_SOURCE_DESC_FMT, entity.toLowerCase()));
            } else if (formatEntityAttribute(entity, EntityCreatedType.name()).equals(attr.getName())) {
                attr.setCategory(category);
                attr.setSubcategory(null);
                attr.setDisplayName(ENTITY_CREATED_ENTITY_TYPE_DISPLAY_NAME);
                attr.setDescription(String.format(ENTITY_CREATED_ENTITY_TYPE_DESC_FMT, entity.toLowerCase()));
            } else {
                enrichSystemAttributes(attr, entity, category, templateSystemMap, systemMap);
            }
        });
    }

    private static String formatSystemEntityAttribute(@NotNull String template, @NotNull String entity,
            @NotNull String attributeName) {
        return formatSystemAttribute(template, formatEntityAttribute(entity, attributeName));
    }

    private static String formatEntityAttribute(@NotNull String entity, @NotNull String attributeName) {
        // prefix attr name with entity to prevent conflict
        return String.format(ENTITY_ATTR_PREFIX_FORMAT, entity) + attributeName;
    }

    private static String formatSystemAttribute(@NotNull String template, @NotNull String attributeName) {
        return String.format(SYSTEM_ATTR_FORMAT, template, attributeName);
    }

    private static String getEntity(DataFeedTask task) {
        if (task == null || StringUtils.isBlank(task.getEntity())) {
            return null;
        }

        String entity = task.getEntity();
        DataFeedTask.SubType subType = task.getSubType();
        if (subType != null) {
            // use more specific name (e.g., Lead, Opportunity)
            return subType.name();
        } else if (BusinessEntity.ActivityStream.name().equals(entity)) {
            // for legacy reason, WebVisit have null as subType and entity is ActivityStream
            // TODO fix this if we ever add subType to WebVisit
            return EntityType.WebVisit.name();
        } else {
            return StringUtils.isBlank(entity) ? null : entity;
        }
    }

    private static void enrichSystemAttributes(@NotNull Attribute attribute, @NotNull String entity,
            @NotNull Category category, @NotNull Map<String, String> templateSystemMap,
            @NotNull Map<String, S3ImportSystem> systemMap) {
        if (StringUtils.isBlank(attribute.getName())) {
            return;
        }

        Optional<String> matchingTmpl = templateSystemMap.keySet() //
                .stream() //
                .filter(tmpl -> {
                    String attr = formatSystemEntityAttribute(tmpl, entity, EntityLastUpdatedDate.name());
                    return attr.equals(attribute.getName());
                }) //
                .findFirst();
        matchingTmpl.ifPresent(tmpl -> {
            String system = templateSystemMap.get(tmpl);
            if (!systemMap.containsKey(system)) {
                log.warn("No corresponding system found for template {}, system {} in attribute {}", tmpl, system,
                        attribute.getName());
                return;
            }

            S3ImportSystem sys = systemMap.get(system);
            if (sys == null || StringUtils.isBlank(sys.getDisplayName())) {
                log.warn("No display name found for system {}", system);
                return;
            }
            String displayName = String.format(ENTITY_SYS_MODIFIED_DATE_NAME_FMT, sys.getDisplayName());
            String description = String.format(ENTITY_SYS_MODIFIED_DATE_DESC_FMT, entity.toLowerCase(),
                    sys.getDisplayName());
            log.info("Enriching system attribute {} with system display name {}. system = {}, template = {}",
                    attribute.getName(), sys.getDisplayName(), system, tmpl);
            enrichDateAttribute(attribute, category, displayName, description);
        });
    }

    private static void enrichDateAttribute(@NotNull Attribute attribute, @NotNull Category category,
            @NotNull String displayName, @NotNull String description) {
        attribute.setCategory(category);
        attribute.setSubcategory(null);
        attribute.setDisplayName(displayName);
        attribute.setDescription(description);
        attribute.setLogicalDataType(LogicalDataType.Date);
        attribute.setFundamentalType(FundamentalType.DATE.getName());
    }
}
