package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants.TPS_ATTR_RECORD_ID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.service.GenericMetadataService;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Service
public class GenericMetadataServiceImpl implements GenericMetadataService {

    private static final Logger log = LoggerFactory.getLogger(GenericMetadataServiceImpl.class);

    @Inject
    private PrimeMetadataService primeMetadataService;

    @Inject
    private BeanDispatcherImpl beanDispatcher;

    @Override
    public ColumnSelection parseColumnSelection(MatchInput input) {
        if (BusinessEntity.PrimeAccount.name().equals(input.getTargetEntity())) {
            //FIXME: should merge to column selection service after ingesting DataBlock metadata
            return input.getCustomSelection();
        } else if (ContactMasterConstants.MATCH_ENTITY_TPS.equals(input.getTargetEntity())) {
            // FIXME [M39-LiveRamp]: to be changed to read from SQL
            ColumnSelection columnSelection = new ColumnSelection();
            columnSelection.setColumns(Collections.singletonList(new Column(TPS_ATTR_RECORD_ID)));
            return columnSelection;
        } else {
            // Primary LDC matches
            ColumnSelectionService columnSelectionService = beanDispatcher
                    .getColumnSelectionService(input.getDataCloudVersion());
            String dataCloudVersion = input.getDataCloudVersion();
            if (input.getUnionSelection() != null) {
                return combineSelections(columnSelectionService, input.getUnionSelection(), dataCloudVersion);
            } else if (input.getPredefinedSelection() != null) {
                return columnSelectionService.parsePredefinedColumnSelection(input.getPredefinedSelection(),
                        dataCloudVersion);
            } else {
                return input.getCustomSelection();
            }
        }
    }

    @Override
    public List<ColumnMetadata> getOutputSchema(MatchInput input, ColumnSelection columnSelection) {
        if (CollectionUtils.isNotEmpty(input.getMetadatas())) {
            return input.getMetadatas();
        }
        if (BusinessEntity.PrimeAccount.name().equals(input.getTargetEntity())) {
            Set<String> requestedCols = new HashSet<>(columnSelection.getColumnIds());
            Map<String, PrimeColumn> columnMap  = primeMetadataService.getPrimeColumns(requestedCols) //
                    .stream().collect(Collectors.toMap(PrimeColumn::getPrimeColumnId, Function.identity()));
            return columnSelection.getColumnIds().stream().filter(columnMap::containsKey) //
                    .map(columnMap::get).map(PrimeColumn::toColumnMetadata).collect(Collectors.toList());
        } else if (ContactMasterConstants.MATCH_ENTITY_TPS.equals(input.getTargetEntity())) {
            // FIXME [M39-LiveRamp]: to be changed to read from SQL, using columnSelection.getColumnIds()
            ColumnMetadata cm = new ColumnMetadata();
            cm.setAttrName(TPS_ATTR_RECORD_ID);
            cm.setDisplayName("Record Id");
            cm.setJavaClass("String");
            return Collections.singletonList(cm);
        } else {
            String dataCloudVersion = input.getDataCloudVersion();
            ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
            return columnMetadataService.fromSelection(columnSelection, dataCloudVersion);
        }
    }

    @Override
    public Schema getOutputAvroSchema(MatchInput input, ColumnSelection columnSelection) {
        Schema outputSchema;
        String targetEntity = input.getTargetEntity();
        if (ContactMasterConstants.MATCH_ENTITY_TPS.equals(input.getTargetEntity())) {
            // FIXME [M39-LiveRamp]: read and parse from SQL
            List<Pair<String, Class<?>>> pairs = new ArrayList<>();
            pairs.add(Pair.of(TPS_ATTR_RECORD_ID, String.class));
            outputSchema = AvroUtils.constructSchema("TriPeopleSegment", pairs);
        } else if (BusinessEntity.PrimeAccount.name().equals(input.getTargetEntity())) {
            List<PrimeColumn> primeColumns = //
                    primeMetadataService.getPrimeColumns(columnSelection.getColumnIds());
            Map<String, PrimeColumn> columnMap  = primeColumns.stream() //
                    .collect(Collectors.toMap(PrimeColumn::getPrimeColumnId, Function.identity()));
            List<Pair<String, Class<?>>> pairs = new ArrayList<>();
            columnSelection.getColumnIds().forEach(columnId -> {
                    if (columnMap.containsKey(columnId)) {
                        PrimeColumn pc = columnMap.get(columnId);
                        try {
                            pairs.add(Pair.of(pc.getAttrName(), //
                                    Class.forName("java.lang." + pc.getJavaClass())));
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException("Cannot parse java class " + pc.getJavaClass(), e);
                        }
                    } else {
                        log.warn("[{}] is not a valid PrimeColumn", columnId);
                    }
            });
            outputSchema = AvroUtils.constructSchema("PrimeAccount", pairs);
        } else {
            throw new UnsupportedOperationException("Do not know how to construct avro schema for " + targetEntity);
        }
        return outputSchema;
    }

    private ColumnSelection combineSelections(ColumnSelectionService columnSelectionService,
                                             UnionSelection unionSelection, String dataCloudVersion) {
        List<ColumnSelection> selections = new ArrayList<>();
        for (Map.Entry<ColumnSelection.Predefined, String> entry : unionSelection.getPredefinedSelections().entrySet()) {
            ColumnSelection.Predefined predefined = entry.getKey();
            selections.add(columnSelectionService.parsePredefinedColumnSelection(predefined, dataCloudVersion));
        }
        if (unionSelection.getCustomSelection() != null && !unionSelection.getCustomSelection().isEmpty()) {
            selections.add(unionSelection.getCustomSelection());
        }
        return ColumnSelection.combine(selections);
    }

}
