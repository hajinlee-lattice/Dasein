package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Service("CDLColumnSelectionService")
public class CDLColumnSelectionServiceImpl implements ColumnSelectionService {

    @Override
    public boolean accept(String version) {
        return false;
    }

    @Override
    public ColumnSelection parsePredefinedColumnSelection(ColumnSelection.Predefined predefined, String dataCloudVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Pair<BitCodeBook, List<String>>> getDecodeParameters(ColumnSelection columnSelection, String dataCloudVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentVersion(ColumnSelection.Predefined predefined) {
        return "1.0";
    }

}
