package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component("accountMasterColumnSelectionService")
public class AccountMasterColumnSelectionServiceImpl implements ColumnSelectionService {

    private Log log = LogFactory.getLog(ColumnSelectionServiceImpl.class);

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    private ConcurrentMap<Predefined, ColumnSelection> predefinedSelectionMap = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("taskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        loadCaches();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCaches();
            }
        }, TimeUnit.MINUTES.toMillis(1));
    }

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    public ColumnSelection parsePredefinedColumnSelection(Predefined predefined) {
        if (Predefined.supportedSelections.contains(predefined)) {
            return predefinedSelectionMap.get(predefined);
        } else {
            throw new UnsupportedOperationException("Selection " + predefined + " is not supported.");
        }
    }

    @Override
    public List<String> getMatchedColumns(ColumnSelection selection) {
        List<String> columnNames = new ArrayList<>();
        for (Column column : selection.getColumns()) {
            AccountMasterColumn externalColumn = accountMasterColumnService
                    .getMetadataColumn(column.getExternalColumnId());
            if (externalColumn != null) {
                columnNames.add(externalColumn.getAmColumnId());
            } else {
                columnNames.add(column.getColumnName());
            }
        }
        return columnNames;
    }

    @Override
    public Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentVersion(Predefined predefined) {
        return "2.0";
    }

    private void loadCaches() {
        for (Predefined selection : Predefined.supportedSelections) {
            try {
                List<AccountMasterColumn> externalColumns = accountMasterColumnService.findByColumnSelection(selection);
                ColumnSelection cs = new ColumnSelection();
                cs.createAccountMasterColumnSelection(externalColumns);
                predefinedSelectionMap.put(selection, cs);
            } catch (Exception e) {
                log.error(e);
            }
        }
    }
}
