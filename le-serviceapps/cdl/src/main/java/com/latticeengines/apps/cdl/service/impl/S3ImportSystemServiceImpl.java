package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.S3ImportSystemEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.DataFeedTaskTemplateService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("s3ImportSystemService")
public class S3ImportSystemServiceImpl implements S3ImportSystemService {

    private static final Logger log = LoggerFactory.getLogger(S3ImportSystemServiceImpl.class);

    private static final String DEFAULTSYSTEM = "DefaultSystem";

    @Inject
    private S3ImportSystemEntityMgr s3ImportSystemEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private DataFeedTaskTemplateService dataFeedTaskTemplateService;

    @Override
    public void createS3ImportSystem(String customerSpace, S3ImportSystem importSystem) {
        if (importSystem == null) {
            log.warn("Create NULL S3ImportSystem!");
            return;
        }
        if (s3ImportSystemEntityMgr.findS3ImportSystem(importSystem.getName()) != null) {
            throw new LedpException(LedpCode.LEDP_40066, new String[] {
                    "Already have import system with name: " + importSystem.getName()});
        }
        log.info("Create S3Import System: " + importSystem.getName());
        List<S3ImportSystem> currentSystems = s3ImportSystemEntityMgr.findAll();
        if (CollectionUtils.isEmpty(currentSystems)) {
            importSystem.setPriority(1);
        } else {
            if (importSystem.getPriority() == 1) {
                for (S3ImportSystem system : currentSystems) {
                    system.setPriority(system.getPriority() + 1);
                    s3ImportSystemEntityMgr.update(system);
                }
            } else {
                importSystem.setPriority(currentSystems.size() + 1);
            }
        }
        s3ImportSystemEntityMgr.createS3ImportSystem(importSystem);
    }

    @Override
    public void createDefaultImportSystem(String customerSpace) {
        S3ImportSystem importSystem = new S3ImportSystem();
        importSystem.setPriority(1);
        importSystem.setName(DEFAULTSYSTEM);
        importSystem.setDisplayName(DEFAULTSYSTEM);
        importSystem.setSystemType(S3ImportSystem.SystemType.Other);
        importSystem.setTenant(tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString()));
        createS3ImportSystem(customerSpace, importSystem);
    }

    @Override
    public void updateS3ImportSystem(String customerSpace, S3ImportSystem importSystem) {
        S3ImportSystem s3ImportSystem = s3ImportSystemEntityMgr.findS3ImportSystem(importSystem.getName());
        if (s3ImportSystem == null) {
            log.warn("Cannot find import System with name: " + importSystem.getName());
            return;
        }
        List<S3ImportSystem> currentSystems = s3ImportSystemEntityMgr.findAll();
        // check if we can set current system as primary
        if (importSystem.getPriority() > 1 && importSystem.isMapToLatticeAccount()) {
            for (S3ImportSystem system : currentSystems) {
                if (system.getPriority() == 1) {
                    if (system.isMapToLatticeAccount()) {
                        throw new LedpException(LedpCode.LEDP_40075, new String[] {system.getDisplayName(), "Account ID"});
                    }
                }
            }
            importSystem.setPriority(1);
        }
        if (importSystem.isMapToLatticeContact()) {
            for (S3ImportSystem system : currentSystems) {
                if (!system.getName().equals(importSystem.getName()) && Boolean.TRUE.equals(system.isMapToLatticeContact())) {
                    throw new LedpException(LedpCode.LEDP_40075, new String[] {system.getDisplayName(), "Contact ID"});
                }
            }
        }

        s3ImportSystem.setDisplayName(importSystem.getDisplayName());
        if (StringUtils.isEmpty(s3ImportSystem.getAccountSystemId())) {
            s3ImportSystem.setAccountSystemId(importSystem.getAccountSystemId());
        }
        if (StringUtils.isEmpty(s3ImportSystem.getContactSystemId())) {
            s3ImportSystem.setContactSystemId(importSystem.getContactSystemId());
        }
        if (importSystem.getSecondaryAccountIds() != null) {
            s3ImportSystem.setSecondaryAccountIds(importSystem.getSecondaryAccountIds());
        }
        if (importSystem.getSecondaryContactIds() != null) {
            s3ImportSystem.setSecondaryContactIds(importSystem.getSecondaryContactIds());
        }
        s3ImportSystem.setMapToLatticeAccount(importSystem.isMapToLatticeAccount());
        s3ImportSystem.setMapToLatticeContact(importSystem.isMapToLatticeContact());
        if (importSystem.getPriority() != s3ImportSystem.getPriority() && importSystem.getPriority() < Integer.MAX_VALUE) {
            int currentPriority = s3ImportSystem.getPriority();
            int destPriority = importSystem.getPriority();
            // 5->3
            if (currentPriority > destPriority) {
                for (S3ImportSystem system : currentSystems) {
                    if (system.getPriority() >= destPriority && system.getPriority() < currentPriority) {
                        system.setPriority(system.getPriority() + 1);
                        s3ImportSystemEntityMgr.update(system);
                    }
                }
            } else { // 3->5
                for (S3ImportSystem system : currentSystems) {
                    if (system.getPriority() <= destPriority && system.getPriority() > currentPriority) {
                        system.setPriority(system.getPriority() - 1);
                        s3ImportSystemEntityMgr.update(system);
                    }
                }
            }
            s3ImportSystem.setPriority(importSystem.getPriority());
        }
        s3ImportSystemEntityMgr.update(s3ImportSystem);
    }

    @Override
    public S3ImportSystem getS3ImportSystem(String customerSpace, String name) {
        S3ImportSystem importSystem = s3ImportSystemEntityMgr.findS3ImportSystem(name);
        if (importSystem == null && DEFAULTSYSTEM.equals(name)) {
            log.warn("DefaultSystem will not be created when bootstrap EntityMatch tenant. " +
                    "Please create DefaultSystem explicitly!");
        }
        return importSystem;
    }

    @Override
    public List<S3ImportSystem> getAllS3ImportSystem(String customerSpace) {
        return s3ImportSystemEntityMgr.findAll();
    }

    @Override
    public void updateAllS3ImportSystemPriority(String customerSpace, List<S3ImportSystem> systemList) {
        if (CollectionUtils.isEmpty(systemList)) {
            return;
        }
        List<S3ImportSystem> currentSystems = s3ImportSystemEntityMgr.findAll();
        if (currentSystems.size() != systemList.size()) {
            throw new LedpException(LedpCode.LEDP_40062, new String[] {String.valueOf(currentSystems.size()),
                    String.valueOf(systemList.size())});
        }
        Map<String, S3ImportSystem> systemMap = systemList.stream()
                .collect(Collectors.toMap(S3ImportSystem::getName, system -> system));
        for (S3ImportSystem importSystem : currentSystems) {
            if (!systemMap.containsKey(importSystem.getName())) {
                throw new LedpException(LedpCode.LEDP_40063, new String[] {importSystem.getName()});
            }
        }
        Optional<S3ImportSystem> primarySystem = currentSystems.stream().filter(system -> system.getPriority() == 1).findFirst();
        Optional<S3ImportSystem> newPrimarySystem = systemList.stream().filter(system -> system.getPriority() == 1).findFirst();
        if (primarySystem.isPresent() && newPrimarySystem.isPresent()) {
            if (!primarySystem.get().getName().equals(newPrimarySystem.get().getName())
                    && (primarySystem.get().isMapToLatticeAccount())) {
                throw new LedpException(LedpCode.LEDP_40061, new String[] {primarySystem.get().getDisplayName()});
            }
        }

        for (S3ImportSystem importSystem : currentSystems) {
            S3ImportSystem newSystem = systemMap.get(importSystem.getName());
            if (newSystem.getPriority() != importSystem.getPriority()) {
                importSystem.setPriority(newSystem.getPriority());
                s3ImportSystemEntityMgr.update(importSystem);
            }
        }
    }

    @Override
    public void validateAndUpdateSystemPriority(String customerSpace, List<S3ImportSystem> systemList) {
        if (CollectionUtils.isEmpty(systemList)) {
            return;
        }
        List<S3ImportSystem> currentSystems = s3ImportSystemEntityMgr.findAll();
        if (currentSystems.size() != systemList.size()) {
            throw new LedpException(LedpCode.LEDP_40062, new String[] {String.valueOf(currentSystems.size()),
                    String.valueOf(systemList.size())});
        }
        Map<String, S3ImportSystem> systemMap = systemList.stream()
                .collect(Collectors.toMap(S3ImportSystem::getName, system -> system));
        for (S3ImportSystem importSystem : currentSystems) {
            if (!systemMap.containsKey(importSystem.getName())) {
                throw new LedpException(LedpCode.LEDP_40063, new String[] {importSystem.getName()});
            }
        }
        Optional<S3ImportSystem> primarySystem = currentSystems.stream().filter(system -> system.getPriority() == 1).findFirst();
        Optional<S3ImportSystem> newPrimarySystem = systemList.stream().filter(system -> system.getPriority() == 1).findFirst();
        if (primarySystem.isPresent() && newPrimarySystem.isPresent()) {
            if (!primarySystem.get().getName().equals(newPrimarySystem.get().getName())
                    && (primarySystem.get().isMapToLatticeAccount())) {
                throw new LedpException(LedpCode.LEDP_40061, new String[] {primarySystem.get().getDisplayName()});
            }
        }

        // Mark the changed systems and also the priority delta.
        List<Pair<S3ImportSystem, Integer>> changedSystems = new ArrayList<>();
        for (S3ImportSystem importSystem : currentSystems) {
            S3ImportSystem newSystem = systemMap.get(importSystem.getName());
            if (newSystem.getPriority() != importSystem.getPriority()) {
                changedSystems.add(Pair.of(importSystem, newSystem.getPriority() - importSystem.getPriority()));
                importSystem.setPriority(newSystem.getPriority());
            }
        }
        if (CollectionUtils.isNotEmpty(changedSystems)) {
            Map<String, Integer> warningSystems = validatePriorityChange(customerSpace, changedSystems);
            if (MapUtils.isEmpty(warningSystems)) {
                changedSystems.forEach(systemPair -> s3ImportSystemEntityMgr.update(systemPair.getLeft()));
            } else {
                // If the all changed system(with data) has the same delta,
                // then means the relative position is not changed.
                if (warningSystems.values().stream().distinct().count() > 1) {
                    throw new LedpException(LedpCode.LEDP_40091, new String[]{StringUtils.join(warningSystems.keySet(),
                            System.lineSeparator())});
                } else {
                    changedSystems.forEach(systemPair -> s3ImportSystemEntityMgr.update(systemPair.getLeft()));
                }
            }
        }
    }

    private Map<String, Integer> validatePriorityChange(String customerSpace,
                                              List<Pair<S3ImportSystem, Integer>> changedSystems) {
        Map<String, Integer> changedSystemNames =
                changedSystems.stream().collect(Collectors.toMap(pair -> pair.getLeft().getName(), Pair::getRight));
        Map<String, List<String>> systemToUniqueIdsMap = dataFeedTaskService.getSystemNameToUniqueIdsMap(customerSpace);
        Map<String, Integer> warningSystems = new HashMap<>();
        if (MapUtils.isNotEmpty(systemToUniqueIdsMap)) {
            log.info("System to UniqueId maps: " + JsonUtils.serialize(systemToUniqueIdsMap));
            systemToUniqueIdsMap.forEach((systemName, uniqueIdList) -> {
                if (changedSystemNames.containsKey(systemName)) {
                    for (String uniqueId: uniqueIdList) {
                        if (dataFeedTaskTemplateService.hasPAConsumedImportAction(customerSpace, uniqueId)) {
                            warningSystems.put(systemName, changedSystemNames.get(systemName));
                            break;
                        }
                    }
                }
            });
        }
        return warningSystems;
    }

    @Override
    public boolean hasSystemMapToLatticeAccount(String customerSpace) {
        return CollectionUtils.isNotEmpty(s3ImportSystemEntityMgr.findByMapToLatticeAccount(Boolean.TRUE));
    }

    @Override
    public boolean hasSystemMapToLatticeContact(String customerSpace) {
        return CollectionUtils.isNotEmpty(s3ImportSystemEntityMgr.findByMapToLatticeContact(Boolean.TRUE));
    }

    @Override
    public List<String> getAllS3ImportSystemIds(String customerSpace) {
        List<S3ImportSystem> allSystems = getAllS3ImportSystem(customerSpace);
        if (CollectionUtils.isEmpty(allSystems)) {
            return Collections.emptyList();
        }
        List<String> idList = new ArrayList<>();
        allSystems.forEach(system -> {
            if (StringUtils.isNotEmpty(system.getAccountSystemId())) {
                idList.add(system.getAccountSystemId());
            }
            if (StringUtils.isNotEmpty(system.getContactSystemId())) {
                idList.add(system.getContactSystemId());
            }
            List<String> secondaryAccountIds = system.getSecondaryAccountIdsSortByPriority();
            List<String> secondaryContactIds = system.getSecondaryContactIdsSortByPriority();
            if (CollectionUtils.isNotEmpty(secondaryAccountIds)) {
                idList.addAll(secondaryAccountIds);
            }
            if (CollectionUtils.isNotEmpty(secondaryContactIds)) {
                idList.addAll(secondaryContactIds);
            }
        });
        return idList;
    }

    @Override
    public void deleteS3ImportSystem(S3ImportSystem importSystem) {
        s3ImportSystemEntityMgr.delete(importSystem);
    }
}
