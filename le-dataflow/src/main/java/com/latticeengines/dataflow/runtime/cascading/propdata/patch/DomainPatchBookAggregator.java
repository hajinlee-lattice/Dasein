package com.latticeengines.dataflow.runtime.cascading.propdata.patch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DomainPatchBookAggregator extends BaseAggregator<DomainPatchBookAggregator.Context>
        implements Aggregator<DomainPatchBookAggregator.Context> {

    public static final String CLEANUP = "CLEANUP";
    public static final String ADD = "ADD";
    private static final long serialVersionUID = 2657797493053039356L;
    private int patchDunsLoc;
    private int patchDomainsLoc;

    public DomainPatchBookAggregator(Fields fieldDeclaration, String patchDunsField,
            String patchDomainsField) {
        super(fieldDeclaration);
        this.patchDunsLoc = namePositionMap.get(patchDunsField);
        this.patchDomainsLoc = namePositionMap.get(patchDomainsField);
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(PatchBook.COLUMN_DUNS);
        if (grpObj == null) {
            return true;
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.domainsToAdd = new HashSet<>();
        context.domainsToRemove = new HashSet<>();
        return context;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        if (context.duns == null) {
            context.duns = arguments.getString(PatchBook.COLUMN_DUNS);
            if (StringUtils.isBlank(context.duns)) {
                throw new RuntimeException("Found empty patching duns");
            }
        }

        String itemStr = arguments.getString(PatchBook.COLUMN_PATCH_ITEMS);
        Map map = JsonUtils.deserialize(itemStr, Map.class);
        Map<String, String> items = JsonUtils.convertMap(map, String.class, String.class);
        String domain = items.get(DataCloudConstants.ATTR_LDC_DOMAIN);
        if (StringUtils.isBlank(domain)) {
            throw new RuntimeException("Found empty domain for patching duns " + context.duns);
        }

        boolean cleanup = arguments.getBoolean(PatchBook.COLUMN_CLEANUP);
        if (cleanup) {
            context.domainsToRemove.add(domain);
        } else {
            context.domainsToAdd.add(domain);
        }
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        Map<String, List<String>> patchDomains = new HashMap<>();
        if (!context.domainsToRemove.isEmpty()) {
            patchDomains.put(CLEANUP, new ArrayList<>());
        }
        if (!context.domainsToAdd.isEmpty()) {
            patchDomains.put(ADD, new ArrayList<>());
        }
        context.domainsToRemove.forEach(domain -> {
            patchDomains.get(CLEANUP).add(domain);
        });
        context.domainsToAdd.forEach(domain -> {
            patchDomains.get(ADD).add(domain);
        });
        result.set(patchDunsLoc, context.duns);
        result.set(patchDomainsLoc, JsonUtils.serialize(patchDomains));
        return result;
    }

    public static class Context extends BaseAggregator.Context {
        String duns;
        Set<String> domainsToAdd;
        Set<String> domainsToRemove;
    }
}
