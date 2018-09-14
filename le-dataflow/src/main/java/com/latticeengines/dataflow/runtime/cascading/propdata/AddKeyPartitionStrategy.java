package com.latticeengines.dataflow.runtime.cascading.propdata;

import cascading.tuple.TupleEntry;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddFieldStrategyBase;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;

import java.util.Map;

/**
 * Add KeyPartition column whose value is based on the combination of non-null {@link MatchKey} in the tuple entry
 */
public class AddKeyPartitionStrategy extends AddFieldStrategyBase {

	private static final long serialVersionUID = -288839367295707093L;
	private final Map<MatchKey, String> matchKeyColumnNameMap;

    /**
     * Constructor to pass in the map from {@link MatchKey} to the column name
     * @param columnName key partition column name
     * @param matchKeyColumnNameMap value is the associated column name
     */
    public AddKeyPartitionStrategy(@NotNull String columnName, @NotNull Map<MatchKey, String> matchKeyColumnNameMap) {
        super(columnName, String.class);
        Preconditions.checkNotNull(columnName);
        Preconditions.checkNotNull(matchKeyColumnNameMap);
        this.matchKeyColumnNameMap = matchKeyColumnNameMap;
    }

    @Override
    public Object compute(TupleEntry arguments) {
        String name = get(arguments, matchKeyColumnNameMap.get(MatchKey.Name));
        String country = get(arguments, matchKeyColumnNameMap.get(MatchKey.Country));
        String state = get(arguments, matchKeyColumnNameMap.get(MatchKey.State));
        String city = get(arguments, matchKeyColumnNameMap.get(MatchKey.City));
        return MatchKeyUtils.evalKeyPartition(getTuple(name, country, state, city));
    }

    private String get(@NotNull TupleEntry entry, String columnName) {
        if (columnName == null) {
            return null;
        }

        return entry.getString(columnName);
    }

    private MatchKeyTuple getTuple(String name, String country, String state, String city) {
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setName(name);
        tuple.setCountryCode(country);
        tuple.setState(state);
        tuple.setCity(city);
        return tuple;
    }
}
