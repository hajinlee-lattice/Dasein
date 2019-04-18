/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.planner;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;

import com.dataartisans.flink.cascading.runtime.stats.AccumulatorCache;
import com.dataartisans.flink.cascading.runtime.stats.EnumStringConverter;

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;

@SuppressWarnings({ "rawtypes" })
public class FlinkFlowStepStats extends FlowStepStats {

    /**
     *
     */
    private static final long serialVersionUID = -4484514062904460899L;
    private AccumulatorCache accumulatorCache;

    protected FlinkFlowStepStats(FlowStep flowStep, ClientState clientState,
            AccumulatorCache accumulatorCache) {
        super(flowStep, clientState);
        this.accumulatorCache = accumulatorCache;
    }

    @Override
    public void recordChildStats() {
        // TODO
    }

    @Override
    public String getProcessStepID() {
        return null;
    }

    @Override
    public Collection<String> getCounterGroupsMatching(String regex) {
        return null;
    }

    @Override
    public void captureDetail(Type depth) {
        // TODO
    }

    @Override
    public long getLastSuccessfulCounterFetchTime() {
        return accumulatorCache.getLastUpdateTime();
    }

    @Override
    public Collection<String> getCounterGroups() {
        accumulatorCache.update();
        Map<String, Object> currentAccumulators = accumulatorCache.getCurrentAccumulators();
        Set<String> result = new HashSet<String>();

        for (String key : currentAccumulators.keySet()) {
            result.add(EnumStringConverter.groupCounterToGroup(key));
        }
        return result;
    }

    @Override
    public Collection<String> getCountersFor(String group) {
        accumulatorCache.update();
        Map<String, Object> currentAccumulators = accumulatorCache.getCurrentAccumulators();
        Set<String> result = new HashSet<String>();

        for (String key : currentAccumulators.keySet()) {
            if (EnumStringConverter.accInGroup(group, key)) {
                result.add(EnumStringConverter.groupCounterToCounter(key));
            }
        }
        return result;
    }

    @Override
    public long getCounterValue(Enum counter) {
        return getCounterValue(EnumStringConverter.enumToGroup(counter),
                EnumStringConverter.enumToCounter(counter));
    }

    @Override
    public long getCounterValue(String group, String counter) {
        accumulatorCache.update();
        Map<String, Object> currentAccumulators = accumulatorCache.getCurrentAccumulators();

        for (String key : currentAccumulators.keySet()) {
            if (EnumStringConverter.accMatchesGroupCounter(key, group, counter)) {
                Object o = currentAccumulators.get(key);
                OptionalFailure<Long> failure = (OptionalFailure<Long>) o;
                try {
                    return failure.get();
                } catch (FlinkException e) {
                    logWarn("Failed to extract count.", e);
                }
            }
        }
        // Cascading returns 0 in case of empty accumulators
        return 0;
    }

}
