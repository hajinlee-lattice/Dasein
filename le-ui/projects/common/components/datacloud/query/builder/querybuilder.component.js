angular.module('common.datacloud.query.builder', [])
.controller('QueryBuilderCtrl', function($scope, $state, QueryRestriction, QueryStore, DataCloudStore, SegmentServiceProxy, BucketRestriction) {

    var vm = this;
    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        filters: {
            any: [],
            all: []
        },
        cubeStats: null,
        loading: true,
        saving: false
    });

    vm.size = function(obj) {
        if (angular.isArray(obj)) {
            return obj.length;
        } else {
            return Object.keys(obj).length;
        }
    };

    vm.init = function () {
        DataCloudStore.getEnrichments().then(function(result) {
            vm.filters = createFiltersFromRestrictions(QueryRestriction, result.data);
            vm.loading = false;
        }).then(function() {
            if (vm.inModel) {
                DataCloudStore.getCube().then(function(result) {
                    vm.cubeStats = result.data.Stats;
                    addLiftToRestrictions();
                });
            }
        });
    };

    vm.update = function() {
        var restrictions = createRestrictionsFromFilters(vm.filters);
        QueryStore.updateRestriction(restrictions);
    };

    vm.move = function(src, dest, key) {
        var item = src[key];
        dest[key] = item;
        delete src[key];

        vm.update();
    };

    vm.delete = function(group, columnName, index) {
        var filterGroup = vm.filters[group];
        var filterAttribute = filterGroup[columnName];

        filterAttribute.buckets.splice(index, 1);
        if (filterAttribute.buckets.length === 0) {
            delete filterGroup[columnName];
        }

        vm.update();
    };

    vm.goAttributes = function() {
        if (vm.inModel) {
            $state.go('home.model.analysis.explorer.attributes');
        } else{
            $state.go('home.segment.explorer.attributes');
        }
    };

    vm.saveSegment = function() {
        vm.saving = true;
        SegmentServiceProxy.CreateOrUpdateSegment().then(function(result) {
            if (!result.errorMsg) {
                if (vm.inModel) {
                    $state.go('home.model.segmentation', {}, {notify: true});
                } else {
                    $state.go('home.segments', {}, {notify: true});
                }
            }
        }).finally(function() {
            vm.saving = false;
        });
    };

    function findAttribute(columnName, attributes) {
        for (var i = 0; i < attributes.length; i++) {
            if (attributes[i].FieldName === columnName) {
                return attributes[i];
            }
        }

        return null;
    }

    function createFiltersFromRestrictions(restrictions, attributesMetadata) {
        var filterFields = {};
        for (var groupKey in restrictions) {
            filterFields[groupKey] = {};
            var filterGroup = filterFields[groupKey];

            var group = restrictions[groupKey];
            for (var i = 0; i < group.length; i++) {
                var fieldName = BucketRestriction.getColumnName(group[i].bucketRestriction);
                var objectType = BucketRestriction.getObjectType(group[i].bucketRestriction);
                if (!filterGroup[fieldName]) {
                    var attribute = findAttribute(fieldName, attributesMetadata);
                    if (attribute) {
                        filterGroup[fieldName] = {
                            category: attribute.Category,
                            categoryClassName: attribute.Category.replace(/\s+/g, '-').toLowerCase(),
                            columnName: fieldName,
                            objectType: objectType,
                            displayName: attribute.DisplayName,
                            buckets: []
                        };
                    }
                }

                filterGroup[fieldName].buckets.push({
                    range: group[i].bucketRestriction.range
                });
            }
        }

        return filterFields;
    }

    function createRestrictionsFromFilters(filters) {
        var restrictions = { any:[], all:[] };

        for (var groupKey in filters) {
            var group = filters[groupKey];
            for (var fieldName in group) {
                var attribute = group[fieldName];
                for (var i = 0; i < attribute.buckets.length; i++) {
                    restrictions[groupKey].push({
                        bucketRestriction: new BucketRestriction(fieldName, attribute.objectType, attribute.buckets[i].range)
                    });
                }
            }
        }

        return restrictions;
    }

    function addLiftToRestrictions() {
        for (var groupKey in vm.filters) {
            var group = vm.filters[groupKey];
            for (var fieldName in group) {
                var restrictionBuckets = group[fieldName].buckets;

                var cubeStat = vm.cubeStats[fieldName];
                if (cubeStat && cubeStat.RowStats && cubeStat.RowStats.Bkts && cubeStat.RowStats.Bkts.List) {
                    var cubeBuckets = cubeStat.RowStats.Bkts.List;

                    for (var i = 0; i < restrictionBuckets.length; i++) {
                        var liftSet = false;
                        for (var j = 0; j < cubeBuckets.length; j++) {
                            if (BucketRestriction.isEqualRange(restrictionBuckets[i].range, cubeBuckets[j].Range)) {
                                restrictionBuckets[i].lift = cubeBuckets[j].Lift.toFixed(1) + 'x'
                                liftSet = true;
                                break;
                            }
                        }
                        if (!liftSet) {
                            restrictionBuckets[i].lift = null;
                        }
                    }
                } else {
                    for (var i = 0; i < restrictionBuckets.length; i++) {
                        restrictionBuckets[i].lift = null;
                    }
                }
            }
        }
    }

    vm.init();
});
