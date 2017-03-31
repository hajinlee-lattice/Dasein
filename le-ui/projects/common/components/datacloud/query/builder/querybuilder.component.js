angular.module('common.datacloud.query.builder', [])
.controller('QueryBuilderCtrl', function($scope, $state, QueryRestriction, QueryStore, DataCloudStore, SegmentServiceProxy, BucketRestriction) {

    var vm = this;
    angular.extend(this, {
        filters: {
            any: {},
            all: {}
        },
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
            loading = false;
        });
    };
    vm.init();

    vm.update = function() {
        // debounced, spinner, update counts, update data
    };

    vm.moveToAll = function(key) {
        move(vm.filters.any, vm.filters.all, key);
    };

    vm.moveToAny = function(key) {
        move(vm.filters.all, vm.filters.any, key);
    };

    function move(src, dest, key) {
        var item = src[key];
        dest[key] = item;
        delete src[key];
    }

    vm.delete = function(src, key) {
        delete src[key];
    };

    vm.goAttributes = function() {
        $state.go('home.model.analysis.explorer.attributes');
    };

    vm.saveSegment = function() {
        var restrictions = createRestrictionsFromFilters(vm.filters);
        QueryStore.setRestriction(restrictions);
        vm.saving = true;
        SegmentServiceProxy.CreateOrUpdateSegment().then(function(result) {
            if (!result.errorMsg) {
                $state.go('home.model.segmentation', {}, {notify: true})
            }
        });
    }

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
                var fieldName = group[i].bucketRestriction.lhs.columnLookup.column_name;
                if (!filterGroup[fieldName]) {
                    var attribute = findAttribute(fieldName, attributesMetadata);
                    if (attribute) {
                        filterGroup[fieldName] = {
                            category: attribute.Category,
                            categoryClassName: attribute.Category.replace(/\s+/g, '-').toLowerCase(),
                            displayName: attribute.DisplayName,
                            buckets: []
                        };
                    }
                }

                filterGroup[fieldName].buckets.push({range: group[i].bucketRestriction.range});
            }
        }

        return filterFields;
    };

    function createRestrictionsFromFilters(filters) {
        var restrictions = { any:[], all:[] };

        for (var groupKey in filters) {
            var group = filters[groupKey];
            for (var fieldName in group) {
                var attribute = group[fieldName];
                for (var i = 0; i < attribute.buckets.length; i++) {
                    restrictions[groupKey].push(new BucketRestriction(fieldName, attribute.buckets[i].range));
                }
            }
        }

        return restrictions;
    }
});
