angular.module('common.datacloud.valuepicker', [])
    .controller('ValuePickerController', function (
        $state, $stateParams, $timeout, DataCloudStore, QueryStore,
        QueryTreeService, PickerBuckets, SegmentStore, Segment
    ) {
        var vm = this;

        angular.extend(vm, {
            stateParams: $stateParams,
            buckets: PickerBuckets.Bkts.List,
            picker_object: null,
            sortPrefix: '-',
            page: 1,
            row_limit: 15,
            page_size: Math.ceil(15 * 3),
            mode: $stateParams.mode
        });

        vm.init = function () {
            vm.picker_object = QueryTreeService.getPickerObject() || {
                item: null,
                restriction: null
            };

            if (vm.picker_object.item) {
                vm.item = vm.picker_object.item;
                vm.fieldname = vm.item.ColumnId;
                vm.entity = vm.item.Entity;

                if (!vm.item.cube) {
                    vm.item.cube = PickerBuckets;
                }
            }

            if (vm.picker_object.restriction) {
                vm.bucketRestriction = vm.picker_object.restriction.bucketRestriction;

                var values = vm.bucketRestriction.bkt.Vals;

                vm.buckets.forEach(function (bucket) {
                    if (values.indexOf(bucket.Vals[0]) >= 0) {
                        bucket.checked = true;
                    }
                })
            }
        }

        vm.changeBucketState = function (bucket) {
            var restriction = vm.bucketRestriction,
                bucket = angular.copy(bucket),
                vals, bkt;

            if (restriction) {
                bkt = restriction.bkt;
                vals = bkt.Vals;
            }

            if (bucket.checked) {
                var entity = vm.item.Entity.toLowerCase();

                if (!bkt || !vals) {
                    QueryStore['add' + vm.item.Entity + 'Restriction']({
                        columnName: vm.fieldname,
                        resourceType: vm.entity,
                        bkt: bucket
                    });

                    var restriction = QueryStore[entity + 'Restriction'].restriction;
                    var restrictions = restriction.logicalRestriction.restrictions;

                    vm.bucketRestriction = restrictions[restrictions.length - 1].bucketRestriction;
                    vm.picker_object.restriction = {
                        "$$hashKey": "object:9999",
                        "bucketRestriction": vm.bucketRestriction,
                        "collapsed": false,
                        "labelGlyph": restrictions.length
                    }
                    bkt = vm.bucketRestriction.bkt;
                    vals = bkt.Vals;
                } else {
                    vals.push(bucket.Vals[0]);
                }
            } else {
                vals.splice(vals.indexOf(bucket.Vals[0]), 1);
            }

            if (vals.length == 0) {
                bkt.Cmp = 'IS_NULL';
            } else if (bkt.Cmp == 'NOT_EQUAL' || bkt.Cmp == 'NOT_IN_COLLECTION') {
                bkt.Cmp = vals.length == 1 ? 'NOT_EQUAL' : 'NOT_IN_COLLECTION';
            } else {
                bkt.Cmp = vals.length == 1 ? 'EQUAL' : 'IN_COLLECTION';
            }

            if (vm.controller) {
                vm.updateCounts(vals);
            }

            QueryStore.setPublicProperty('enableSaveSegmentButton', true);
        }

        vm.getBucketLabel = function (bucket) {
            return bucket.labelGlyph;
        }

        vm.updateCounts = function (vals) {
            if ($state.current.name != 'home.ratingsengine.rulesprospects.segment.attributes.rules.picker') {
                QueryStore.setEntitiesProperty('loading', true);

                var segment = {
                    'free_form_text_search': "",
                    'page_filter': {
                        'num_rows': 10,
                        'row_offset': 0
                    }
                };

                segment['account_restriction'] = angular.copy(QueryStore.accountRestriction);
                segment['contact_restriction'] = angular.copy(QueryStore.contactRestriction);

                QueryStore.getEntitiesCounts(segment).then(function (result) {
                    QueryStore.setResourceTypeCount('accounts', false, result['Account']);
                    QueryStore.setResourceTypeCount('contacts', false, result['Contact']);
                });
            }

            //if (vals && vals.length > 0) {
            vm.controller.updateBucketCount(Segment.name);
            //}
        }

        vm.pushItem = function (item, bucketRestriction, controller) {
            // console.log('pushItem', item, bucketRestriction, controller);
            vm.controller = controller;

            vm.updateCounts();
        }

        vm.go = function (state) {
            $state.go(state);
        }

        vm.getRecordLabel = function () {
            let item = this.item || { Entity: 'Record' };
            return item.Entity + 's';
        }

        vm.init();
    });