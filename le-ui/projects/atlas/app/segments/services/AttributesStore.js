let AttributesStore = function (
    SegmentStore, PercentStore, DataCloudStore,
    QueryTreeService, QueryTreeDateAttributeStore
) {
    'ngInject';
    let AttributesStore = this;

    this.getTopNAttributes = function (segment, n) {
        var restrictions = SegmentStore.flattenSegmentRestrictions(segment);

        if (n > restrictions.length) {
            return restrictions;
        }
        var minIndex = 0,
            i;

        for (i = n; i < restrictions.length; i++) {
            minIndex = 0;
            for (var j = 0; j < n; j++) {
                if (
                    restrictions[minIndex].bucketRestriction.bkt.Cnt >
                    restrictions[j].bucketRestriction.bkt.Cnt
                ) {
                    minIndex = j;
                    restrictions[minIndex] = restrictions[j];
                }
            }
            if (
                restrictions[minIndex].bucketRestriction.bkt.Cnt <
                restrictions[i].bucketRestriction.bkt.Cnt
            ) {
                SegmentStore.swap(restrictions, minIndex, i);
            }
        }

        var result = restrictions.splice(0, n); // unsorted list of top n attributes by volume
        return result;
    };

    this.sortAttributesByCnt = function (restrictions) {
        var counts = restrictions.map(function (restriction, idx) {
            return {
                index: idx,
                count: restriction.bucketRestriction.bkt.Cnt
            };
        });
        counts.sort(function (a, b) {
            return b.count - a.count;
        });
        restrictions = counts.map(function (restriction) {
            return restrictions[restriction.index];
        });
        return restrictions;
    };

    this.getEnrichmentsMap = function (enrichments) {
        let map = {};

        enrichments = enrichments || [];

        for (var i = 0, enrichment; i < enrichments.length; i++) {
            enrichment = enrichments[i];
            if (!enrichment) {
                continue;
            }

            map[enrichment.ColumnId] = i;
        }

        return map;
    }

    this.formatAttributes = function (restrictions, cubedata, enrichments) {
        let attrs = [];
        let enrichmentsMap = this.getEnrichmentsMap(enrichments);

        //console.log('AttributesStore', enrichmentsMap);
        if (!enrichments || enrichments.length == 0) {
            return false;
        }

        restrictions.forEach(function (restriction) {
            var bucketEntity = restriction.bucketRestriction.attr.split('.')[0],
                bucketColumnId = restriction.bucketRestriction.attr.split('.')[1],
                enrichment = enrichments[enrichmentsMap[bucketColumnId]];

            if (enrichment && cubedata[bucketEntity] != undefined) {
                var cube = cubedata[bucketEntity].Stats[bucketColumnId];

                if (cube.Bkts) {
                    var operatorType = cube.Bkts.Type;
                    //console.log(operatorType)
                    switch (operatorType) {
                        case 'Enum':
                            var vals = QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType);
                            if (vals.length > 1) {
                                attrs.push({ label: enrichment.DisplayName + ': ', value: vals.length + ' Values Selected' });
                            } else {
                                attrs.push({
                                    label: enrichment.DisplayName + ': ', value:
                                        `${vals[0] != undefined ? `${vals[0]}` : `${QueryTreeService.cmpMap[restriction.bucketRestriction.bkt.Cmp]}`}`
                                });
                            }

                            break;

                        case 'Numerical':
                            if (QueryTreeService.two_inputs.indexOf(restriction.bucketRestriction.bkt.Cmp) < 0) {
                                let label = QueryTreeService.numerical_labels[restriction.bucketRestriction.bkt.Cmp];
                                let operation = QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType, 0);
                                attrs.push({
                                    label: enrichment.DisplayName + ': ',
                                    value: QueryTreeService.numerical_labels[restriction.bucketRestriction.bkt.Cmp] +
                                        `${operation != undefined ? operation : ''}`
                                });
                            } else {
                                attrs.push({
                                    label: enrichment.DisplayName + ': '
                                    , value: QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType, 0) +
                                        '-' + QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType, 1)
                                });
                            }

                            break;

                        case 'Boolean':
                            let cmp = restriction.bucketRestriction.bkt.Cmp;
                            let val = QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType);

                            if (!val) {
                                switch (cmp) {
                                    case "IS_NULL": val = 'Is Empty'; break;
                                    case "IS_NOT_NULL": val = 'Is Present'; break;
                                }
                            }

                            attrs.push({ label: enrichment.DisplayName + ': ', value: val });

                            break;

                        case 'TimeSeries':
                            var value = QueryTreeService.getOperationValue(restriction.bucketRestriction, 'Boolean') ? 'True' : 'False';
                            attrs.push({ label: enrichment.DisplayName + ' (' + enrichment.Subcategory + '): ', value: value });

                            break;
                        case 'PercentChange':
                            var value = PercentStore.getDirectionRedable(restriction.bucketRestriction) + ' ' + PercentStore.getCmpRedable(restriction.bucketRestriction).toLowerCase()
                                + ' ' + PercentStore.getValuesFormatted(restriction.bucketRestriction);
                            attrs.push({ label: enrichment.DisplayName + ': ', value: value });

                            break;
                        case 'Date':
                            var ret = QueryTreeDateAttributeStore.getCmpValueReadable(enrichment.DisplayName, restriction.bucketRestriction.bkt);
                            attrs.push(ret);
                            break;
                    }
                } else {
                    // for pure string attributes
                    var value = QueryTreeService.getOperationLabel('String', restriction.bucketRestriction);
                    if (QueryTreeService.hasInputs('String', restriction.bucketRestriction)) {
                        value += " '" + QueryTreeService.getOperationValue(restriction.bucketRestriction, 'String') + "'";
                    }
                    attrs.push({ label: enrichment.DisplayName + ': ', value: value });
                }
            } else {
                //vm.invalidSegments.add(segment.name);
                return 'invalid';
            }
        });

        return attrs;
    };
};

angular.module("lp.segments").service("AttributesStore", AttributesStore);