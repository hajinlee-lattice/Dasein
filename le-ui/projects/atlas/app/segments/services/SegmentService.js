angular
    .module("lp.segments")
    .service("SegmentStore", function (
        $q, $rootScope, $state, SegmentService,
        PercentStore, QueryTreeDateAttributeStore
    ) {
        var SegmentStore = this;

        this.segments = [];

        this.setSegments = function (segments) {
            this.segments = segments;
        };

        this.getSegments = function () {
            return this.segments;
        };

        this.modalEditSegment = function (config) {
            if (config.action === "cancel") {
                $state.go("home.segments");
            }
            return true;
        };

        this.modalSetTileEditSegment = function (config) {
            $rootScope.$broadcast("tileEditSegment:" + config.action);
            return true; // modal will hide itself
        };

        this.flattenSegmentRestrictions = function (segment) {
            var restrictions = [];
            if (
                segment.account_restriction != null &&
                segment.account_restriction.restriction !== null &&
                segment.account_restriction.restriction.logicalRestriction &&
                segment.contact_restriction != null &&
                segment.contact_restriction.restriction !== null &&
                segment.contact_restriction.restriction.logicalRestriction
            ) {
                segment.account_restriction.restriction.logicalRestriction.restrictions.forEach(
                    function (restriction) {
                        SegmentStore.flattenRestriction(
                            restriction,
                            restrictions
                        );
                    }
                );
                segment.contact_restriction.restriction.logicalRestriction.restrictions.forEach(
                    function (restriction) {
                        SegmentStore.flattenRestriction(
                            restriction,
                            restrictions
                        );
                    }
                );
            }

            return restrictions;
        };

        this.flattenRestriction = function (restriction, array) {
            if (restriction.bucketRestriction) {
                array.push(restriction);
            } else if (restriction.logicalRestriction) {
                restriction.logicalRestriction.restrictions.forEach(function (
                    restriction
                ) {
                    SegmentStore.flattenRestriction(restriction, array);
                });
            }
        };

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

        this.formatAttributes = function (restrictions, cubedata) {
            let attrs = [];
            let enrichments = [];
            let enrichmentsMap = DataCloudStore.getEnrichmentsMap();
            restrictions.forEach(function (restriction) {
                var bucketEntity = restriction.bucketRestriction.attr.split('.')[0],
                    bucketColumnId = restriction.bucketRestriction.attr.split('.')[1],
                    enrichment = enrichments[enrichmentsMap[bucketColumnId]];

                if (enrichment && cubedata[bucketEntity] != undefined) {
                    var cube = cubedata[bucketEntity].Stats[bucketColumnId];

                    if (cube.Bkts) {
                        var operatorType = cube.Bkts.Type;

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
                    vm.invalidSegments.add(segment.name);
                }
            });

            return attrs;
        };

        this.swap = function (array, i, j) {
            var temp = array[i];
            array[i] = array[j];
            array[j] = temp;
        };

        this.getSegmentByName = function (segmentName) {
            var deferred = $q.defer(),
                found = false;

            for (var i = 0; i < this.segments.length; i++) {
                var segment = this.segments[i];

                if (segment.name === segmentName) {
                    deferred.resolve(segment);
                    found = true;

                    break;
                }
            }

            if (!found) {
                SegmentService.GetSegmentByName(segmentName).then(function (
                    result
                ) {
                    deferred.resolve(result ? result : null);
                });
            }

            return deferred.promise;
        };

        this.CreateOrUpdateSegment = function (segment, restriction) {
            var ts = new Date().getTime();

            if (!segment) {
                segment = {
                    name: "segment" + ts,
                    display_name: "segment" + ts,
                    account_restriction: restriction,
                    page_filter: {
                        row_offset: 0,
                        num_rows: 10
                    }
                };
            } else {
                segment = {
                    name: segment.name,
                    display_name: segment.display_name,
                    account_restriction:
                        restriction || segment.account_restriction,
                    page_filter: {
                        row_offset: 0,
                        num_rows: 10
                    }
                };
            }

            // remove Advanced Query Builder-specific temp properties
            this.sanitizeSegment(segment);

            return SegmentService.CreateOrUpdateSegment(segment);
        };

        this.sanitizeSegment = function (segment) {
            var aRestriction = segment.account_restriction
                ? segment.account_restriction.restriction
                : {};

            var cRestriction = segment.contact_restriction
                ? segment.contact_restriction.restriction
                : {};

            this.sanitizeSegmentRestriction([aRestriction]);
            this.sanitizeSegmentRestriction([cRestriction]);

            return segment;
        };

        this.sanitizeRuleBuckets = function (rule, keepEmptyBuckets) {
            var map = rule.ratingRule.bucketToRuleMap,
                prune = [];

            if (!keepEmptyBuckets) {
                Object.keys(map).forEach(function (bucketName) {
                    var account =
                        map[bucketName].account_restriction.logicalRestriction
                            .restrictions;
                    var contact =
                        map[bucketName].contact_restriction.logicalRestriction
                            .restrictions;

                    if (account.length + contact.length == 0) {
                        prune.push(bucketName);
                    }
                });

                for (var i = prune.length - 1; i >= 0; i--) {
                    delete map[prune[i]];
                }
            }

            Object.keys(map).forEach(function (bucketName) {
                var bucket = map[bucketName];

                if (bucket) {
                    if (!keepEmptyBuckets) {
                        SegmentStore.removeEmptyBuckets([
                            bucket.account_restriction
                        ]);
                        SegmentStore.removeEmptyBuckets([
                            bucket.contact_restriction
                        ]);
                    }

                    SegmentStore.sanitizeSegmentRestriction([
                        bucket.account_restriction
                    ]);
                    SegmentStore.sanitizeSegmentRestriction([
                        bucket.contact_restriction
                    ]);
                }
            });

            return rule;
        };

        this.sanitizeSegmentRestriction = function (tree) {
            if (tree && tree.length > 0) {
                tree.forEach(function (branch) {
                    if (branch && typeof branch.labelGlyph !== undefined) {
                        delete branch.labelGlyph;
                    }

                    if (branch && typeof branch.collapsed !== undefined) {
                        delete branch.collapsed;
                    }

                    if (branch && branch.logicalRestriction) {
                        SegmentStore.sanitizeSegmentRestriction(
                            branch.logicalRestriction.restrictions
                        );
                    }
                });
            }

            return tree;
        };

        this.removeEmptyBuckets = function (tree) {
            for (var branch, i = tree.length - 1; i >= 0; i--) {
                branch = tree[i];

                if (
                    branch &&
                    branch.bucketRestriction &&
                    (!branch.bucketRestriction.bkt ||
                        !branch.bucketRestriction.bkt.Id)
                ) {
                    tree.splice(i, 1);
                }

                if (branch && branch.logicalRestriction) {
                    SegmentStore.removeEmptyBuckets(
                        branch.logicalRestriction.restrictions
                    );
                }
            }
        };
        this.modalCallback = function (args) {
            if (args.action === "ok") {
                return true;
            } else if (args.action === "closedForced") {
                return false;
            }
        };
    })
    .service("SegmentService", function ($http, $q, $state) {
        this.GetSegments = function () {
            var deferred = $q.defer(),
                result,
                url = "/pls/datacollection/segments";

            $http({
                method: "GET",
                url: url,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.GetSegmentByName = function (name) {
            var deferred = $q.defer(),
                result,
                url = "/pls/datacollection/segments/" + name;

            $http({
                method: "GET",
                url: url,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.CreateOrUpdateSegment = function (segment) {
            var deferred = $q.defer();

            $http({
                method: "POST",
                url: "/pls/datacollection/segments",
                data: segment,
                headers: {
                    "Content-Type": "application/json"
                    // 'ErrorDisplayMethod': 'none' // segment:demo:fixme
                }
            }).then(
                function onSuccess(response) {
                    var result = {
                        data: response.data,
                        success: true
                    };
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.DeleteSegment = function (segmentName) {
            var deferred = $q.defer(),
                result = {},
                url =
                    "/pls/datacollection/segments/" +
                    segmentName +
                    "/modelAndView";

            $http({
                method: "DELETE",
                url: url,
                headers: {
                    Accept: "application/json",
                    ErrorDisplayCallback: "SegmentStore.modalCallback"
                }
            }).then(
                function onSuccess(response) {
                    var result = {
                        data: response.data,
                        success: true
                    };
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }
                    var result = {
                        data: {},
                        errorMessage:
                            response.data.errorMsg || "unspecified error",
                        success: false
                    };

                    // var errorMsg = response.data.errorMsg || 'unspecified error';
                    deferred.resolve(result);
                }
            );

            return deferred.promise;
        };

        this.GetSegmentExports = function () {
            var deferred = $q.defer(),
                result,
                url = "/pls/datacollection/segments/export";

            $http({
                method: "GET",
                url: url,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.GetSegmentExportByExportId = function (exportID) {
            var deferred = $q.defer(),
                result,
                url = "/pls/datacollection/segments/export/" + exportID;

            $http({
                method: "GET",
                url: url,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.CreateOrUpdateSegmentExport = function (segment) {
            var deferred = $q.defer(),
                result = {},
                url = "/pls/datacollection/segments/export";

            $http({
                method: "POST",
                url: url,
                data: segment,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    var result = {
                        data: response.data,
                        success: true
                    };
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.DeleteExpiredSegmentExports = function () {
            var deferred = $q.defer(),
                result = {},
                url = "/pls/datacollection/segments/export/cleanup";

            $http({
                method: "DELETE",
                url: url,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    var result = {
                        data: response.data,
                        success: true
                    };
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );

            return deferred.promise;
        };

        this.DownloadExportedSegment = function (id) {
            var deferred = $q.defer(),
                result,
                url = "/pls/datacollection/segments/export/" + id + "/download";

            $http({
                method: "GET",
                url: url,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    deferred.resolve(response);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.DownloadExportedOrphans = function (id) {
            var deferred = $q.defer(),
                result,
                url = `/pls/datacollection/orphans/orphanexport/${id}`;

            $http({
                method: "GET",
                url: url,
                headers: {
                    Accept: "application/json"
                }
            }).then(
                function onSuccess(response) {
                    deferred.resolve(response);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };

        this.GetSegmentDependenciesModelView = function (
            segmentId,
            errorDisplayCallback
        ) {
            var deferred = $q.defer(),
                result,
                url =
                    "/pls/datacollection/segments/" +
                    segmentId +
                    "/dependencies/modelAndView";

            $http({
                method: "GET",
                url: url,
                headers: {
                    Accept: "application/json",
                    ErrorDisplayCallback: errorDisplayCallback
                }
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    deferred.resolve(result);
                },
                function onError(response) {
                    if (!response.data) {
                        response.data = {};
                    }

                    var errorMsg =
                        response.data.errorMsg || "unspecified error";
                    deferred.resolve(errorMsg);
                }
            );
            return deferred.promise;
        };
    });
