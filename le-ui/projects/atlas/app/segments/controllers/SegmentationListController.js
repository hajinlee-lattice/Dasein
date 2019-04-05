angular.module('lp.segments', [
    'common.modal',
    'lp.tile.edit',
    'le.connectors'
])
    .controller('SegmentationListController', function ($q, $scope, $rootScope, $element, $state, $stateParams,
        SegmentsList, Enrichments, Cube, Modal, Banner, SegmentStore, SegmentService, RatingsEngineStore, QueryTreeService,
        DataCloudStore, LookupResponse, LookupStore, PercentStore, QueryTreeDateAttributeStore, AttributesStore
    ) {
        var vm = this;

        angular.extend(vm, {
            modelId: $stateParams.modelId,
            tenantName: $stateParams.tenantName,
            segments: SegmentsList || [],
            enrichments: [],
            enrichmentsMap: DataCloudStore.getEnrichmentsMap(),
            segmentAttributesMap: {},
            cube: Cube,
            //count: QueryStore.getCounts(),
            filteredItems: [],
            totalLength: SegmentsList.length,
            tileStates: {},
            query: '',
            currentPage: 1,
            lookupMode: (LookupResponse && LookupResponse.attributes !== null),
            lookupFiltered: LookupResponse.attributes,
            LookupResponse: LookupStore.response,
            header: {
                sort: {
                    label: 'Sort By',
                    icon: 'numeric',
                    order: '-',
                    property: 'updated',
                    items: [
                        { label: 'Creation Date', icon: 'numeric', property: 'created' },
                        { label: 'Modified Date', icon: 'numeric', property: 'updated' },
                        { label: 'Author Name', icon: 'alpha', property: 'created_by' },
                        { label: 'Segment Name', icon: 'alpha', property: 'display_name' }
                    ]
                }
            },
            editConfig: {
                data: { id: 'name' },
                fields: {
                    name: { fieldname: 'display_name', visible: true, maxLength: 50 },
                    description: { fieldname: 'description', visible: true, maxLength: 1000 }
                }
            },
            invalidSegments: new Set()
        });

        vm.init = function () {
            vm.processEnrichments(Enrichments);

            vm.segmentIds = [];
            SegmentsList.forEach(function (segment) {
                vm.tileStates[segment.name] = {
                    showCustomMenu: false,
                    editSegment: false,
                    saveEnabled: false
                };
                vm.segmentIds.push(segment.name);
                vm.segmentAttributesMap[segment.name] = vm.displayAttributes(segment, 5);
            });
            // RatingsEngineStore.getSegmentsCounts(vm.segmentIds).then(function(response){
            //     console.log(response);
            // });

            if ($stateParams.edit && vm.isValid($stateParams.edit)) {
                var tileState = vm.tileStates[$stateParams.edit];
                if (tileState) {
                    tileState.editSegment = !tileState.editSegment;
                    tileState.saveEnabled = true;
                    // $stateParams.edit = null; TODO: Why are we updating this?
                }
            }
        }

        /**
         * 
         * @param {*} obj segment object
         * @param {*} newData object that containes the update values
         */
        vm.saveNameDescription = function (obj, newData) {
            var tileState = vm.tileStates[obj.name];
            if (!newData) {
                tileState.editSegment = !tileState.editSegment;
                vm.saveInProgress = false;
                vm.showAddSegmentError = false;
            } else {
                vm.saveInProgress = true;
                obj.display_name = newData.display_name;
                obj.description = newData.description;
                createOrUpdateSegment(obj).then(function (result) {
                    if (result.success === true) {
                        vm.saveInProgress = false;
                    }

                });
            }
            tileState.showCustomMenu = false;
        }

        vm.onInputFocus = function ($event) {
            $event.target.select();
        };

        vm.customMenuClick = function ($event, segment) {
            if ($event != null) {
                $event.stopPropagation();
            }
            var tileState = vm.tileStates[segment.name];
            tileState.showCustomMenu = !tileState.showCustomMenu

            if (tileState.showCustomMenu) {
                $(document).bind('click', function (event) {
                    var isClickedElementChildOfPopup = $element
                        .find(event.target)
                        .length > 0;

                    if (isClickedElementChildOfPopup)
                        return;

                    $scope.$apply(function () {
                        tileState.showCustomMenu = false;
                        $(document).unbind(event);
                    });
                });
            }
        };

        vm.tileClick = function ($event, segment) {
            $event.preventDefault();
            if ($state.current.name == 'home.segments') {
                // $state.go('home.segment.accounts', {segment: segment.name}, { reload: true } );
                if (segment.is_master_segment) {
                    $state.go('home.segment.explorer.attributes', { segment: "Create" }, { reload: true });
                } else if (!vm.invalidSegments.has(segment.name)) {
                    $state.go('home.segment.explorer.builder', { segment: segment.name }, { reload: true });
                }
            } else {
                $state.go('home.model.analysis', { segment: segment.name }, { reload: true });
            };
        };

        vm.processEnrichments = function (enrichments) {
            if (vm.lookupFiltered !== null) {
                for (var i = 0, _enrichments = []; i < enrichments.length; i++) {
                    if (vm.lookupFiltered && vm.lookupFiltered[enrichments[i].ColumnId]) {
                        _enrichments.push(enrichments[i]);
                    }
                }
            } else {
                var _enrichments = enrichments;
            }

            let enrichmentsMap = AttributesStore.getEnrichmentsMap(vm.enrichments)

            DataCloudStore.setEnrichments(vm.enrichments);
            DataCloudStore.setEnrichmentsMap(enrichmentsMap);
        }

        vm.editSegmentClick = function ($event, segment) {
            $event.stopPropagation();
            var tileState = vm.tileStates[segment.name];
            tileState.showCustomMenu = !tileState.showCustomMenu;
            tileState.editSegment = !tileState.editSegment;
        }

        vm.nameChanged = function (segment) {
            var tileState = vm.tileStates[segment.name];
            if (!segment.display_name || segment.display_name.trim().length == 0) {
                tileState.saveEnabled = false;
            }
            else {
                tileState.saveEnabled = !!(segment.display_name.length > 0);
            }
        };

        vm.addSegment = function () {
            if (vm.modelId) {
                $state.go('home.model.analysis');
            } else {
                $state.go('home.segment.explorer.attributes', { segment: 'Create' });
            }
        };

        vm.duplicateSegmentClick = function ($event, segment) {
            $event.preventDefault();
            $event.stopPropagation();

            vm.saveInProgress = true;
            segment.name = 'segment' + new Date().getTime();

            createOrUpdateSegment(segment);
        };

        vm.callbackModalWindow = function (args) {

            var modal = Modal.get('deleteSegmentWarning');

            if (args.action === 'cancel') {

                // console.log("cancel");            
                Modal.modalRemoveFromDOM(modal, args);

            } else if (args.action === 'ok') {

                var segmentName = vm.segment.name;
                if (modal) {
                    modal.waiting(true);
                }

                SegmentService.DeleteSegment(segmentName).then(function (result) {
                    if (result != null && result.success === true) {

                        Modal.modalRemoveFromDOM(modal, args);

                        $state.go('home.segments', {}, { reload: true });
                    } else {
                        Banner.error({ message: result.errorMessage });
                    }
                });
            }
        }

        vm.showDeleteSegmentModalClick = function ($event, segment) {
            $event.preventDefault();
            $event.stopPropagation();

            vm.segment = segment;

            Modal.warning({
                name: 'deleteSegmentWarning',
                title: "Delete Segment",
                message: `Are you sure you want to delete this segment: <strong>${segment.display_name}</strong>?`,
                confirmtext: "Delete Segment"
            }, vm.callbackModalWindow);
        };

        vm.isValid = function (segmentDisplayName) {
            if (!segmentDisplayName || segmentDisplayName.trim().length == 0) {
                return false;
            } else {
                return true;
            }
        }

        vm.displayAttributes = function (segment, n) {
            var restrictions = SegmentStore.getTopNAttributes(segment, n);

            restrictions = SegmentStore.sortAttributesByCnt(restrictions);

            let attrs = AttributesStore.formatAttributes(restrictions, vm.enrichments, vm.cube)

            return attrs;
        };


        function createOrUpdateSegment(segment) {
            var deferred = $q.defer();
            SegmentService.CreateOrUpdateSegment(segment).then(function (result) {
                var errorMsg = result.errorMsg;

                if (result.success) {
                    var tileState = vm.tileStates[segment.name];

                    if (tileState) {
                        tileState.editSegment = !tileState.editSegment;
                        $state.go('home.segments', { edit: null }, { reload: false });
                        deferred.resolve({ success: true });
                    } else {
                        $state.go('home.segments', {}, { reload: true });
                    }

                    vm.saveInProgress = false;
                    vm.showAddSegmentError = false;
                } else {
                    vm.saveInProgress = false;
                    vm.addSegmentErrorMessage = errorMsg;
                    vm.showAddSegmentError = true;
                }
            });
            return deferred.promise;
        }

        vm.init();
    });
