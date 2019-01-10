angular.module('lp.segments', [
    'common.modal',
    'lp.tile.edit'
])
.controller('SegmentationListController', function ($q, $scope, $rootScope, $element, $state, $stateParams,
    SegmentsList, Enrichments, Cube, Modal, Banner, SegmentStore, SegmentService, RatingsEngineStore, QueryTreeService, 
    DataCloudStore, LookupResponse, LookupStore, PercentStore, AttrConfigStore
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
        editConfig:{
            data: {id: 'name'},
            fields:{
                name: {fieldname: 'display_name', visible: true, maxLength: 50},
                description: {fieldname: 'description', visible: true, maxLength: 1000}
          }
        },
        invalidSegments: new Set()
    });

    vm.init = function() {
        vm.processEnrichments(Enrichments);

        vm.segmentIds = [];
        SegmentsList.forEach(function(segment) {
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
            if(tileState) {
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
    vm.saveNameDescription = function(obj, newData){
        var tileState = vm.tileStates[obj.name];
        if(!newData){
            tileState.editSegment = !tileState.editSegment;
            vm.saveInProgress = false;
            vm.showAddSegmentError = false;
        } else {
            vm.saveInProgress = true;
            obj.display_name = newData.display_name;
            obj.description = newData.description;
            createOrUpdateSegment(obj).then(function(result){
                if(result.success === true){
                    vm.saveInProgress = false;
                }
                
            });
        }
        tileState.showCustomMenu = false;
    }

    vm.onInputFocus = function($event) {
        $event.target.select();
    };

    vm.customMenuClick = function($event, segment) {
        if ($event != null) {
            $event.stopPropagation();
        }
        var tileState = vm.tileStates[segment.name];
        tileState.showCustomMenu = !tileState.showCustomMenu

        if (tileState.showCustomMenu) {
            $(document).bind('click', function(event){
                var isClickedElementChildOfPopup = $element
                    .find(event.target)
                    .length > 0;

                if (isClickedElementChildOfPopup)
                    return;

                $scope.$apply(function(){
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
                $state.go('home.segment.explorer.attributes', {segment: "Create"}, {reload: true});
            } else if (!vm.invalidSegments.has(segment.name)){
                $state.go('home.segment.explorer.builder', {segment: segment.name}, {reload: true});
            }
        } else {
            $state.go('home.model.analysis', {segment: segment.name}, { reload: true } );
        };
    };

    vm.processEnrichments = function(enrichments) {
        if (vm.lookupFiltered !== null) {
            for (var i=0, _enrichments=[]; i<enrichments.length; i++) {
                if (vm.lookupFiltered && vm.lookupFiltered[enrichments[i].ColumnId]) {
                    _enrichments.push(enrichments[i]);
                }
            }
        } else {
            var _enrichments = enrichments;
        }

        for (var i=0, enrichment; i<_enrichments.length; i++) {
            enrichment = _enrichments[i];

            if (!enrichment) {
                continue;
            }

            if (enrichment.IsInternal !== true) {
                enrichment.IsInternal = false;
            }

            vm.enrichmentsMap[enrichment.ColumnId] = i;
            vm.enrichments.push(enrichment);
        }

        DataCloudStore.setEnrichments(vm.enrichments);
        DataCloudStore.setEnrichmentsMap(vm.enrichmentsMap);
    }

    vm.editSegmentClick = function($event, segment){
        $event.stopPropagation();
        var tileState = vm.tileStates[segment.name];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editSegment = !tileState.editSegment;
    }
    
    vm.nameChanged = function(segment) {
        var tileState = vm.tileStates[segment.name];
        if(!segment.display_name || segment.display_name.trim().length == 0){
            tileState.saveEnabled = false;
        }
        else{
            tileState.saveEnabled = !!(segment.display_name.length > 0);
        }
    };

    vm.addSegment = function() {
        if (vm.modelId) {
            $state.go('home.model.analysis');
        } else {
            $state.go('home.segment.explorer.attributes', {segment: 'Create'});
        }
    };

    vm.duplicateSegmentClick = function($event, segment) {
        $event.preventDefault();
        $event.stopPropagation();

        vm.saveInProgress = true;
        segment.name = 'segment' + new Date().getTime();

        createOrUpdateSegment(segment);
    };

    vm.callbackModalWindow = function(args) {
        
        var modal = Modal.get('deleteSegmentWarning');

        if (args.action === 'cancel') {

            // console.log("cancel");            
            Modal.modalRemoveFromDOM(modal, args);

        } else if (args.action === 'ok') {

            var segmentName = vm.segment.name;
            if(modal){
                modal.waiting(true);
            }

            SegmentService.DeleteSegment(segmentName).then(function(result) {
                if (result != null && result.success === true) {
                    
                    Modal.modalRemoveFromDOM(modal, args);

                    $state.go('home.segments', {}, { reload: true } );
                } else {
                    Banner.error({ message: result.errorMessage });
                }
            });
        }
    }

    vm.showDeleteSegmentModalClick = function($event, segment){
        $event.preventDefault();
        $event.stopPropagation();

        vm.segment = segment;

        Modal.warning({
            name: 'deleteSegmentWarning',
            title: "Delete Segment",
            message: "Are you sure you want to delete this segment: " + segment.name + "?",
            confirmtext: "Delete Segment"
        }, vm.callbackModalWindow);
    };

    vm.isValid = function(segmentDisplayName){
        if (!segmentDisplayName || segmentDisplayName.trim().length == 0){
            return false;
        } else {
            return true;
        }
    }

    vm.displayAttributes = function(segment, n) {
        var attrs = [];
        var restrictions = SegmentStore.getTopNAttributes(segment, n);
        
        restrictions = SegmentStore.sortAttributesByCnt(restrictions);

        restrictions.forEach(function(restriction) {
            var bucketEntity = restriction.bucketRestriction.attr.split('.')[0],
                bucketColumnId = restriction.bucketRestriction.attr.split('.')[1],
                enrichment = vm.enrichments[vm.enrichmentsMap[bucketColumnId]];

            if (enrichment && vm.cube[bucketEntity] != undefined) {
                var cube = vm.cube[bucketEntity].Stats[bucketColumnId];
                
                if (cube.Bkts) {
                    var operatorType = cube.Bkts.Type;
                    
                    switch (operatorType) {
                        case 'Enum': 
                            var vals = QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType);
                            if (vals.length > 1) {
                                attrs.push({label: enrichment.DisplayName + ': ', value: vals.length + ' Values Selected'});
                            } else {
                                attrs.push({label: enrichment.DisplayName + ': ', value:  
                                `${vals[0] != undefined ? `${vals[0]}` : `${QueryTreeService.cmpMap[restriction.bucketRestriction.bkt.Cmp]}`}`});
                            }
                            
                            break;

                        case 'Numerical':
                            if (QueryTreeService.two_inputs.indexOf(restriction.bucketRestriction.bkt.Cmp) < 0) {
                                let label = QueryTreeService.numerical_labels[restriction.bucketRestriction.bkt.Cmp];
                                let operation = QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType, 0);
                                attrs.push({
                                    label: enrichment.DisplayName + ': ', 
                                    value: QueryTreeService.numerical_labels[restriction.bucketRestriction.bkt.Cmp] + 
                                    `${operation != undefined ? operation : ''}`});
                            } else {
                                attrs.push({
                                    label: enrichment.DisplayName + ': '
                                    , value: QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType, 0) + 
                                    '-' + QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType, 1)});
                            }
                            
                            break;

                        case 'Boolean': 
                            attrs.push({label: enrichment.DisplayName + ': ', value: QueryTreeService.getOperationValue(restriction.bucketRestriction, operatorType)});
                            
                            break;

                        case 'TimeSeries':
                            var value = QueryTreeService.getOperationValue(restriction.bucketRestriction, 'Boolean') ? 'True' : 'False';
                            attrs.push({label: enrichment.DisplayName + ' (' + enrichment.Subcategory +  '): ', value: value});
                            
                            break;
                        case 'PercentChange':
                                var value = PercentStore.getDirectionRedable(restriction.bucketRestriction) + ' ' + PercentStore.getCmpRedable(restriction.bucketRestriction).toLowerCase()
                                    + ' ' + PercentStore.getValuesFormatted(restriction.bucketRestriction);
                            attrs.push({label: enrichment.DisplayName + ': ', value: value});

                            break;
                    }
                } else {
                    // for pure string attributes
                    var value = QueryTreeService.getOperationLabel('String', restriction.bucketRestriction);
                    if (QueryTreeService.hasInputs('String', restriction.bucketRestriction)) {
                        value += " '" + QueryTreeService.getOperationValue(restriction.bucketRestriction, 'String') + "'";
                    }
                    attrs.push({label: enrichment.DisplayName + ': ', value: value});
                }
            } else {
                vm.invalidSegments.add(segment.name);
            }
        });

        return attrs;
    };

    function createOrUpdateSegment(segment) {
        var deferred = $q.defer();
        SegmentService.CreateOrUpdateSegment(segment).then(function(result) {
            var errorMsg = result.errorMsg;
            
            if (result.success) {
                var tileState = vm.tileStates[segment.name];
                
                if(tileState){
                    tileState.editSegment = !tileState.editSegment;
                    $state.go('home.segments', {edit: null}, {reload: false } );
                    deferred.resolve({success: true});
                } else {
                    $state.go('home.segments', {}, { reload: true } );
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
