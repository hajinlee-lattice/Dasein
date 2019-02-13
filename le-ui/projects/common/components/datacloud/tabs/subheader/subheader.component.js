angular
    .module('common.datacloud.tabs.subheader', [])
<<<<<<< HEAD
    .controller('SubHeaderTabsController', function (
        $state, $rootScope, $stateParams, $timeout, StateHistory,
        FeatureFlagService, DataCloudStore, QueryStore, SegmentService,
        SegmentStore, HealthService, QueryTreeService, ModelStore,
        TopPredictorService, RatingsEngineStore, Banner
=======
    .controller('SubHeaderTabsController', function(
        $state,
        $rootScope,
        $stateParams,
        $timeout,
        FeatureFlagService,
        DataCloudStore,
        QueryStore,
        SegmentService,
        SegmentStore,
        HealthService,
        QueryTreeService,
        StateHistory
>>>>>>> parent of 72e22223db... - View Remodel changes
    ) {
        var vm = this,
            flags = FeatureFlagService.Flags();
        // vm.showExportDropdown = false;
        vm.displayExportBanner = false;
        angular.extend(vm, {
            stateParams: $stateParams,
            segment: $stateParams.segment,
            section: $stateParams.section,
            show_lattice_insights: FeatureFlagService.FlagIsEnabled(
                flags.LATTICE_INSIGHTS
            ),
            public: QueryStore.getPublic(),
            builderClicked: false,
            attribuesClicked: false,
            isSaving: false,
            enableSaveSegmentMsg: false,
            header: {
                exportSegment: {
                    class: 'white-button select-label',
                    click: false,
                    icon: 'fa fa-chevron-down',
                    iconlabel: 'Export',
                    iconclass: 'save button white-button select-more',
                    iconrotate: true,
                    icondisabled: false,
                    showSpinner: false
                }
            },
            counts: QueryStore.getCounts()
        });

        vm.init = function() {
            QueryStore.setPublicProperty('enableSaveSegmentButton', false);
            this.header.exportSegment.items = [
                {
                    label: 'Enriched Accounts',
                    icon: 'fa fa-building-o',
                    click: checkStatusBeforeExport.bind(null, 'ACCOUNT'),
                    disabledif:
                        !QueryStore.counts.accounts.loading &&
                        !QueryStore.counts.accounts.value
                },
                {
                    label: 'Enriched Contacts with Account Attributes',
                    icon: 'fa fa-briefcase',
                    click: checkStatusBeforeExport.bind(
                        null,
                        'ACCOUNT_AND_CONTACT'
                    ),
                    disabledif:
                        !QueryStore.counts.accounts.loading &&
                        !QueryStore.counts.accounts.loading &&
                        (QueryStore.counts.accounts.value == 0 ||
                            QueryStore.counts.contacts.value == 0)
                },
                {
                    label: 'Enriched Contacts (No Account Attributes)',
                    icon: 'fa fa-users',
                    click: checkStatusBeforeExport.bind(null, 'CONTACT'),
                    disabledif:
                        !QueryStore.counts.contacts.loading &&
                        !QueryStore.counts.contacts.value
                }
            ];
        };

        vm.getPickerItem = function() {
            return QueryTreeService.getPickerObject();
        };

<<<<<<< HEAD
        vm.getIterationFilterNumber = function (type) {
            switch (type) {
                case 'all':
                    return vm.enrichments.length;
                case 'used':
                    return vm.enrichments.filter((item) => {
                        return typeof item.ImportanceOrdering != 'undefined';
                    }).length;
                case 'warnings':
                    return vm.enrichments.filter((item) => {
                        return item.HasWarnings;
                    }).length;
                case 'disabled':
                    return vm.enrichments.filter((item) => {
                        return item.ApprovedUsage[0] == 'None';
                    }).length;
            };

            return 0;
        }

        vm.clickIterationFilter = function (type) {
            DataCloudStore.ratingIterationFilter = type;
        }

        vm.checkIterationFilter = function (type) {
            return DataCloudStore.ratingIterationFilter == type;
        }

        vm.checkState = function (type) {
=======
        vm.checkState = function(type) {
>>>>>>> parent of 72e22223db... - View Remodel changes
            var state = $state.current.name;

            var map = {
                'home.segment.explorer.attributes': 'attributes',
                'home.segment.explorer.builder': 'builder',
                'home.segment.explorer.enumpicker': 'picker',
                'home.segment.accounts': 'accounts',
                'home.segment.contacts': 'contacts',
                'home.model.datacloud': 'model_iteration'
            };

            return map[state] == type;
        };

        vm.clickBuilder = function() {
            var state = vm.ifInModel(
                'home.model.analysis.explorer.builder',
                'home.segment.explorer.builder'
            );

            vm.builderClicked = true;
            vm.attribuesClicked = false;

            $timeout(function() {
                $state.go(state, $stateParams);
            }, 1);
        };

        vm.clickAttributes = function() {
            var state = vm.ifInModel(
                'home.model.analysis.explorer.attributes',
                'home.segment.explorer.attributes'
            );

            vm.builderClicked = false;
            vm.attribuesClicked = true;

            $timeout(function() {
                $state.go(state, $stateParams);
            }, 1);
        };

<<<<<<< HEAD
        vm.clickedExport = function () {
            var data = ModelStore.data;
            var csvRows = TopPredictorService.GetTopPredictorExport(data);
            var lineArray = [];

            csvRows.forEach(function (infoArray, index) {
                var line = infoArray.join(",");
                lineArray.push(line);
            });

            var csvContent = lineArray.join("\n");
            var element = document.createElement("a");

            element.setAttribute(
                "href",
                "data:text/csv;charset=utf-8," + encodeURIComponent(csvContent)
            );
            element.setAttribute("download", "attributes.csv");
            element.style.display = "none";

            document.body.appendChild(element);
            element.click();
            document.body.removeChild(element);

        };

        vm.clickPickerBack = function () {
=======
        vm.clickPickerBack = function() {
>>>>>>> parent of 72e22223db... - View Remodel changes
            var state = StateHistory.lastFrom();
            var params = StateHistory.lastFromParams();

            $state.go(state.name, params);
        };

        vm.clickSegmentButton = function(parms) {
            var state = vm.ifInModel(
                'home.model.segmentation',
                'home.segments'
            );
            var opts = parms ? {} : { notify: true };

            $state.go(state, parms, opts);
        };

        vm.clearSegment = function() {
            QueryStore.resetRestrictions();
            QueryStore.setPublicProperty('enableSaveSegmentButton', false);
            $rootScope.$broadcast('clearSegment');
        };

        vm.saveSegment = function() {
            var segmentName = $stateParams.segment,
                isNewSegment = segmentName === 'Create',
                accountRestriction = QueryStore.getAccountRestriction(),
                contactRestriction = QueryStore.getContactRestriction(),
                ts = new Date().getTime();

            var xhrSaveSegment = function(segmentData) {
                console.log(segmentData);

                var name = isNewSegment ? 'segment' + ts : segmentData.name;

                var displayName = isNewSegment
                    ? 'segment' + ts
                    : segmentData.display_name;

                var description = isNewSegment ? null : segmentData.description;

                var segment = SegmentStore.sanitizeSegment({
                    name: name,
                    display_name: displayName,
                    description: description,
                    account_restriction: angular.copy(accountRestriction),
                    contact_restriction: angular.copy(contactRestriction),
                    page_filter: {
                        row_offset: 0,
                        num_rows: 10
                    }
                });
                QueryStore.setPublicProperty('enableSaveSegmentButton', false);
                vm.isSaving = true;
                SegmentService.CreateOrUpdateSegment(segment).then(function(
                    result
                ) {
                    if (isNewSegment) {
                        vm.clickSegmentButton({
                            edit: segment.name
                        });
                    } else {
                        vm.enableSaveSegmentMsg = true;
                        $timeout(function() {
                            vm.enableSaveSegmentMsg = false;
                        }, 3500);
                    }

                    vm.saved = true;
                    vm.isSaving = false;
                });
            };

            QueryStore.setPublicProperty('enableSaveSegmentButton', false);

            var xhrGetSegmentResult = function(result) {
                xhrSaveSegment(result);
            };

            isNewSegment
                ? xhrSaveSegment()
                : SegmentStore.getSegmentByName(segmentName).then(
                      xhrGetSegmentResult
                  );
        };

<<<<<<< HEAD
        vm.remodel = function () {

            console.log($stateParams);

            var engineId = $stateParams.rating_id,
                iteration = RatingsEngineStore.getRemodelIteration(),
                iterationId = iteration.id;

            RatingsEngineStore.getRating(engineId).then(function (engine) {
                RatingsEngineStore.setRatingEngine(engine);
            });

            vm.remodelingProgress = true;

            RatingsEngineStore.getRatingModel(engineId, iterationId).then(function (result) {
                RatingsEngineStore.setRemodelIteration(result);
                RatingsEngineStore.saveIteration('attributes').then(function (result) {
                    if (!result.result) {
                        Banner.success({
                            message:
                                "A remodel job has started. You can track it's progress on the jobs page."
                        });
                    }
                    vm.remodelingProgress = result.showProgress;
                });
            });
        }

        vm.changeSettings = function () {
            var iteration = RatingsEngineStore.getRemodelIteration(),
                modelId = iteration.modelSummaryId,
                rating_id = $stateParams.rating_id,
                url = 'home.ratingsengine.dashboard.training';

            $state.go(url, {
                rating_id: rating_id,
                modelId: modelId
            }, { reload: true });
        }

        vm.inModel = function () {
=======
        vm.inModel = function() {
>>>>>>> parent of 72e22223db... - View Remodel changes
            var name = $state.current.name.split('.');
            return name[1] == 'model';
        };

        vm.ifInModel = function(model, not) {
            return vm.inModel() ? model : not;
        };

        vm.exportSegment = function(exportType) {
            var segmentName = $stateParams.segment,
                ts = new Date().getTime();
            // console.log('export type', exportType);
            QueryStore.setPublicProperty('resetLabelIncrementor', true);

            if (segmentName === 'Create') {
                var accountRestriction = QueryStore.getAccountRestriction(),
                    contactRestriction = QueryStore.getContactRestriction(),
                    segmentExport = SegmentStore.sanitizeSegment({
                        account_restriction: accountRestriction,
                        contact_restriction: contactRestriction,
                        type: exportType
                    });

                console.log(
                    'saveMetadataSegmentExport new',
                    segmentName,
                    ts,
                    segmentExport
                );

                SegmentService.CreateOrUpdateSegmentExport(segmentExport).then(
                    function(result) {
                        console.log(result);
                        if (result.success) {
                            vm.displayExportBanner = true;
                        }
                        vm.toggleExportDropdown(false);
                    }
                );
            } else {
                SegmentStore.getSegmentByName(segmentName).then(function(
                    result
                ) {
                    var segmentData = result,
                        accountRestriction = QueryStore.getAccountRestriction(),
                        contactRestriction = QueryStore.getContactRestriction(),
                        segmentExport = SegmentStore.sanitizeSegment({
                            export_prefix: segmentData.display_name,
                            account_restriction: accountRestriction,
                            contact_restriction: contactRestriction,
                            type: exportType
                        });
                    console.log(
                        'saveSegment existing',
                        segmentData,
                        segmentExport
                    );

                    SegmentService.CreateOrUpdateSegmentExport(
                        segmentExport
<<<<<<< HEAD
                    ).then(function (result) {
=======
                    ).then(function(result) {
                        console.log(result);
>>>>>>> parent of 72e22223db... - View Remodel changes
                        if (result.success) {
                            vm.displayExportBanner = true;
                        }
                        vm.toggleExportDropdown(false);
                    });
                });
            }
        };

        // vm.toggleExportDropdown = function($event) {
        //     if ($event != null) {
        //         $event.stopPropagation();
        //     }
        //     vm.showExportDropdown = !vm.showExportDropdown;
        // }

        vm.hideExportBanner = function() {
            vm.displayExportBanner = false;
        };

        vm.toggleExportDropdown = function(bool) {
            vm.header.exportSegment.icondisabled = bool;
            vm.header.exportSegment.showSpinner = bool;
        };

        vm.disableExport = function() {
            var accountsAvailable = vm.counts.accounts.value;
            var contactsAvailable = vm.counts.contacts.value;
            vm.header.exportSegment.items[0].disabledif = !accountsAvailable;
            vm.header.exportSegment.items[1].disabledif = !contactsAvailable;
            vm.header.exportSegment.items[2].disabledif =
                !accountsAvailable || !contactsAvailable;
        };

        function checkStatusBeforeExport(exportType, $event) {
            $event.preventDefault();

            HealthService.checkSystemStatus().then(function() {
                vm.toggleExportDropdown(true); //disable dropdown
                vm.exportSegment(exportType);
            });
        }

        vm.init();
    });
