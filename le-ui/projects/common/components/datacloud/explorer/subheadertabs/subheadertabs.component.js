angular.module('common.datacloud.explorer.subheadertabs', [])
.controller('SubHeaderTabsController', function(
    $state, $stateParams, $timeout, FeatureFlagService, DataCloudStore, QueryStore, 
    SegmentService, SegmentStore, HealthService
) {
    var vm = this,
        flags = FeatureFlagService.Flags();
    // vm.showExportDropdown = false;
    vm.displayExportBanner = false;
    angular.extend(vm, {
        stateParams: $stateParams,
        segment: $stateParams.segment,
        section: $stateParams.section,
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS),
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
        }
    });

    vm.init = function() {
        QueryStore.setPublicProperty('enableSaveSegmentButton', false);
        this.header.exportSegment.items = [
            {
                label: 'Accounts',
                icon: 'fa fa-building-o',
                click: checkStatusBeforeExport.bind(null, 'ACCOUNT')
            },{
                label: 'Contacts',
                icon: 'fa fa-users',
                click: checkStatusBeforeExport.bind(null, 'CONTACT')
            },{
                label: 'Accounts and Contacts',
                icon: 'fa fa-briefcase',
                click: checkStatusBeforeExport.bind(null, 'ACCOUNT_AND_CONTACT')
            }
        ];
    }

    vm.checkState = function(type) {
        var state = $state.current.name;

        var map = {
            'home.model.analysis.explorer.builder':'builder',
            'home.model.analysis.explorer.attributes':'attributes',
            'home.model.analysis.accounts':'accounts',
            'home.model.analysis.contacts':'contacts',
            'home.segment.explorer.attributes':'attributes',
            'home.segment.explorer.builder':'builder',
            'home.segment.accounts':'accounts',
            'home.segment.contacts':'contacts'
        };

        return map[state] == type;
    }

    vm.clickBuilder = function() {
        var state = vm.ifInModel('home.model.analysis.explorer.builder', 'home.segment.explorer.builder');

        vm.builderClicked = true;
        vm.attribuesClicked = false;

        $timeout(function() {
            $state.go(state, $stateParams);
        },1);
    }

    vm.clickAttributes = function() {
        var state = vm.ifInModel('home.model.analysis.explorer.attributes', 'home.segment.explorer.attributes');

        vm.builderClicked = false;
        vm.attribuesClicked = true;

        $timeout(function() {
            $state.go(state, $stateParams);
        },1);
    }

    vm.clickSegmentButton = function(parms) {
        var state = vm.ifInModel('home.model.segmentation', 'home.segments');
        var opts = parms ? {} : { notify: true };
        
        $state.go(state, parms, opts);
    }

    vm.clearSegment = function() {
        alert('no worky yet')
        //QueryStore.resetRestrictions();
    }

    vm.saveSegment = function() {
        var segmentName = $stateParams.segment,
            isNewSegment = segmentName === 'Create',
            accountRestriction = QueryStore.getAccountRestriction(),
            contactRestriction = QueryStore.getContactRestriction(),
            ts = new Date().getTime();

        var xhrSaveSegment = function(segmentData) {
            var name = isNewSegment 
                ? 'segment' + ts 
                : segmentData.name;

            var displayName = isNewSegment 
                ? 'segment' + ts 
                : segmentData.display_name;

            var segment = SegmentStore.sanitizeSegment({
                name: name,
                display_name: displayName,
                account_restriction: angular.copy(accountRestriction),
                contact_restriction: angular.copy(contactRestriction),
                page_filter: {
                    row_offset: 0,
                    num_rows: 10
                }
            });
            QueryStore.setPublicProperty('enableSaveSegmentButton', false);
            vm.isSaving = true;
            SegmentService.CreateOrUpdateSegment(segment).then(function(result) {
                
                if (isNewSegment) { 
                    vm.clickSegmentButton({
                        edit: segment.name
                    });
                }else{
                    vm.enableSaveSegmentMsg = true;
                    $timeout(function() {
                        vm.enableSaveSegmentMsg = false;
                    },3500);
                }

                vm.saved = true;
                vm.isSaving = false;
            });
        }

        QueryStore.setPublicProperty('enableSaveSegmentButton', false);
        
        var xhrGetSegmentResult = function(result) {
            xhrSaveSegment(result);
        }

        isNewSegment
            ? xhrSaveSegment()
            : SegmentStore.getSegmentByName(segmentName).then(xhrGetSegmentResult);
    }

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    }

    vm.ifInModel = function(model, not) {
        return vm.inModel() ? model : not;
    }

    vm.exportSegment = function(exportType) {
        var segmentName = $stateParams.segment,
            ts = new Date().getTime();
        // console.log('export type', exportType);
        QueryStore.setPublicProperty('resetLabelIncrementor', true);

        if (segmentName === 'Create') {
            var accountRestriction = QueryStore.getAccountRestriction(),
                contactRestriction = QueryStore.getContactRestriction(),
                segmentExport = SegmentStore.sanitizeSegment({
                    'account_restriction': accountRestriction,
                    'contact_restriction': contactRestriction,
                    'type': exportType
                });

            console.log('saveMetadataSegmentExport new', segmentName, ts, segmentExport);

            SegmentService.CreateOrUpdateSegmentExport(segmentExport).then(function(result) {
                console.log(result);
                if (result.success) { 
                    vm.displayExportBanner = true;
                }
                vm.toggleExportDropdown(false);
            });
        } 
        else {
            SegmentStore.getSegmentByName(segmentName).then(function(result) {
                var segmentData = result,
                    accountRestriction = QueryStore.getAccountRestriction(),
                    contactRestriction = QueryStore.getContactRestriction(),
                    segmentExport = SegmentStore.sanitizeSegment({
                        'export_prefix': segmentData.display_name, 
                        'account_restriction': accountRestriction,
                        'contact_restriction': contactRestriction,
                        'type': exportType
                    });
                console.log('saveSegment existing', segmentData, segmentExport);

                SegmentService.CreateOrUpdateSegmentExport(segmentExport).then(function(result) {
                    console.log(result);
                    if (result.success) { 
                        vm.displayExportBanner = true;
                    }
                    vm.toggleExportDropdown(false);

                });
            });
        };
    };


    // vm.toggleExportDropdown = function($event) {
    //     if ($event != null) {
    //         $event.stopPropagation();
    //     }
    //     vm.showExportDropdown = !vm.showExportDropdown;
    // }

    vm.hideExportBanner = function() {
        vm.displayExportBanner = false;
    }

    vm.toggleExportDropdown = function(bool) {
        vm.header.exportSegment.icondisabled = bool;
        vm.header.exportSegment.showSpinner = bool;
    }

    function checkStatusBeforeExport(exportType, $event) {
        $event.preventDefault();

        HealthService.checkSystemStatus().then(function() {
            vm.toggleExportDropdown(true); //disable dropdown
            vm.exportSegment(exportType);
        });
    }

    vm.init();
});