angular.module('common.datacloud.explorer.subheadertabs', [])
.controller('SubHeaderTabsController', function(
    $state, $stateParams, $timeout, FeatureFlagService, DataCloudStore, QueryStore, 
    SegmentService, SegmentStore
) {
    var vm = this,
        flags = FeatureFlagService.Flags();
    vm.showExportDropdown = false;
    vm.displayExportBanner = false;
    angular.extend(vm, {
        stateParams: $stateParams,
        segment: $stateParams.segment,
        section: $stateParams.section,
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS),
        public: QueryStore.getPublic(),
        builderClicked: false,
        attribuesClicked: false,
        isSaving: false
    });

    vm.init = function() {
        QueryStore.setPublicProperty('enableSaveSegmentButton', false);
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
                account_restriction: accountRestriction,
                contact_restriction: contactRestriction,
                page_filter: {
                    row_offset: 0,
                    num_rows: 10
                }
            });
            // vm.public.enableSaveSegmentButton = false;
            QueryStore.setPublicProperty('enableSaveSegmentButton', false);
            vm.isSaving = true;
            SegmentService.CreateOrUpdateSegment(segment).then(function(result) {
                
                if (isNewSegment) { 
                    vm.clickSegmentButton({
                        edit: segment.name
                    });
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
        console.log('export type', exportType);
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

                });
            });
        };
        vm.displayExportBanner = true;
    };


    vm.toggleExportDropdown = function($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        vm.showExportDropdown = !vm.showExportDropdown;
        
    }

    vm.hideExportBanner = function() {
        vm.displayExportBanner = false;
    }

    vm.init();
});