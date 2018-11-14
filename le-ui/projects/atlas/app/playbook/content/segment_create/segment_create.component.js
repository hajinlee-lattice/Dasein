angular.module('lp.playbook.wizard.segmentcreate', [])
.controller('PlaybookWizardSegmentCreate', function(
    $state, $stateParams, $scope, $filter, ResourceUtility, Segments, PlaybookWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.segment_form,
        segments: Segments,
        currentPage: 1,
        pageSize: 10,
        block_user: true
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    vm.init = function() {
        PlaybookWizardStore.setValidation('segment', false);
        if(vm.stored.segment_selection) {
            PlaybookWizardStore.setValidation('segment', true);
        }

        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                vm.block_user = false;
                if(play.ratingEngine) {
                    vm.stored.segment_selection = play.ratingEngine.segmentName;
                    PlaybookWizardStore.setRating(play.ratingEngine);
                    PlaybookWizardStore.setValidation('segment', true);
                }
            });
        } else {
            vm.block_user = false;
        }
    }

    vm.saveSegment = function(segment) {
        PlaybookWizardStore.setRating({
            targetSegment: {
                name: segment.name
            }
        });
    }

    if($stateParams.segment_name) {
        vm.stored.segment_selection = $stateParams.segment_name;
        PlaybookWizardStore.setRating({
            targetSegment: {
                name: $stateParams.segment_name
            }
        });
        PlaybookWizardStore.setSettings({
            targetSegment: {
                name: $stateParams.segment_name
            }
        });
    }

    function findSegmentByName(segment_name) {
        var segment = Segments.find(function(segment) {
            return segment.name === segment_name
        });
        return segment;
    }


    vm.searchFields = function(segment){
        if (vm.search) {
            if (segment.display_name && textSearch(segment.display_name, vm.search)) {
                return true;
            } else {
                return false;
            }
        }

        return true;
    }

    vm.hasAccountCount = function(rating) {
        if(vm.getAccountCount(rating)) {
            return true;
        }
        return false;
    }

    var textSearch = function(haystack, needle, case_insensitive) {
        var case_insensitive = (case_insensitive === false ? false : true);

        if (case_insensitive) {
            var haystack = haystack.toLowerCase(),
            needle = needle.toLowerCase();
        }

        // .indexOf is faster and more supported than .includes
        return (haystack.indexOf(needle) >= 0);
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('segment', form.$valid);
        if(vm.stored.segment_selection) {
            PlaybookWizardStore.setSettings({
                targetSegment: {
                    name: vm.stored.segment_selection
                }
            });
        }
    }

    vm.init();
});