angular.module('lp.playbook.wizard.rating', [])
.controller('PlaybookWizardRating', function(
    $state, $stateParams, $scope, $filter, ResourceUtility, Ratings, PlaybookWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.rating_form,
        ratings: Ratings,
        ratingsCounts: null,
        currentPage: 1,
        pageSize: 10,
        block_user: true
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    if($stateParams.rating_id) {
        var rating = $filter('filter')(Ratings, {id: $stateParams.rating_id})[0];
        vm.stored.rating_selection = $stateParams.rating_id;
        PlaybookWizardStore.setRating(rating);
        PlaybookWizardStore.setSettings({
            ratingEngine: {
                id: $stateParams.rating_id
            }
        });
    }

    vm.init = function() {
        PlaybookWizardStore.setValidation('rating', false);
        if(vm.stored.rating_selection) {
            PlaybookWizardStore.setValidation('rating', true);
        }
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                vm.block_user = false;
                if(play.ratingEngine) {
                    vm.stored.rating_selection = play.ratingEngine.id;
                    PlaybookWizardStore.setRating(play.ratingEngine);
                    PlaybookWizardStore.setValidation('rating', true);
                }

            });
        } else {
            vm.block_user = false;
        }
    }

    vm.getAccountCount = function(rating){
        var count = 0;
        if(rating && rating.bucketMetadata){
            rating.bucketMetadata.forEach(function(bucket){
                if(bucket.num_leads){
                    count = count + bucket.num_leads;
                }
            });
        }else{
            return rating.accountsInSegment;
        }
        return count;
    }

    vm.saveRating = function(rating) {
        var _rating = angular.merge({}, PlaybookWizardStore.savedRating, rating);
        PlaybookWizardStore.setRating(_rating);
    }

    vm.searchFields = function(rating){
        if (vm.search) {
            if (rating.displayName && textSearch(rating.displayName, vm.search)) {
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
        PlaybookWizardStore.setValidation('rating', form.$valid);
        if(vm.stored.rating_selection) {
            PlaybookWizardStore.setSettings({
                ratingEngine: {
                    id: vm.stored.rating_selection
                }
            });
        }
    }

    vm.init();
});