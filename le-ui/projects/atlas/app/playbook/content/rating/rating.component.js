angular.module('lp.playbook.wizard.rating', [])
.controller('PlaybookWizardRating', function(
    $state, $stateParams, $scope, $filter, ResourceUtility, Ratings, PlaybookWizardStore, PlaybookWizardService
) {
    var vm = this,
        requireModel = false; // whether or not you can deselect a model or not have one

    angular.extend(vm, {
        stored: PlaybookWizardStore.rating_form,
        ratings: Ratings,
        ratingsCounts: null,
        currentPage: 1,
        pageSize: 10,
        block_user: true,
        scoredAccountsKeys: [],
        scoredAccounts: {},
        showCounts: false
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
            vm.makeScores();
        }
    });

    $scope.$watch('vm.currentPage', function(newValue, oldValue) {
        vm.makeScores();
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
        PlaybookWizardStore.setValidation('rating', !requireModel); // model is no longer required as per PLS-11534 (is toggable)
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

    vm.makeScores = _.debounce(makeScores, 250);

    function makeScores() {
        var filteredList = vm.filteredList.slice(vm.pageSize * (vm.currentPage-1), vm.pageSize * vm.currentPage),
            engines = [];

        filteredList.forEach(function(engine) {
            if(vm.scoredAccountsKeys.indexOf(engine.id) === -1) {
                engines.push(engine.id);
            }
        });
        if(engines.length) {
            var segmentName = (PlaybookWizardStore.savedRating && PlaybookWizardStore.savedRating.targetSegment ? PlaybookWizardStore.savedRating.targetSegment.name : '');
            if(!segmentName) {
                segmentName = (PlaybookWizardStore.settings && PlaybookWizardStore.settings.targetSegment ? PlaybookWizardStore.settings.targetSegment.name : '');   
            }
            if(segmentName) {
                PlaybookWizardService.getRatingSegmentCounts(segmentName, engines, {
                    loadContactsCount: true, 
                    loadContactsCountByBucket: true
                }).then(function(result) {
                    for(var i in result.ratingModelsCoverageMap) {
                            var item = result.ratingModelsCoverageMap[i];
                        vm.scoredAccounts[i] = {
                            scoredAccountCount: item.accountCount,
                            unscoredAccountCount: item.unscoredAccountCount
                        }
                        vm.scoredAccountsKeys.push(i);
                    }
                });
            }
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
        var current_rating = PlaybookWizardStore.getSavedRating();
        current_rating = current_rating || {};

        if(!requireModel && vm.stored.rating_selection && vm.stored.rating_selection === current_rating.id) {
            vm.stored.rating_selection = null;
            PlaybookWizardStore.setRating(null);
            PlaybookWizardStore.setSettings({
                ratingEngine: null
            }, true);
            return false;
        }
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