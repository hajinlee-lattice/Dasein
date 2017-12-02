angular.module('lp.ratingsengine.dashboard', ['mainApp.appCommon.directives.modal.window'])
.controller('RatingsEngineDashboard', function(
    $q, $stateParams, $state, $rootScope, $scope, StateHistory, RatingsEngineStore, RatingsEngineService, 
    Rating, TimestampIntervalUtility, NumberUtility, ModalStore
) {
    var vm = this;

    angular.extend(vm, {
        rating: Rating,
        hasRules: RatingsEngineStore.hasRules(Rating.summary),
        editable: true,
        editing: {},
        StateHistory: StateHistory,
        TimestampIntervalUtility: TimestampIntervalUtility,
        NumberUtility: NumberUtility
    });
    vm.initModalWindow = function() {
        vm.config = {
            'name': "rating_engine_deactivate",
            'type': 'sm',
            'title': 'Deactivate Rating Engine',
            'titlelength': 100,
            'dischargetext': 'CANCEL',
            'dischargeaction' :'cancel',
            'confirmtext': 'DEACTIVATE',
            'confirmaction' : 'deactivate',
            'icon': 'ico ico-model ico-black',
            'showclose': false
        };
    
        vm.modalCallback = function (args) {
            if('closedForced' === args) {
            }else if(vm.config.dischargeaction === args){
                vm.toggleModal();
            } else if(vm.config.confirmaction === args){
                var modal = ModalStore.get(vm.config.name);
                modal.waiting(true);
                modal.disableDischargeButton(true);
                vm.deactivateRating();
            }
        }
        vm.toggleModal = function () {
            var modal = ModalStore.get(vm.config.name);
            if(modal){
                modal.toggle();
            }
        }
        vm.viewUrl = function () {
            return 'app/ratingsengine/content/dashboard/deactive-message.component.html';
        }

        $scope.$on("$destroy", function() {
            ModalStore.remove(vm.config.name);
        });
    }

   
    vm.getBarHeight = function(label, count) {
        // find the highest bucket value and use that for total
        var max = Object.values(vm.buckets.data).reduce(function (a, b) {
            return (a > b ? a : b);
        });

        return vm.NumberUtility.MakePercentage(count, max, '%');
    }

    vm.init = function() {
        if(vm.rating.coverageInfo && vm.rating.coverageInfo.bucketCoverageCounts) {
            vm.buckets = makeGraph(vm.rating.coverageInfo.bucketCoverageCounts, {label: 'bucket', count: 'count'});
        }
        vm.initModalWindow();
        // vm.buckets = makeGraph([
        //     {bucket: 'A', count: 0},
        //     {bucket: 'A-', count: 753},
        //     {bucket: 'B', count: 9913},
        //     {bucket: 'C', count: 32576},
        //     {bucket: 'E', count: 0},
        //     {bucket: 'F', count: 3310},
        // ], {label: 'bucket', count: 'count'});
    }

    var makeGraph = function(data, params) {
        if(!data) {
            return null;
        }
        var label_key = params.label || 'bucket',
            count_key = params.count || 'count',
            total = 0,
            _graph = {},
            graph = {};
        angular.forEach(data, function(value) {
            total = total + value[count_key];
            _graph[value[label_key]] = value[count_key];
        });
        graph = {
            total: total,
            data: _graph
        }
        return graph;
    }

    /**
     * contenteditable elements convert to html entities, so I removed it but want to keep this 
     * function because it could be useful if I figure out a way around this issue
     */
    vm.keydown = function($event, max, debug) {
        var element = angular.element($event.currentTarget),
            html = element.html();
            length = html.length,
            max = max || 50,
            allowedKeys = [8, 35, 36, 37, 38, 39, 40, 46]; // up, down, home, end, delete, backspace, things like that go in here

        if(debug) {
            console.log('pressed', $event.keyCode, 'length', length, 'html', html);
        }
        
        if(length > (max - 1) && allowedKeys.indexOf($event.keyCode) === -1) {
            $event.preventDefault();
        }

    }

    vm.autofocus = function($event) {
        var element = angular.element($event.currentTarget),
            target = element.find('[autofocus]');

        target.focus();
        // set focus and put cursor at begining
        setTimeout(function() {
            target.focus(); // because textareas
            target[0].setSelectionRange(0, 0);
        }, 10);
    }

    vm.edited = function(property) {
        if(!vm.editing[property]) {
            return false;
        }

        var content = vm.editing[property],
            newRating = angular.copy(vm.rating.summary),
            save = false;

        newRating[property] = content;

        if(vm.rating.summary[property] != newRating[property]) {
            save = true;
        }

        if(save) {
            vm.editable = false; // block rapid edits
            saveRating = {
                id: vm.rating.summary.id,
                displayName: newRating.displayName  
            }
            RatingsEngineService.saveRating(saveRating).then(function(data){
                vm.rating.summary = data;
                vm.editable = true;
                $rootScope.$broadcast('header-back', { 
                    path: '^home.rating.dashboard',
                    displayName: data.displayName,
                    sref: 'home.ratingsengine'
                });
            });
        }
    }


    vm.isActive = function(status) {
        return (status === 'ACTIVE' ? true : false);
    }

    vm.deactivateRating = function(){
        var newStatus = (vm.isActive(vm.rating.summary.status) ? 'INACTIVE' : 'ACTIVE'),
        newRating = {
            id: vm.rating.summary.id,
            status: newStatus
        }
        RatingsEngineService.saveRating(newRating).then(function(data){
            vm.rating.summary = data;
            vm.status_toggle = vm.isActive(data.status);
            vm.toggleModal();
        });
    }

    vm.status_toggle = vm.isActive(vm.rating.summary.status);

    vm.toggleActive = function() {
        var active = vm.isActive(vm.rating.summary.status);
        if(active && vm.rating.plays.length > 0){
            var modal = ModalStore.get(vm.config.name);
            modal.toggle();
        } else {
            var newStatus = (vm.isActive(vm.rating.summary.status) ? 'INACTIVE' : 'ACTIVE'),
                newRating = {
                id: vm.rating.summary.id,
                status: newStatus
            }
            RatingsEngineService.saveRating(newRating).then(function(data){
                vm.rating.summary = data;
                vm.status_toggle = vm.isActive(data.status);
            });
        }
    }

    vm.init();
});
