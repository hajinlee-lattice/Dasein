export default function () {
    return {
        restrict: 'A',
        scope: {
            vm: '='
        },
        templateUrl: '/components/datacloud/explorer/latticeratingcard/latticeratingcard.component.html',
        controllerAs: 'vm',
        controller: function ($scope) {
            'ngInject';

            var vm = $scope.vm;

            angular.extend(vm, {
            });

            var getLatticeRatingData = function () {
                if (vm.workingBuckets.length) {
                    var total = 0,
                        lifts = 0,
                        top_lift = 0,
                        data = {
                            total: 0,
                            lifts: 0,
                            ratings: [],
                            slices: []
                        };

                    vm.workingBuckets.forEach(function (bucket) {
                        var bucket_data = {
                            Lbl: bucket.bucket_name,
                            Cnt: bucket.num_leads,
                            Lift: bucket.lift,
                            Classname: bucket.bucket_name.replace('+', '-plus')
                        }
                        data.ratings.push(bucket_data);
                    });

                    data.ratings.forEach(function (item) {
                        total = total + item.Cnt;
                        lifts = lifts + item.Lift;
                        top_lift = (item.Lift > top_lift ? item.Lift : top_lift);
                    });
                    data.total = total;
                    data.lifts = lifts;
                    data.slice_size = top_lift.toFixed(1) / 4;
                    data.top_lift = top_lift;
                    for (var i = 0; i < 4; i++) {
                        data.slices.push(i * data.slice_size.toFixed(2));
                    }
                    return data;
                }
            }

            vm.latticeRatings = getLatticeRatingData();
        }
    };
};