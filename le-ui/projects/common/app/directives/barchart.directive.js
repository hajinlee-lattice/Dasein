angular.module('mainApp.appCommon.directives.barchart', [])
.directive('barchart', function () {
    return {
        restrict: 'E',
        templateUrl: '/components/charts/barchart.component.html',
        scope: { 
            data: '=',
            ratings: '=',
            header: '@'
        },
        link: function (scope, element, attrs, ctrl) {

            scope.isRatingsChart = scope.ratings || false;

            // console.log(scope.data);

            var data = scope.data,
                index = null,
                max = Math.max.apply(Math,data.map(function(o){return o.count;}));

            data.forEach(function(bucket){
                bucket.height = ((bucket.count / max) * 100);
            });

            if(scope.isRatingsChart){
                data.forEach(function(bucket){
                    if(bucket.bucket === 'A+'){

                        for(var i = 0; i < data.length; i += 1) {
                            if(data[i].bucket === "A+") {
                                var index = i;
                            }
                        }

                        if (index === 0) return;
                        if (index > 0) {
                            data.splice( index, 1 );
                        }

                        data.unshift( bucket );
                    }
                });
            }
        }
    }
});