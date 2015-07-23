var app = angular.module("app.tenants.directive.ListEntryDirective", [
    'le.common.util.UnderscoreUtility'
]);

app.directive('listEntry', function(){
    return {
        restrict: 'AE',
        templateUrl: 'app/tenants/view/ListEntryView.html',
        scope: {config: '=', isValid: '=', readonly: '='},
        controller: function($scope){
            $scope.list = _.map(JSON.parse($scope.config.Data), function(str){
                return {value: str};
            });

            $scope.addItem = function() {
                $scope.list.push({value: ""});
                $scope.syncData();
            };

            $scope.deleteItem = function(idx) {
                $scope.list.splice(idx, 1);
                $scope.syncData();
            };

            $scope.syncData = function() {
                $scope.config.Data = JSON.stringify(
                    _.map($scope.list, "value")
                );
            };
        }
    };
});