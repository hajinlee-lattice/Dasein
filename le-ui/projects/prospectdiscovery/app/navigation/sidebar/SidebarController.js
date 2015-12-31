angular
    .module('pd.navigation.sidebar', [
        'pd.builder.attributes'
    ])
    .controller('SidebarCtrl', function($scope, $rootScope) {
        $scope.toggle = function() {
            $("body").toggleClass("open-nav");
        }
    })
    .controller('BuilderSidebarCtrl', function($scope, AttributesModel) {
        angular.extend($scope, AttributesModel, {
            init: function() {
                console.log('!',$scope, AttributesModel);
            },
            handleSubCategoryRemoval: function(selected, children) {
                console.log('remove', selected, children.length, children, $scope);
                
                if (children.length <= 1) {
                    AttributesModel
                        .getList({
                            AttrKey: selected.ParentKey,
                            AttrValue: selected.ParentValue
                        })
                        .then(angular.bind(this, function(result) {
                            (result.length > 0 ? result[0] : {})
                                .selected = selected.selected;
                        }));
                }
            }
        });

        $scope.init();
    }
);