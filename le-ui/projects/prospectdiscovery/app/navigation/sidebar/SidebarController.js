angular
    .module('lp.navigation.sidebar', [
        'pd.builder.attributes'
    ])
    .controller('SidebarCtrl', function($scope, $rootScope) {
        $scope.handleSidebarToggle = function ($event) {
            var target = angular.element($event.target),
                collapsable_click = !target.parents('.menu').length;
            if(collapsable_click) {
                $('body').toggleClass('open-nav');
                $('body').addClass('controlled-nav');  // indicate the user toggled the nav

                if (typeof(sessionStorage) !== 'undefined'){
                    sessionStorage.setItem('open-nav', $('body').hasClass('open-nav'));
                }
                $rootScope.$broadcast('sidebar:toggle');
            }
        };


        angular.extend($scope, {
            init: function(){
                if (typeof(sessionStorage) !== 'undefined') {
                    if(sessionStorage.getItem('open-nav') === 'true' || !sessionStorage.getItem('open-nav')) {
                        $("body").addClass('open-nav');
                    } else {
                        $("body").removeClass('open-nav');
                    }
                }
            }
        })

        $scope.init();


    })
    .controller('BuilderSidebarCtrl', function($scope, AttributesModel) {
            
        $scope.handleSidebarToggle = function ($event) {
            var target = angular.element($event.target),
                collapsable_click = !target.parents('.menu').length;
            if(collapsable_click) {
                $('body').toggleClass('open-nav');
                $('body').addClass('controlled-nav');  // indicate the user toggled the nav

                if (typeof(sessionStorage) !== 'undefined'){
                    sessionStorage.setItem('open-nav', $('body').hasClass('open-nav'));
                }
                $rootScope.$broadcast('sidebar:toggle');
            }
        };


        angular.extend($scope, AttributesModel, {
            init: function() {
                if (typeof(sessionStorage) !== 'undefined') {
                    if(sessionStorage.getItem('open-nav') === 'true' || !sessionStorage.getItem('open-nav')) {
                        $("body").addClass('open-nav');
                    } else {
                        $("body").removeClass('open-nav');
                    }
                }
            },
            handleRootCategoryRemoval: function(item, children) {
                (children || []).forEach(function(child) {
                    child.selected = false;
                });
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