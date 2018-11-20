angular.module('lp.cg.talkingpoint.attributepane', [
])
.directive('cgTpAttributePane', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            opts: '=',
            entities: '='
        },
        templateUrl: 'app/cgtalkingpoint/attributepane/attributepane.component.html',
        link: function($scope, $element) {
            $scope.$on("deleteClicked", function (event, args) {
                $element[0].style.top = '';
            });
        },
        controller: function($scope, $element, $filter, $window, CgTalkingPointStore) {
            //console.log($scope.entities);

            $scope.selected = $scope.entities[0];//Object.keys($scope.opts)[0];
            $scope.categories = new Set($scope.opts['account'].map(function(attr) {
                return attr.category;
            }));
            $scope.categories = Array.from($scope.categories);
            $scope.search = {};
            $scope.search.name = '';
            $scope.search.category = $scope.categories[0];
            $scope.priorSelectedCategory = null;

            $window.addEventListener('scroll', handleWindowScroll);

            $scope.$on('$destroy', function() {
                $window.removeEventListener('scroll', handleWindowScroll);
            });

            var originalTop = $element[0].offsetTop;

            $scope.changeEntity = function() {
                if ($scope.selected != 'account') {
                    $scope.priorSelectedCategory = $scope.search.category;
                    delete $scope.search.category;
                } else {
                    $scope.search.category = $scope.priorSelectedCategory ? $scope.priorSelectedCategory : $scope.categories[0];
                }
            }

            $scope.getAttributes = function() {
                return $scope.opts[$scope.selected];

            }

            function handleWindowScroll(evt) {

                var scrollY = evt.currentTarget.scrollY,
                    paneBottom = $element.offset().top + $element.outerHeight(),
                    container = $element[0].parentElement,
                    $container = angular.element(container),
                    containerBottom = $container.offset().top + $container.outerHeight();

                if (scrollY > originalTop) {
                    $element.addClass('loose');
                    if(paneBottom < containerBottom) {
                        $element[0].style.top = (scrollY - originalTop + 10) + 'px';
                    }
                } else {
                    $element.removeClass('loose');
                    $element[0].style.top = '';
                }
            }

            $scope.eleMouseDown = function($event) {
                $event.preventDefault();
                $event.stopPropagation();

                $event.target.style.width = '200%';
                $event.target.style.left = '-200%';

                $window.addEventListener('mousemove', eleMouseMove, false);
                $window.addEventListener('mouseup', eleMouseUp, false);
            };

            var originalX = null, originalWidth;
            function eleMouseMove(evt) {
                evt.preventDefault();
                evt.stopPropagation();

                if (originalX === null) {
                    originalX = evt.clientX;
                    originalWidth = $element[0].offsetWidth;
                }

                var dx = originalX - evt.clientX;
                $element[0].style.width = originalWidth + dx + 'px';
            };

            function eleMouseUp(evt) {
                evt.preventDefault();
                evt.stopPropagation();

                evt.target.style.width = '';
                evt.target.style.left = '';

                originalX = null;
                originalWidth = null;

                $window.removeEventListener('mousemove', eleMouseMove, false);
                $window.removeEventListener('mouseup', eleMouseUp, false);
            };
        },
        controllerAs: 'vm'
    }
})
.directive('attributeDraggable', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            attribute: '='
        },
        templateUrl: 'app/cgtalkingpoint/attributepane/attributedraggable.component.html',
        controller: function($scope, $element) {
            $element[0].addEventListener('dragstart', onDragStart);

            $scope.$on('$destroy', function() {
                $element[0].removeEventListener('dragstart', onDragStart);
            });

            function onDragStart(evt) {
                evt.dataTransfer.dropEffect = 'move';
                evt.dataTransfer.effectAllowed = 'all';
                evt.dataTransfer.setData('text', '{!' + $scope.attribute.value + '}');
            }
        }
    }
});
