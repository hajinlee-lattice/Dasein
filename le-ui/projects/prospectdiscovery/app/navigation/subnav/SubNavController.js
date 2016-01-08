angular.module('pd.navigation.subnav', [
        'pd.builder.attributes'
    ])
    .controller('SubNavSummaryCtrl', function($scope, $http, $filter, AttributesModel, StateMapping) {
        angular.extend($scope, AttributesModel);

        $scope.MatchedTotal = 0;
        $scope.RevenueTotal = 0;
        $scope.ContactsTotal = 0;
        var request = {}, LastLength = 0, LastMatchedTotal = 0;
        
        $scope.$watch('MasterList', function (lists) {
            var request = {};

            [
                'SubIndustry',
                'State',
                'EmployeesRange',
                'RevenueRange'
            ].forEach(function(category) {
                var list = $filter('filter')($scope.MasterList, {
                    AttrKey: category,
                    visible: true, 
                    selected: true 
                }, true);
                list.forEach(function(item, key) {
                    if (typeof request != "object") {
                        request = {};
                    }

                    if (!request[category]) {
                        request[category] = '';
                    }

                    request[category] += 
                        (request[category] ? '#' : '') + 
                        (StateMapping.AttrValue[item.AttrValue] || item.AttrValue);
                });
            });

            var keys = Object.keys(request), length = 0;

            keys.forEach(function(key) { length += request[key].length; });

            if (length == LastLength) {
                return false;
            }

            LastLength = length;

            $scope.refreshing = true;

            $http({
                method: 'GET',
                url: '/pls/companies/count',
                params: request
            }).then(function(MatchedTotal) {
                $scope.refreshing = false;
                $scope.MatchedTotal = parseInt(MatchedTotal.data);

                if ($scope.MatchedTotal == LastMatchedTotal) {
                    return false;
                }

                var min = 10, max = 20;
                $scope.ContactsTotal = 
                    (Math.floor(Math.random() * (max - min + 1)) + min) * $scope.MatchedTotal;
                
                var min = 500, max = 1500;
                $scope.RevenueTotal = 
                    (Math.floor(Math.random() * (max - min + 1)) + min) * $scope.MatchedTotal;
                
                LastMatchedTotal = $scope.MatchedTotal;
            });
        }, true);
    })
    .controller('SubNavCtrl', function ($scope, $rootScope) {
        this.init = function() { 
            this.lis = lis = $('div.carousel-slide-container div.white-border');
            $(lis).on('mousedown', this.handleClick.bind(this));
        }

        this.getMouseXY = function(target, event) {
            return {
                x: event.offsetX,
                y: event.offsetY
            }
        }

        this.clamp = function(number, min, max) {
            return Math.max(min, Math.min(number, max));
        }

        this.handleClick = function(event) {
            var target = event.target,
                target = $(target).hasClass('white-border') ? target : target.parentNode;

            if ($(target).hasClass('white-border')) {
                this.render(target, event);
            }
        }

        this.render = function(li, event) {
            var lis = this.lis,
                item = li,//$('div.carousel-slide-container a>span',li)[0],
                mouse = this.getMouseXY(li, event),
                dimensions = item.getClientRects()[0],
                center = {
                    x: dimensions.width >> 1,
                    y: dimensions.height >> 1
                },
                delta = {
                    x: (mouse.x - center.x) / center.x,
                    y: (center.y - mouse.y) / center.y
                }, 
                origin = {
                    x: this.clamp(1 - ((dimensions.width - mouse.x) / dimensions.width), 0, 1),
                    y: this.clamp(1 - ((dimensions.height - mouse.y) / dimensions.height), 0, 1)
                },
                flipThreshold = .70,
                maxRotation = 4,
                maxAngle = 33,
                minDuration = 500,
                maxDuration = 1250,
                magnitude = Math.min(Math.max(Math.abs(delta.x), Math.abs(delta.y)), 1),
                x = Math.min(Math.round(delta.x * maxRotation), maxRotation) * 360,
                y = Math.min(Math.round(delta.y * maxRotation), maxRotation) * 360,
                angle = Math.atan2(y, x) * (180 / Math.PI),
                duration = ((1 - magnitude) * (maxDuration - minDuration)) + minDuration,
                transition, transform, perspective, transitionEvent;

            transform = 'rotate3d(' + delta.y + ', ' + delta.x + ', 0, 360deg) translate3d(0,0,0px)';
            perspective = Math.round(origin.x * 100) + '% ' + Math.round(origin.y * 100) + '%';
            transition = 'transform ' + duration + 'ms ease';
            transitionEvent = this.whichTransitionEvent();

            item.style.transition = 'transform 0ms linear';
            //item.style.transform = 'rotate3d(0, 0, 0, 0deg) translate3d(0,0,0px)'; 
            li.style.perspectiveOrigin = '50% 50%';
            
            setTimeout(function() {
                item.style.transition = 'transform '+duration+'ms ease';
                item.style.transform = transform;
                li.style.perspectiveOrigin = perspective;

                //console.log(origin.x, origin.y, duration, delta.x, delta.y,  transitionEvent, transform, perspective);
                
                $(item).one(transitionEvent, function(event) {
                    item.style.transition = 'transform 0s linear';
                    item.style.transform = 'rotate3d(0, 0, 0, 0deg) translate3d(0,0,0px)'; 
                    li.style.perspectiveOrigin = '50% 50%';
                    
                    $(lis).removeClass('active');
                    $(li).addClass('active');
                });
            }, 0);
        }

        this.whichTransitionEvent = function() {
          var t,
              el = document.createElement("fakeelement");

          var transitions = {
            "transition"      : "transitionend",
            "OTransition"     : "oTransitionEnd",
            "MozTransition"   : "transitionend",
            "WebkitTransition": "webkitTransitionEnd"
          }

          for (t in transitions){
            if (el.style[t] !== undefined){
              return transitions[t];
            }
          }
        }


        this.init();
    }
);