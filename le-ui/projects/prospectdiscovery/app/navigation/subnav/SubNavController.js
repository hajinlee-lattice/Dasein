angular.module('pd.navigation.subnav', [
        'pd.builder.attributes'
    ])

    /*
        Controls the Target Market Builder Attributes SummaryCount Table
    */
    .controller('SummaryCtrl', function($scope, $http, $filter, AttributesModel, StateMapping) {
        var vm = this,
            request = {}, 
            LastLength = 0, 
            LastMatchedTotal = 0;

        angular.extend(this, AttributesModel, {
            init: function() {
                vm.LiftTotal = 2.725;
                vm.Percentage = 75;
                vm.MatchedTotal = 0;
                vm.RevenueTotal = 0;
                vm.ContactsTotal = 0;
                
                $scope.$watch(
                    angular.bind(this, this.getMasterList), 
                    angular.bind(this, this.render),
                    true);
            },
            getMasterList: function() { 
                return this.MasterList;
            },
            render: function(lists) {
                var request = {};

                [
                    'SubIndustry',
                    'State',
                    'EmployeesRange',
                    'RevenueRange'
                ].forEach(function(category) {
                    var list = $filter('filter')(vm.MasterList, {
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

                keys.forEach(function(key) { 
                    length += request[key].length; 
                });

                if (length == LastLength) {
                    return false;
                }

                LastLength = length;

                vm.refreshing = true;

                $http({
                    method: 'GET',
                    url: '/pls/companies/count',
                    params: request
                }).then(function(MatchedTotal) {
                    vm.refreshing = false;
                    vm.MatchedTotal = parseInt(MatchedTotal.data);

                    if (vm.MatchedTotal == LastMatchedTotal) {
                        return false;
                    }

                    var min = 10, max = 20;
                    vm.ContactsTotal = 
                        (Math.floor(Math.random() * (max - min + 1)) + min) * vm.MatchedTotal;
                    
                    var min = 500, max = 1500;
                    vm.RevenueTotal = 
                        (Math.floor(Math.random() * (max - min + 1)) + min) * vm.MatchedTotal;
                    
                    LastMatchedTotal = vm.MatchedTotal;
                });
            }
        });

        this.init();
    })

    /*
        Animated Attribute-disc carousel navigation
    */
    .controller('CarouselCtrl', function ($window, $scope) {
        var vm = this;

        angular.extend(this, {
            init: function() {
                this.container = $('div.carousel');

                vm.current = 1;
                vm.pagesize = 5;

                // this link data should be from the API, in the future
                vm.links = [
                    { name: "Industry", label: "Industries", color: "saphire", ico: "industry", amount: "5", total: "123" },
                    { name: "Locations", label: "Locations", color: "jade", ico: "locations", amount: "0", total: "52" },
                    { name: "Employees Range", label: "Employees Range", color: "gold", ico: "tmb-user", amount: "3", total: "54" },
                    { name: "Revenue Range", label: "Revenue Range", color: "amber", ico: "tmb-chart", amount: "0", total: "64" },
                    { name: "Technology Profile", label: "Technology Profile", color: "amethyst", ico: "tech-profile", amount: "2", total: "11" },
                    { name: "Web Presence", label: "Web Presence", color: "ruby", ico: "web", amount: "5", total: "32" },
                    { name: "Other", label: "Other Attributes", color: "saphire", ico: "chip", amount: "5", total: "123" }
                ];

                angular
                    .element($window)
                    .bind('resize', angular.bind(this, this.resize));

                this.resize(true);
            },
            resize: function(skipApply) {
                var width = $(this.container).width(),
                    segment = 170,
                    pagesize = vm.pagesize = Math.floor(width / segment);

                if ((vm.pagesize * vm.current) - vm.pagesize >= vm.links.length) {
                    vm.current = 1;
                }

                (skipApply !== true)
                    ? $scope.$apply() 
                    : null;
            },
            previous: function() {
                vm.current--;

                if (vm.current < 0) {
                    vm.current = Math.floor(vm.links.length / vm.pagesize);
                }
            },
            next: function() {
                vm.current++;

                if (vm.current > Math.floor(vm.links.length / vm.pagesize)) {
                    vm.current = 0;
                }
            },
            click: function($event) {
                var target = $('div.white-border', $event.currentTarget);

                if (target.length > 0) {
                    this.render(target[0], $event);
                }
            },
            render: function(item, event) {
                var mouse = this.getMouseXY(item, event),
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
                
                setTimeout(function() {
                    item.style.transition = 'transform '+duration+'ms ease';
                    item.style.transform = transform;

                    $(item).one(transitionEvent, function(event) {
                        item.style.transition = 'transform 0s linear';
                        item.style.transform = 'rotate3d(0, 0, 0, 0deg) translate3d(0,0,0px)'; 
                    });
                }, 0);
            },
            whichTransitionEvent: function() {
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
            },
            getMouseXY: function(target, event) {
                return {
                    x: event.offsetX,
                    y: event.offsetY
                }
            },
            clamp: function(number, min, max) {
                return Math.max(min, Math.min(number, max));
            }
        });

        this.init();
    });