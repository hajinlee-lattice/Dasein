angular.module('lp.ratingsengine.remodel.list', [])
.component('attrRemodelList', {
    templateUrl: 'app/ratingsengine/content/remodel/list/list.component.html',
    bindings: {
        filters: '<'
    },
    controller: function ($scope, $state, $stateParams, AtlasRemodelStore) {
        var vm = this;

        angular.extend(vm, {
            store: AtlasRemodelStore,
            params: $stateParams,
            allAttributes: AtlasRemodelStore.getRemodelAttributes(),
            currentPage: vm.filters.currentPage,
            pageSize: vm.filters.pageSize,
            queryText: vm.filters.queryText,
            sortBy: 'DisplayName'
        });

        // $scope.$watch('category', function(newValue, oldValue) {
        //     if(newValue != oldValue) {
        //         vm.filters.currentPage = 1;
        //     }
        // });

        vm.$onInit = function() {

            console.log(vm.filters);

        };

        vm.getCategoryAttributes = function(){
            var category = AtlasRemodelStore.get('category'),
                categoryAttributes = vm.allAttributes[category];

            angular.forEach(categoryAttributes, function(attribute){
                attribute.hasWarning = (attribute.IsCoveredByOptionalRule || attribute.IsCoveredByMandatoryRule) ? true : false;
            });

            return categoryAttributes;
        }

        vm.searchFilter = function(attr) {
            var text = vm.filters.queryText;
            if (text) {

                var chkName = attr.DisplayName.indexOf(text) >= 0,
                    chkCategory = (attr.Category || '').indexOf(text) >= 0;
                
                if (chkName || chkCategory) {
                    return true;
                } else if (attr.Attributes) {
                    for (var i=0; i<attr.Attributes.length; i++) {
                        if (attr.Attributes[i].DisplayName.indexOf(text) >= 0) {
                            return true;
                        }
                    }
                }
            } else {
                return true;
            }

            return false;
        }

        vm.endsWith = function(item, string) {
            var reg = new RegExp(string + '$'),
                item = item || '',
                match = item.match(reg);
            if(match) {
                return true;
            }
            return false;
        }

        vm.toggleSelected = function(attribute){
            var approvedUsage = attribute.ApprovedUsage;

            if ( approvedUsage.indexOf( 'None' ) > -1 ) {
                approvedUsage.splice(0,1);
                approvedUsage.push(attribute.OriginalApprovedUsage);
            } else {
                attribute.OriginalApprovedUsage = approvedUsage[0];
                approvedUsage.splice(0,1);
                approvedUsage.push('None');
            }
        }

    }
});