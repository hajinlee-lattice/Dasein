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
            sortBy: 'DisplayName'
        });

        // $scope.$watch('category', function(newValue, oldValue) {
        //     if(newValue != oldValue) {
        //         vm.filters.currentPage = 1;
        //     }
        // });

        vm.$onInit = function() {

            // console.log(vm.filters);

        };

        vm.getCategoryAttributes = function(){
            var category = AtlasRemodelStore.get('category'),
                categoryAttributes = vm.allAttributes[category],
                associatedRules = AtlasRemodelStore.get('associatedRules');

            angular.forEach(categoryAttributes, function(attribute){

                var attributeRules = attribute.AssociatedDataRules;

                if(attributeRules.length > 0){
                    var filtered = associatedRules.filter(function (e) {
                        return attributeRules.indexOf(e.name) >= 0; 
                    });

                    console.log(filtered);
                    attribute.ruleTooptips = filtered;
                }

                // attribute.hasWarning = (attribute.IsCoveredByOptionalRule || attribute.IsCoveredByMandatoryRule) ? true : false;

                // if(attribute.hasWarning){
                //     console.log(attribute);
                // }
            });

            return categoryAttributes;
        }

        vm.searchFilter = function(attr) {

            // vm.filters.currentPage = 1;

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