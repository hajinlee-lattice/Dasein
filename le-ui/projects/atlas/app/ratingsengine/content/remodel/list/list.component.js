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

        vm.$onInit = function() {

            vm.associatedRules = AtlasRemodelStore.get('associatedRules');

        };

        vm.getCategoryAttributes = function(){
            var category = AtlasRemodelStore.get('category'),
                categoryAttributes = vm.allAttributes[category];

            angular.forEach(categoryAttributes, function(attribute){
                attribute.hasWarning = (attribute.IsCoveredByOptionalRule || attribute.IsCoveredByMandatoryRule) ? true : false;
            });

            return categoryAttributes;
        }

        vm.filterRules = function(attr) {

            vm.attrTooltipContent = [];

            angular.forEach(vm.associatedRules, function(rule){
                angular.forEach(attr.AssociatedDataRules, function(attrRule){
                    if(rule.name === attrRule){
                        vm.attrTooltipContent.push(rule);
                    }
                });
            });
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