angular.module('lp.ratingsengine.remodel.list', [])
.component('attrRemodelList', {
    templateUrl: 'app/ratingsengine/content/remodel/list/list.component.html',
    bindings: {
        filters: '<'
    },
    controller: function ($scope, $state, $stateParams, AtlasRemodelStore) {
        var vm = this;

        vm.$onInit = function() {

            vm.store = AtlasRemodelStore;
            vm.params = $stateParams;
            vm.allAttributes = AtlasRemodelStore.getRemodelAttributes(),
            vm.currentPage = 1;
            vm.pageSize = 10;
            vm.sortBy = 'DisplayName';

        };

        vm.getCategoryAttributes = function(){
            vm.category = AtlasRemodelStore.get('category');
            return vm.allAttributes[vm.category];
        }

        vm.searchFilter = function(attr) {
            // var text = vm.filters.queryText; 

            // if (text) {

            //     console.log(vm.filters.queryText, attr);
            //     var chkName = attr.DisplayName.indexOf(text) >= 0;
            //     var chkCategory = (attr.Category || '').indexOf(text) >= 0;
                
            //     if (chkName || chkCategory) {
            //         return true;
            //     } else if (attr.Attributes) {
            //         for (var i=0; i<attr.Attributes.length; i++) {
            //             if (attr.Attributes[i].DisplayName.indexOf(text) >= 0) {
            //                 return true;
            //             }
            //         }
            //     }
            // } else {
            //     return true;
            // }

            return true;
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
            
            if (attribute.ApprovedUsage === "None"){
                attribute.ApprovedUsage = attribute.OriginalApprovedUsage;
            } else {
                attribute.OriginalApprovedUsage = attribute.ApprovedUsage;
                attribute.ApprovedUsage = "None";    
            }

        }

    }
});