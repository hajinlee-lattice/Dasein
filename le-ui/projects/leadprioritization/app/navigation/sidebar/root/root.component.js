angular
.module('pd.navigation.sidebar.root', [])
.controller('SidebarRootController', function(SidebarStore, ResourceUtility) {
    var vm = this;
    vm.items = SidebarStore.items;
    vm.ResourceUtility = ResourceUtility;
});