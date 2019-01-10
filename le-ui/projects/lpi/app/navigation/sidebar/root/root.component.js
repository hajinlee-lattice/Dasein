angular
.module('lp.navigation.sidebar.root', [])
.controller('SidebarRootController', function(SidebarStore, ResourceUtility) {
    var vm = this;
    vm.items = SidebarStore.get();
    vm.back = SidebarStore.back;
    vm.ResourceUtility = ResourceUtility;
});