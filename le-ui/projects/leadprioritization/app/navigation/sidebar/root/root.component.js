angular
.module('pd.navigation.sidebar.root', [])
.controller('SidebarRootController', function(SidebarStore) {
    var vm = this;
    vm.items = SidebarStore.items;
});