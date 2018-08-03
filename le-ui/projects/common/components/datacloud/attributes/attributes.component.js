angular.module('common.attributes', [
    'common.attributes.header',
    'common.attributes.subheader',
    'common.attributes.controls',
    'common.attributes.filters',
    'common.attributes.categories',
    'common.attributes.list',
    'common.attributes.enable',
    'common.attributes.activate',
    'common.attributes.edit'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.attributes', {
            url: '/attributes',
            // onEnter: function($state, SidebarStore, StateHistory) {
            //     console.log('enter home.attributes', $state.get(), StateHistory.lastFrom());
            //     SidebarStore.set([{
            //         sref: "home.attributes.activate",
            //         label: "Choose Premium",
            //         icon: "ico-analysis ico-light-gray"
            //     },{
            //         sref: "home.attributes.enable",
            //         label: "Enable and Disable",
            //         icon: "ico-analysis ico-light-gray"
            //     }], StateHistory.lastFrom().name || "home");
            // },
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Attribute Admin'
            },
            resolve: {
                tabs: [function() {
                    return [{
                        sref: "home.attributes.activate",
                        label: "Activate Premium Attributes"
                    },{
                        sref: "home.attributes.enable",
                        label: "Enable & Disable Attributes"
                    // },{
                    //     sref: "home.attributes.edit",
                    //     label: "Edit Name & Description"
                    }];
                }]
            },
            views: {
                "summary@": "attrHeader"
            },
            redirectTo: 'home.attributes.activate'
        });
})
.component('adminAttributes', {
    templateUrl: '',
    controller: function() {
    }
});