export default function ($stateProvider) {
    'ngInject';
    $stateProvider
        .state('home.segmentation', {
            url: '/segmentation',
            params: {
                pageTitle: 'Segmentation List',
                pageIcon: 'ico-segments',
                edit: null
            },
            resolve: {
                path: () => {
                    return 'segmentationlist';
                },
                ngservices: (DataCloudStore, AttributesStore) => {
                    let obj = {
                        DataCloudStore : DataCloudStore,
                        AttributesStore: AttributesStore
                    }
                    return obj;
                }
            },
            views: {
                "main@": 'reactAngularMainComponent'
            }
        })
};