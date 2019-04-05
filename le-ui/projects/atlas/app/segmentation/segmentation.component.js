import LeSort from 'widgets/filters/sort/le-sort';
import LeSearch from 'widgets/filters/search/le-search';
import LeButtons from 'widgets/buttons/group/le-buttons';
import LeButton from 'widgets/buttons/le-button';
import SegmentItem from './segmentation.item';
import GridTile from './gridtile/gridtile.component';
import ListTile from './listtile/listtile.component';

let template = /*html*/`
    <section class="container segments-list">
        <le-item-bar store="$ctrl.ReduxPath"></le-item-bar>
        <le-item-view store="$ctrl.ReduxPath"></le-item-view>
    </section>
`;

let controller = function ($state, AttributesStore, DataCloudStore) {
    'ngInject';
    let vm = this;

    vm.ReduxPath = 'segmentation';
    vm.ReduxStore = $state.get('home.segmentation').data.redux;
    vm.AttributesStore = AttributesStore;

    vm.config = {
        itemview: {
            stores: {
                items: 'segments',
                view: 'views',
                sortby: 'sort',
                query: 'search'
            },
            pagination: {
                current: 1,
                pagesize: 12,
            },
            tile: {
                component: SegmentItem,
                config: {
                    clickEdit: () => { },
                    clickDelete: () => { },
                    clickDuplicate: () => { }
                },
                views: {
                    grid: GridTile,
                    list: ListTile
                }
            }
        },
        itembar: {
            order: [
                'search',
                'sort',
                'views',
                'addsegment'
            ],
            search: {
                component: LeSearch,
                config: {
                    query: '',
                    open: true,
                    placeholder: 'Search Segments',
                    properties: [
                        'display_name',
                        'description'
                    ]
                }
            },
            sort: {
                component: LeSort,
                config: {
                    label: 'Sort By',
                    icon: 'numeric',
                    order: '-',
                    property: 'updated',
                    visible: false,
                    items: [
                        { label: 'Creation Date', icon: 'numeric', property: 'created' },
                        { label: 'Modified Date', icon: 'numeric', property: 'updated' },
                        { label: 'Author Name', icon: 'alpha', property: 'created_by' },
                        { label: 'Segment Name', icon: 'alpha', property: 'display_name' }
                    ]
                }
            },
            views: {
                component: LeButtons,
                config: {
                    active: 'grid',
                    activeColor: 'blue',
                    items: [
                        { name: 'grid', title: 'Grid View', icon: 'fa-th' },
                        { name: 'list', title: 'List View', icon: 'fa-th-list' },
                        { name: 'table', title: 'Table View', icon: 'fa-table' }
                    ]
                }
            },
            addsegment: {
                component: LeButton,
                containerClass: "pull-right",
                callback: vm.clickAddSegment,
                config: {
                    label: "Add Segment",
                    classNames: ["blue-button"]
                }
            }
        }
    };

    vm.$onInit = function () {
        let enrichments = DataCloudStore.enrichments || [];
        let cube = DataCloudStore.cube || {};

        vm.ReduxStore.getEnrichments(enrichments);
        vm.ReduxStore.getCube(cube);
        vm.ReduxStore.getSegments();

        vm.ReduxStore.setContext(vm);
        vm.ReduxStore.setItemBar(vm.config.itembar);
        vm.ReduxStore.setItemView(vm.config.itemview);
    };

    vm.clickAddSegment = function (element) {
        console.log('[click] Add Segment', this);
    };
};

export default {
    template: template,
    controller: controller,
    bindings: {}
};