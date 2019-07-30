import { actions } from '../../playbook.redux';
import LeSort from 'widgets/filters/sort/le-sort';
import LeSearch from 'widgets/filters/search/le-search';
import LeButtons from 'widgets/buttons/group/le-buttons';
import LeButton from 'widgets/buttons/le-button';
//import PlayItem from './playtile.component';
import PlayItem from './play.item';
import GridTile from './gridtile/gridtile.component';
import ListTile from './listtile/listtile.component';
import './playtile.component.scss';

let template = `
    <section class="container playbook-list">
        <le-item-bar store="$ctrl.ReduxPath"></le-item-bar>
        <le-item-view store="$ctrl.ReduxPath"></le-item-view>
    </section>
`;

let controller = function ($state) {
    'ngInject';
    let vm = this;

    vm.ReduxPath = 'playbook';
    vm.ReduxStore = actions; 

    vm.config = {
        itemview: {
            stores: {
                items: 'plays',
                view: 'views',
                sortby: 'sort',
                query: 'search'
            },
            pagination: {
                current: 1,
                pagesize: 12,
            },
            tile: {
                component: PlayItem,
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
                'addplay'
            ],
            search: {
                component: LeSearch,
                config: {
                    query: '',
                    open: true,
                    placeholder: 'Search Campaigns',
                    properties: [
                        'displayName',
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
                        { label: 'Created By', icon: 'alpha', property: 'createdBy' },
                        { label: 'Campaign Name', icon: 'alpha', property: 'displayName' }
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
                        { name: 'list', title: 'List View', icon: 'fa-th-list' }
                    ]
                }
            },
            addplay: {
                component: LeButton,
                containerClass: "pull-right",
                callback: vm.clickAddPlay,
                config: {
                    label: "Create",
                    classNames: ["blue-button"]
                }
            }
        }
    };

    vm.$onInit = function () {
        vm.ReduxStore.fetchPlays();
        vm.ReduxStore.setContext(vm);
        vm.ReduxStore.setItemBar(vm.config.itembar);
        vm.ReduxStore.setItemView(vm.config.itemview);
    };

    vm.clickAddPlay = function (element) {
        //$state.go('home.playbook.create');
        console.log('[click] Add Play', this);
    };
};

export default {
    template: template,
    controller: controller,
    bindings: {}
};