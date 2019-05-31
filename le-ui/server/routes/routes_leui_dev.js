'use strict';

module.exports = [{
    redirect: {
        '/': '/login'
    }
}, {
    path: '/projects/common',
    folders: {
        '/app': '/app',
        '/components': '/components',
        '/lib': '/lib',
        '/assets': '/assets',
        '/fonts': '/assets/fonts',
        '/images': '/assets/images'
    }
}, {
    path: '/projects/ng2/dist',
    html5mode: true,
    pages: {
        '/ng2': 'index.html'
    },
    folders: {
        '/ng2/lib': '/src/assets/lib'
    }
}, {
    path: '/projects/login',
    html5mode: true,
    xframe_options: 'deny',
    pages: {
        '/login': '/dist/indexwp.html',
        '/login/form': '/dist/indexwp.html',
        '/login/old': 'index.html',
        '/login/old/index': 'index.html'
    },
    folders: {
        '/login': '/dist',
        '/dist': '/dist',
        '/login/app': '/app',
        '/login/assets': '/assets',
        '/login/lib': '/lib',
        '/login/help': '/help'
    }
}, {
    path: '/projects/prospectdiscovery',
    pages: {
        '/pd/': 'index.html',
        '/pd/index': 'index.html'
    },
    folders: {
        '/pd/app': '/app',
        '/pd/assets': '/assets',
        '/pd/lib': '/lib'
    }
}, {
    path: '/projects/atlas',
    html5mode: true,
    pages: {
        '/atlas/': '/dist/indexwp.html',
        '/atlas/index': '/dist/indexwp.html'
    },
    folders: {
        '/atlas': '/dist',
        '/dist': '/dist',
        '/atlas/app': '/app',
        '/atlas/assets': '/assets',
        '/atlas/lib': '/lib'
    }
}, {
    path: '/projects/lpi',
    html5mode: true,
    pages: {
        '/lpi/': '/dist/indexwp.html',
        '/lpi/index': '/dist/indexwp.html'
    },
    folders: {
        '/lpi': '/dist',
        '/dist': '/dist',
        '/lpi/app': '/app',
        '/lpi/assets': '/assets',
        '/lpi/lib': '/lib'
    }
}, {
    path: '/projects/insights',
    html5mode: true,
    xframe_allow: [
        'localhost:3000',
        'localhost:3001',
        'localhost:3002',
        'localhost:3003',
        'force.com',
        'salesforce.com',
        'lattice-engines.com'
    ],
    pages: {
        '/insights/': '/dist/indexwp.html',
        '/insights': '/dist/indexwp.html',
        '/insights/index': '/dist/indexwp.html'
    },
    folders: {
        '/dist': '/dist',
        '/insights': '/dist',
        '/insights/app': '/app',
        '/insights/assets': '/assets',
        '/insights/lib': '/lib'
    }
}, {
    path: '/projects/lp2',
    pages: {
        '/lp2/': 'index.html',
        '/lp2/index': 'index.html'
    },
    folders: {
        '/lp2/app': '/app',
        '/lp2/assets': '/assets',
        '/lp2/lib': '/lib'
    }
}, {
    path: '/projects/demo',
    pages: {
        '/demo/': 'index.html',
        '/demo/index': 'index.html',
        '/demo/index.html': 'index.html',
        '/demo/colors.html': 'colors.html',
        '/demo/forms.html': 'forms.html',
        '/demo/grid.html': 'grid.html',
        '/demo/links.html': 'links.html',
        '/demo/lists.html': 'lists.html',
        '/demo/tables.html': 'tables.html',
        '/demo/typography.html': 'typography.html',
        '/demo/downloads.html': 'downloads.html'
    },
    folders: {
        '/demo/js': '/js',
        '/demo/css': '/css',
        '/demo/img': '/img',
        '/demo/fonts': '/fonts'
    }
}, {
    path: '/projects/websocket',
    html5mode: true,
    pages: {
        '/websocket/': 'index.html',
        '/websocket/index': 'index.html'
    },
    folders: {
        '/websocket/app': '/app',
        '/websocket/assets': '/assets',
        '/websocket/lib': '/lib',
    }
}, {
    path: '/projects/dante',
    pages: {
        '/dante/': 'assets/index.html',
        '/dante/index': 'assets/index.html'
    },
    folders: {
        '/dante': '/assets',
        '/dante/CommonAssets': '/assets/CommonAssets',
        '/dante/resources': '/assets/resources',
        '/dante/styles': '/assets/styles',
        '/dante/images': '/assets/images'
    }
}, {
    path: '/projects/uicomponents',
    html5mode: true,
    pages: {
        '/uicomponents/': '/storybook-static/index.html',
        '/uicomponents/index': '/storybook-static/index.html'
    },
    folders: {
        '/uicomponents': '/storybook-static'
    }
}];
