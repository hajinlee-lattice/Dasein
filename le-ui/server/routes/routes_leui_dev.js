'use strict';

module.exports = [{
    redirect: {
        '/': '/login'
    }
},{
    path: '/projects/common',
    folders: {
        '/app': '/app',
        '/components': '/components',
        '/lib': '/lib',
        '/assets': '/assets',
        '/common/assets': '/assets',
        '/fonts': '/assets/fonts',
        '/images': '/assets/images'
    }
},{
    path: '/projects/ng2/dist',
    html5mode: true,
    pages: {
        '/ng2': 'index.html'
    },
    folders: {
        '/ng2/lib': '/src/assets/lib'
    }
},{
    path: '/projects/login',
    html5mode: true,
    pages: {
        '/login': 'index.html',
        '/login/index': 'index.html'
    },
    folders: {
        '/login/app': '/app',
        '/login/assets': '/assets',
        '/login/lib': '/lib',
        '/login/help': '/help'
    }
},{
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
},{
    path: '/projects/leadprioritization',
    html5mode: true,
    pages: {
        '/lp/': 'index.html',
        '/lp/index': 'index.html'
    },
    folders: {
        '/lp/app': '/app',
        '/lp/assets': '/assets',
        '/lp/lib': '/lib'
    }
},{
    path: '/projects/insights',
    html5mode: true,
    pages: {
        '/insights/': 'index.html',
        '/insights/index': 'index.html'
    },
    folders: {
        '/insights/app': '/app',
        '/insights/assets': '/assets'
    }
},{    
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
},{
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
}];
