"use strict";

module.exports = [
    {
        path: '/projects/common',
        folders: {
            '/app': '/app',
            '/assets': '/assets',
            '/lib': '/lib'
        }
    },{
        path: '/projects/login',
        pages: {
            '/': 'index.html',
            '/index': 'index.html',
            '/login/': 'index.html',
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
    }
];