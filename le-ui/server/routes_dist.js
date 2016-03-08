"use strict";

module.exports = [
    {
        path: '/projects/common',
        folders: {
            '/assets': '/assets',
            '/dist': '/dist',
            '/lib': '/lib',
            '/common/assets': '/assets',
            '/common/dist': '/dist'
        }
    },{
        path: '/projects/login',
        pages: {
            '/': 'dist/index.html',
            '/index': 'dist/index.html',
            '/login/': 'dist/index.html',
            '/login/index': 'dist/index.html'
        },
        folders: {
            '/login/assets': '/assets',
            '/login/lib': '/lib',
            '/login/help': '/help',
            '/login': '/dist'
        }
    },{
        path: '/projects/prospectdiscovery',
        pages: {
            '/pd/': 'index.html',
            '/pd/index': 'index.html'
        },
        folders: {
            '/pd/assets': '/assets',
            '/pd/dist': '/dist',
            '/pd/lib': '/lib'
        }
    },{
        path: '/projects/leadprioritization',
        pages: {
            '/lp/': 'dist/index.html',
            '/lp/index': 'dist/index.html'
        },
        folders: {
            '/lp/assets': '/assets',
            '/lp/dist': '/dist',
            '/lp/lib': '/lib',
            '/lp': '/dist'
        }
    }
];