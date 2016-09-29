'use strict';

const path = require('path');

module.exports = {
    protocols: {
        http: process.env.HTTP_PORT,
        https: process.env.HTTPS_PORT,
    },
    proxies: {
        '/pls': {
            'local_path': '/pls',
            'remote_host': process.env.API_URL || 'http://localhost:8081',
            'remote_path': '/pls',
          'type': 'pipe'
        },
        '/score': {
            'local_path': '/score',
            'remote_host': process.env.API_CON_URL || 'http://localhost:8073',
            'remote_path': '/score',
          'type': 'pipe'
        },
        '/files': {
            'local_path': '/files',
            'remote_host': process.env.API_URL || 'http://localhost:8080',
            'remote_path': '/pls',
          'type': 'file_pipe'
        }
    }
};
