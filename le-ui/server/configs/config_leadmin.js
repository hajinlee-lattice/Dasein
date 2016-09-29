'use strict';

const path = require('path');

module.exports = {
    protocols: {
        http: process.env.ADMIN_HTTP_PORT,
        https: process.env.ADMIN_HTTPS_PORT
    },
    proxies: {
        '/admin': {
            'local_path': '/admin',
            'remote_host': process.env.API_ADMIN_URL || 'http://localhost:8085',
            'remote_path': '/admin',
          'type': 'pipe'
        },
        '/modelquality': {
            'local_path': '/modelquality',
            'remote_host': process.env.API_MCSVC_URL || 'http://localhost:8080',
            'remote_path': '/modelquality',
          'type': 'pipe'
        },
        '/influxdb' : {
            'local_path': '/influxdb',
            'remote_host': process.env.API_INFLUXDB_URL || 'http://localhost:8086',
            'remote_path': '/query',
            'type': 'pipe'
        }
    }
};
