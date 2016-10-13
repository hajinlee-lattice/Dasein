'use strict';

module.exports = {
    protocols: {
        http: parseInt(process.env.ADMIN_HTTP_PORT) || undefined,
        https: parseInt(process.env.ADMIN_HTTPS_PORT) || undefined
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
        '/match': {
            'local_path': '/match',
            'remote_host': process.env.API_MATCHAPI_URL || 'http://localhost:8076',
            'remote_path': '/match',
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
