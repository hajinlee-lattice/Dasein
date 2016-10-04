"use strict";

module.exports = function (grunt) {
    // version of our software. This should really be in the package.json
    // but it gets passed in through 
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();

    // Define the configuration for all the tasks
    grunt.initConfig({
        versionString: versionStringConfig,

        // https://confluence.lattice-engines.com/pages/viewpage.action?pageId=16909888
        env: {
            dev: {
                NODE_APPS: 'leui',
                NODE_ENV: 'development',
                API_URL: 'https://testapp.lattice-engines.com',
                //API_URL: 'https://10.41.0.13:8081',
                API_ADMIN_URL: 'https://10.41.0.25:8085',
                API_CON_URL: 'https://testapi.lattice-engines.com:8073',
                API_MCSVC_URL: 'https://10.41.0.25:8080',
                API_INFLUXDB_URL: 'http://10.41.1.188:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            dev_admin: {
                NODE_APPS: 'leadmin',
                NODE_ENV: 'development',
                API_URL: 'https://testapp.lattice-engines.com',
                //API_URL: 'https://10.41.0.13:8081',
                API_ADMIN_URL: 'https://10.41.0.25:8085',
                API_CON_URL: 'https://testapi.lattice-engines.com:8073',
                API_MCSVC_URL: 'https://10.41.0.25:8080',
                API_INFLUXDB_URL: 'http://10.41.1.188:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            devb: {
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'development',
                API_URL: 'https://bodcdevsvipb13.lattice.local:8081',
                API_ADMIN_URL: 'https://10.41.0.26:8085',
                API_CON_URL: 'https://bodcdevsvipb26.lattice.local:8073',
                API_MCSVC_URL: 'https://10.41.0.26:8080',
                API_INFLUXDB_URL: 'http://10.41.1.188:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            local: {
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'development',
                API_URL: 'http://localhost:8081',
                API_ADMIN_URL: 'http://localhost:8085',
                API_CON_URL: 'http://localhost:8073',
                API_MCSVC_URL: 'http://localhost:8080',
                API_INFLUXDB_URL: 'http://localhost:8086',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            qa: {
                NODE_APPS: 'leui,leadmin',
                NODE_ENV: 'qa',
                API_URL: 'https://testapp.lattice-engines.com',
                //API_URL: 'https://bodcdevsvipb13.lattice.local', // for b
                API_ADMIN_URL: 'https://admin-qa.lattice.local:8085',
                API_CON_URL: 'https://testapi.lattice-engines.com',
                API_MCSVC_URL: 'https://admin-qa.lattice.local:8080',
                API_INFLUXDB_URL: 'http://localhost:8086',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.41.0.14, 10.41.0.16'
            },
            production: {
                NODE_APPS: 'leui',
                NODE_ENV: 'production',
                API_URL: 'https://app.lattice-engines.com',
                API_ADMIN_URL: ' https://admin.prod.lattice.local:8085/',
                API_CON_URL: 'https://api.lattice-engines.com',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.51.12.109, 10.51.51.109'
            },
            proddev: {
                NODE_APPS: 'leui',
                NODE_ENV: 'production',
                API_URL: 'https://app.lattice-engines.com',
                API_ADMIN_URL: 'https://admin.prod.lattice.local:8085/',
                API_CON_URL: 'https://api.lattice-engines.com',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                ADMIN_HTTP_PORT: 3003,
                ADMIN_HTTPS_PORT: 3002,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.51.12.109, 10.51.51.109'
            }
        },
        run: {
            node: {
                args: [ './app.js' ]
            },
            nodemon: {
                cmd: 'nodemon',
                args: [ './app.js' ]
            },
            pm2: {
                cmd: 'pm2.cmd',
                args: [ 'start', './app.js' ]
            },
            killnode: {
                cmd: 'taskkill.exe',
                args: [
                    '/F',
                    '/IM',
                    'node.exe'
                ]
            }
        }
    });

    grunt.loadNpmTasks('grunt-run');
    grunt.loadNpmTasks('grunt-env');

    var defaultText = 'Run Express Server in Production';
    grunt.registerTask('default', defaultText, [
        'env:production',
        'run:node'
    ]);

    grunt.registerTask('prod', defaultText, [
        'env:production',
        'run:node'
    ]);

    grunt.registerTask('proddev', defaultText, [
        'env:proddev',
        'run:node'
    ]);

    grunt.registerTask('production', defaultText, [
        'env:production',
        'run:node'
    ]);

    var devText = 'Run Express Server using external API (52?)';
    grunt.registerTask('dev', devText, [
        'env:dev',
        'run:node'
    ]);

    var devText = 'Run Express Server using external API (52?)';
    grunt.registerTask('devb', devText, [
        'env:devb',
        'run:node'
    ]);

    grunt.registerTask('pm2dev', devText, [
        'env:dev',
        'run:pm2'
    ]);

    grunt.registerTask('dev_admin', devText, [
        'env:dev_admin',
        'run:node'
    ]);

    grunt.registerTask('pm2', devText, [
        'env:qa',
        'run:pm2'
    ]);

    var devText = 'Run Express Server, using Local API Endpoints';
    grunt.registerTask('local', devText, [
        'env:local',
        'run:node'
    ]);

    var devText = 'Run Express Server, using Local API Endpoints';
    grunt.registerTask('localmon', devText, [
        'env:local',
        'run:nodemon'
    ]);

    var integrationText = 'Run Express Server, using 53 API Endpoints';
    grunt.registerTask('integration', integrationText, [
        'env:integration',
        'run:node'
    ]);

    var qaText = 'Run Express Server, using API Endpoints on 52';
    grunt.registerTask('stage', qaText, [
        'env:stage',
        'run:node'
    ]);

    var qaText = 'Run Express Server, using API Endpoints on 52';
    grunt.registerTask('qa', qaText, [
        'env:qa',
        'run:node'
    ]);

    var qaText = 'Run Express Server, using API Endpoints on 52';
    grunt.registerTask('qamon', qaText, [
        'env:qa',
        'run:nodemon'
    ]);

    var qaText = 'Run Express Server, using API Endpoints on 52';
    grunt.registerTask('devmon', qaText, [
        'env:dev',
        'run:nodemon'
    ]);

    var text = 'Kill all node.exe on windows';
    grunt.registerTask('killnode', text, [
        'run:killnode'
    ]);
};
