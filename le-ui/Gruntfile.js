"use strict";

module.exports = function (grunt) {
    // version of our software. This should really be in the package.json
    // but it gets passed in through 
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();

    // Define the configuration for all the tasks
    grunt.initConfig({
        versionString: versionStringConfig,

        env: {
            dev: {
                NODE_ENV: 'development',
                API_URL: 'https://testapp.lattice-engines.com',
                APICON_URL: 'https://testapi.lattice-engines.com:8073',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            devb: {
                NODE_ENV: 'development',
                API_URL: 'https://bodcdevsvipb13.lattice.local:8081',
                APICON_URL: 'https://bodcdevsvipb26.lattice.local:8073',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            local: {
                NODE_ENV: 'development',
                API_URL: 'http://localhost:8081',
                APICON_URL: 'http://localhost:8073',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            qa: {
                NODE_ENV: 'qa',
                API_URL: 'https://testapp.lattice-engines.com',
                APICON_URL: 'https://testapi.lattice-engines.com',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false,
                WHITELIST: '10.41.0.14, 10.41.0.16'
            },
            production: {
                NODE_ENV: 'production',
                API_URL: 'https://app.lattice-engines.com',
                APICON_URL: 'https://api.lattice-engines.com',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3001,
                HTTPS_PORT: 3000,
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
