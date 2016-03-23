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
                API_URL: 'http://bodcdevhdpweb52.dev.lattice.local:8080',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3000,
                HTTPS_PORT: 3001,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            local: {
                NODE_ENV: 'development',
                API_URL: 'http://localhost:8081',
                COMPRESSED: false,
                LOGGING: './server/log',
                HTTP_PORT: 3000,
                HTTPS_PORT: 3001,
                HTTPS_KEY: './server/certs/privatekey.key',
                HTTPS_CRT: './server/certs/certificate.crt',
                HTTPS_PASS: false
            },
            integration: {
                NODE_ENV: 'integration',
                API_URL: 'http://bodcdevhdpweb53.dev.lattice.local:8080',
                COMPRESSED: true,
                LOGGING: '/var/log/ledp',
                HTTP_PORT: false,
                HTTPS_PORT: 3000,
                HTTPS_KEY: '/etc/pki/tls/private/private.key',
                HTTPS_CRT: '/etc/pki/tls/star_lattice-engines_com.crt',
                HTTPS_PASS: 'Lattice1'
            },
            qa: {
                NODE_ENV: 'qa',
                API_URL: 'http://bodcdevhdpweb52.dev.lattice.local:8080',
                COMPRESSED: true,
                LOGGING: './server/log',
                HTTP_PORT: 3000
            },
            production: {
                NODE_ENV: 'production',
                API_URL: false,
                COMPRESSED: true,
                LOGGING: '/var/log/ledp',
                HTTP_PORT: false,
                HTTPS_PORT: 3000,
                HTTPS_KEY: '/etc/pki/tls/private/private.key',
                HTTPS_CRT: '/etc/pki/tls/star_lattice-engines_com.crt',
                HTTPS_PASS: 'Lattice1',
                WHITELIST: [
                    '10.0.0.1',
                    '10.0.10.1'
                ]
            }
        },

        run: {
            node: {
                args: [ './app.js' ]
            },
            nodemon: {
                cmd: 'nodemon.cmd',
                args: [ './app.js' ]
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

    var devText = 'Run Express Server, using Local API Endpoints';
    grunt.registerTask('local', devText, [
        'env:local',
        'run:node'
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
