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
                API_URL: 'http://localhost:8080'
            },
            integration: {
                NODE_ENV: 'integration',
                API_URL: 'http://bodcdevhdpweb53.dev.lattice.local:8080',
                USE_PORT: 3000
            },
            qa: {
                NODE_ENV: 'qa',
                API_URL: 'http://bodcdevhdpweb52.dev.lattice.local:8080',
                USE_PORT: 3000
            },
            stage: {
                NODE_ENV: 'stage',
                API_URL: 'https://app.lattice-engines.com',
                USE_PORT: 8080
            },
            prod: {
                NODE_ENV: 'production',
                API_URL: false,  // load balancer will handle api routing
                USE_PORT: 8080,
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
        'env:prod',
        'run:node'
    ]);

    grunt.registerTask('prod', defaultText, [
        'env:prod',
        'run:node'
    ]);

    var devText = 'Run Express Server, using Local API Endpoints';
    grunt.registerTask('dev', devText, [
        'env:dev',
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
    grunt.registerTask('qa2', qaText, [
        'env:qa',
        'run:nodemon'
    ]);

    var text = 'Kill all node.exe on windows';
    grunt.registerTask('killnode', text, [
        'run:killnode'
    ]);
};
