'use strict';

module.exports = function (grunt) {
    // version of our software. This should really be in the package.json
    // but it gets passed in through 
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();

    // Define the configuration for all the tasks
    grunt.initConfig({
        versionString: versionStringConfig,

        env: {
            dev: {
                API_URL: 'http://localhost:8080',
                API_PROXY: true,
                NODE_ENV: 'development',
                USE_PORT: 3000
            },
            integration: {
                API_URL: 'http://bodcdevhdpweb53.dev.lattice.local:8080',
                API_PROXY: true,
                NODE_ENV: 'integration',
                USE_PORT: 8080
            },
            qa: {
                API_URL: 'http://bodcdevhdpweb52.dev.lattice.local:8080',
                API_PROXY: true,
                NODE_ENV: 'qa',
                USE_PORT: 3000
            },
            stage: {
                API_URL: 'https://app.lattice-engines.com',
                API_PROXY: true,
                NODE_ENV: 'stage',
                USE_PORT: 8080
            },
            prod: {
                API_URL: 'https://app.lattice-engines.com',
                API_PROXY: false,
                NODE_ENV: 'production',
                USE_PORT: 8080
            }
        },

        nodemon: {
          default: {
            script: './server.js'
          }
        }
    });

    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-nodemon');
    grunt.loadNpmTasks('grunt-env');

    var defaultText = 'Run Express Server in Production';
    grunt.registerTask('default', defaultText, [
        'env:prod',
        'nodemon'
    ]);

    var devText = 'Run Express Server, using Local API Endpoints';
    grunt.registerTask('dev', devText, [
        'env:dev',
        'nodemon'
    ]);

    var qaText = 'Run Express Server, using API Endpoints on 52';
    grunt.registerTask('qa', qaText, [
        'env:qa',
        'nodemon'
    ]);

    grunt.registerTask('init', [

    ]);

    grunt.registerTask('dist', [

    ]);
};