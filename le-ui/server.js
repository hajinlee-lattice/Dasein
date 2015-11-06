"use strict";

const express     = require('express');
const path        = require('path');
const bodyParser  = require('body-parser');
const exphbs      = require('express-handlebars');
const request     = require('request');
const routes      = require('./server/routes');

const app         = express();
const mapping     = {
    'login': 'login',
    'common': 'common',
    'prospectdiscovery': 'pd',
    'leadprioritization': 'lp',
    'demo': 'demo'
};

const env         = app.get('env');
const use_port    = process.env.USE_PORT;
const api_proxy   = process.env.API_PROXY;
const api_url     = process.env.API_URL;

class Server {
    constructor() {
        // set up view engine for handlebars
        app.engine('.html', exphbs({extname: '.html'}));
        app.set('view engine', '.html');
        app.set('views', __dirname);

        // set up proxy to forward API requests
        if (api_proxy) {
            app.use('/pls', function(req, res) {
                const url = api_url + '/pls' + req.url;
                let r = null;

                console.log(env, req.method, url);
                if (req.method === 'POST') {
                    r = request.post({ 
                        uri: url, 
                        json: req.body 
                    });
                } else {
                    r = request(url);
                }

                req.pipe(r).pipe(res);
            });
        }

        Object.keys(mapping).forEach((key, i) => {
            const value = mapping[key];
            const appdir = __dirname + '\\projects\\' + key + '\\src';

            // static resources
            app.use([
                '/' + value + '/app',
                '/' + key + '/app'
            ], express.static(appdir + '/app'));
            app.use([
                '/' + value + '/assets',
                '/' + key + '/assets'
            ], express.static(appdir + '/assets'));
            app.use([
                '/' + value + '/lib',
                '/' + key + '/lib'
            ], express.static(appdir + '/lib'));

            // index
            app.get([
                '/' + value,
                '/' + value + '/index',
                '/' + key,
                '/' + key + '/index'
            ], (req, res) => res.render(appdir + '/index.html'));
        });

        // default routing to sign on page
        app.get([
            '/',
            '/index'
        ], (req, res) => res.redirect('/login'));

        // catch 404 and forwarding to error handler
        app.use((req, res, next) => {
            const err = new Error('Not Found');
            err.status = 404;
            next(err);
        });

        // print stack trace for dev environment
        if (env === 'development') {
            app.use((err, req, res, next) => {
                res.status(err.status || 500);
                res.render('./server/error', {
                    message: err.message,
                    error: err,
                    env: env
                });
            });
        }

        // no stacktraces leaked to user for production
        app.use((err, req, res, next) => {
            res.status(err.status || 500);
            res.render('./server/error', {
                message: err.message,
                error: 'hidden',
                env: env
            });
        });
    }

    start() {
        const server = app.listen(use_port, () => {
            console.log(
                'express port:', use_port, '(listening)',
                '\napi proxy:', api_proxy, 
                '\napi url:', api_url, 
                '\nenvironment:', env
            );
        });
    }
}

const server = new Server();

server.start();

module.export = server;