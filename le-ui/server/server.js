"use strict";

/*
            Lattice Engines Express Server
    See Gruntfile.js to define environment variables
*/

const path        = require('path');
const exphbs      = require('express-handlebars');
const request     = require('request');

class Server {
    constructor(express, app, options) {
        this.express = express;
        this.app = app;
        this.options = options;
        
        // set up view engine for handlebars
        this.app.engine('.html', exphbs({ extname: '.html' }));
        this.app.set('view engine', '.html');
        this.app.set('views', options.root);

        //process.on('uncaughtException', err => this.app.close());
        //process.on('SIGTERM', err => this.app.close());
    }

    // trust the load balancer/proxy in production
    trustProxy(WHITELIST) {
        this.app.set('trust proxy', ip => {
            const ips = WHITELIST.split(',');
            
            ips.forEach(current_ip => {
                if (current_ip === ip) {
                    return true;
                }
            });

            return false;
        });
    }

    // forward API requests for dev
    useApiProxy(API_URL) {
        if (API_URL) {
            this.app.use('/pls', (req, res) => {
                const url = API_URL + '/pls' + req.url;
                let r = null;

                try {
                    if (req.method === 'POST') {
                        r = request.post({ 
                            uri: url, 
                            json: req.body 
                        });
                    } else {
                        r = request(url);
                    }

                    req.pipe(r).pipe(res);
                } catch(err) {
                    console.log(err.msg);
                }
            });
        }
    }

    setAppRoutes(routes) {
        routes.forEach(route => {
            const dir = this.options.root + route.path;
            console.log('path:',dir);
            // set up the static routes for app files
            if (route.folders) {
                Object.keys(route.folders).forEach(folder => {
                    console.log('folder:',folder,route.folders[folder]);
                    this.app.use(
                        folder, 
                        this.express.static(dir + route.folders[folder])
                    );
                });
            }

            // users will see the desired render page when entering these routes
            if (route.pages) {
                Object.keys(route.pages).forEach(page => {
                    console.log('page:',page,route.pages[page]);
                    this.app.get(
                        page, 
                        (req, res) => res.render(dir + '/' + route.pages[page])
                    );
                });
            }
        });
    }

    setDefaultRoutes(ENV) {
        // catch 404 and forwarding to error handler
        this.app.use((req, res, next) => {
            const err = new Error('Not Found');
            err.status = 404;
            next(err);
        });

        // print stack trace for dev environment
        if (ENV === 'development') {
            this.app.use((err, req, res, next) => {
                res.status(err.status || 500);
                res.render('./server/error', {
                    message: err.message,
                    error: err,
                    env: ENV
                });
            });
        }

        // no stacktraces leaked to user for production
        this.app.use((err, req, res, next) => {
            res.status(err.status || 500);
            res.render('./server/error', {
                message: err.message,
                error: 'hidden',
                env: ENV
            });
        });
    }

    start() {
        const options = this.options;
        const server = this.app.listen(options.USE_PORT, () => {
            console.log(
                'listening port:', options.USE_PORT + ',', 
                'environment:', options.ENV, '\n' +
                'proxy api:', options.API_URL
            );
        });
    }
}

module.exports = Server;