const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const CleanWebpackPlugin = require('clean-webpack-plugin');
//const webpack = require('webpack');

module.exports = {
    resolve: {
        alias: {
            common: path.resolve(__dirname, '../common'),
            app: path.resolve(__dirname, '../common/app'),
            store: path.resolve(__dirname, '../common/app/store'),
            components: path.resolve(__dirname, '../common/components'),
            widgets: path.resolve(__dirname, '../common/widgets'),
            atlas: path.resolve(__dirname, 'app'),
            assets: path.resolve(__dirname, 'assets')
        }
    },

    entry: {
        common_angular: '../common/angular-vendor.index.js',
        common_vendor: '../common/vendor.index.js',
        common_visualization: '../common/vendor-visualization.index.js',
        common_app: '../common/app/app.index.js',
        common_components: '../common/components/components.index.js',
        common_styles: '../common/assets/sass/lattice.scss',
        atlas_app_1: './index.js',
        atlas_app_2: './index_2.js',
        atlas_sass: './assets/styles/main.scss'
    },

    optimization: {

    },

    plugins: [
        new CleanWebpackPlugin(['dist']),
        new HtmlWebpackPlugin({
            filename: 'indexwp.html',
            template: __dirname + '/indexwp.html'
        })
    ],

    output: {
        filename: '[name].' + Date.now() + '.bundle.js',
        path: path.resolve(__dirname, 'dist')
    },

    module: {
        rules: [
            {
                test: /\.(png|svg|jpg|gif)$/,
                exclude: '/node_modules/',
                use: ['file-loader']
            },
            {
                test: /\.(woff|woff2|eot|ttf|otf)$/,
                exclude: '/node_modules/',
                use: ['file-loader']
            },
            {
                test: /\.html$/,
                exclude: '/node_modules/',
                use: ['raw-loader']
            },
            {
                test: /\.js$/,
                exclude: /node_modules/,
                use: { loader: 'babel-loader', options:{
                    presets: [
                        "@babel/preset-env",
                        "@babel/preset-react"
                    ],
                    plugins: [
                        "@babel/plugin-transform-spread",
                        "angularjs-annotate",
                        "@babel/plugin-syntax-dynamic-import",
                        "@babel/plugin-syntax-import-meta",
                        "@babel/plugin-proposal-class-properties",
                        "@babel/plugin-proposal-json-strings",
                        [
                            "@babel/plugin-proposal-decorators",
                            {
                                "legacy": true
                            }
                        ],
                        "@babel/plugin-proposal-function-sent",
                        "@babel/plugin-proposal-export-namespace-from",
                        "@babel/plugin-proposal-numeric-separator",
                        "@babel/plugin-proposal-throw-expressions"
                    ]
                    } 
                }
            }
        ]
    }
};
