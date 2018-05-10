const path = require('path');
const fs = require('fs-extra');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const WebpackShellPlugin = require('webpack-shell-plugin');
const WebpackOnBuildPlugin = require('on-build-webpack');
const UglifyJSPlugin = require('uglifyjs-webpack-plugin');

module.exports = env => {
    console.log('--------------> ', env);
    var config = {mode: ""};
    if(env !== undefined){
         config = JSON.parse(JSON.stringify(env));
    }
    console.log('************** ', config.mode);
    // console.log('*******************> ',process.env.mode);
    // if (env.mode == 'prod') {
    //     console.log('You are starting in Production mode'); // true
    // } else {
    //     // console.log('You are starting in Dev mode');
    // }
    return {
        entry: './src/dev-main.js',
        output: {
            path: path.join(__dirname, '/dist'),
            filename: 'bundle.js',
            publicPath: '/'
        },
        module: {
            rules: [{
                    test: /\.js$/,
                    exclude: /node_module/,
                    use: {
                        loader: 'babel-loader'
                    }
                },
                {
                    test: /\.html$/,
                    use: 'raw-loader'
                },
                {
                    test: /\.s?css/,
                    use: [
                        "style-loader",
                        "css-loader",
                        "sass-loader"
                    ]
                },
                {
                    test: /\.(png|svg|jpg|gif)$/,
                    use: [
                        'file-loader'
                    ]
                }
            ]
        },
        devtool: 'source-map',
        plugins: [
            new HtmlWebpackPlugin({
                template: './src/index.html'
            }),
            new WebpackShellPlugin({
                onBuildStart: ['echo "LE-Widgets build Starts"'],
                onBuildEnd: ['echo "LE-Widgets build Ended"']
            }),
            
            new WebpackOnBuildPlugin(function (stats) {
                if (config.mode == 'prod') {
                    try {
                        fs.remove('./dist', err =>{
                            if (err) return console.error(err);

                            console.log('success!'); // I just deleted my entire HOME directory.
                        });
                        // fs.emptyDir('./tmp', err => {

                        // });
                        fs.copySync('./dist/le-widgets.js', '../common/lib/bower/le-widgets.js');

                    } catch (err) {
                        console.error(err)
                    }
                }
            }),
            new UglifyJSPlugin({
                sourceMap: true
            })
        ],
        externals: {
            "angular": "angular" // provides a way of excluding dependencies from the output bundles
        },
        devServer: {
            port: 8888, // configuring port for localserver
            historyApiFallback: true
        }
    }
}