const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CleanWebpackPlugin = require("clean-webpack-plugin");
const webpack = require("webpack");

module.exports = {
  entry: {
    widgets: "./index.js"
  },
  devtool: "module-source-map",
  plugins: [
    new CleanWebpackPlugin(["dist"]),
    new HtmlWebpackPlugin({
      filename: "index.html",
      template: __dirname + "/index.html"
    })
  ],

  output: {
    devtoolModuleFilenameTemplate(info) {
      return `file:///${info.absoluteResourcePath.replace(/\\/g, '/')}`;
    },
    filename: "[name].bundle.js",
    path: path.resolve(__dirname, "dist")
  },
  module: {
    rules: [
      {
        test: /\.s?css/,
        use: [
          { loader: "style-loader" },
          {
            loader: "css-loader",
            options: {
              sourceMap: false
            }
          },
          {
            loader: "sass-loader",
            options: {
              sourceMap: false
            }
          }
        ]
      },
      { test: /\.(png|svg|jpg|gif)$/, use: ["file-loader"] },
      { test: /\.(woff|woff2|eot|ttf|otf)$/, use: ["file-loader"] },
      { test: /\.html$/, use: ["raw-loader"] },
      {
        test: /\.*\.js$/,
        exclude: /node_modules/,
        use: ["ng-annotate-loader"]
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: {
          loader: "babel-loader"
        }
      }
    ]
  }
};
