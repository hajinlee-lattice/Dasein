const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CleanWebpackPlugin = require("clean-webpack-plugin");
const webpack = require("webpack");

module.exports = {
  entry: {
    angular: "./angular-vendor.js",
    vendor: "./vendor.index.js"
  },
  devtool: "inline-source-map",
  plugins: [
    new CleanWebpackPlugin(["dist"]),
    new HtmlWebpackPlugin({
      filename: "index.html",
      template: __dirname + "/index.html"
    }),
  ],
  optimization: {
    minimizer: [
      new UglifyJsPlugin({
        exclude: /\/node_modules/,
        parallel: 4})
    ],
    splitChunks: {
      // chunks: "all"
    }
  },
  output: {
    filename: "[name].bundle.js",
    path: path.resolve(__dirname, "dist")
  },
  module: {
    rules: [
      { test: /\.s?css/, use: ["style-loader", "css-loader", "sass-loader"] },
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
        use: { loader: "babel-loader" }
      }
    ]
  }
};
