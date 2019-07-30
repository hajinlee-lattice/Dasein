import Component from './segmentation.component.js';
import Config from './segmentation.config.js';
import LeItemBar from "widgets/itembar/le-itembar";
import LeItemView from "widgets/itemview/le-itemview";
import { react2angular } from "common/react-vendor";

const Dependencies = [];

angular
    .module('atlas.segmentation', Dependencies)
    .config(Config)
    .component('atlasSegmentation', Component);
    // .component("leItemBar", react2angular(LeItemBar, ["store"], []))
    // .component("leItemView", react2angular(LeItemView, ["store"], []));