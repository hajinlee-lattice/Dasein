import Component from "./segmentation.component.js";
import Config from "./segmentation.config.js";
import States from "./segmentation.states.js";
import { react2angular } from "common/react-vendor";

const Dependencies = [];

angular.module("atlas.segmentation", Dependencies).config(Config);
//     .component('atlasSegmentation', Component)
//     .component("leItemBar", react2angular(LeItemBar, ["store"], []))
//     .component("leItemView", react2angular(LeItemView, ["store"], []));
