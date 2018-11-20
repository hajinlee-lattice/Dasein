import React from "../../../common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number
} from "@storybook/addon-knobs";
import "../../../common/assets/css/font-awesome.min.css";
import LeButton, {
  LEFT,
  RIGHT
} from "../../../common/widgets/buttons/le-button";

const stories = storiesOf("Buttons", module);

stories.addDecorator(withKnobs);

const labelColors = "config.classNames";
const options = ["orange-button", "blue-button", "gray-button"];
const defaultColorValue = "orange-button";

const labelState = "disabled?";
const defaultStateValue = false;

stories.add("rich button", () => (
  <LeButton
    name="rich-button"
    callback={action("button-click")}
    disabled={boolean(labelState, defaultStateValue)}
    config={{
      label: text("config.label", "Lattice Engines"),
      classNames: select(labelColors, options, defaultColorValue),
      icon: text("config.icon", "fa fa-check"),
      iconside: select("config.iconside", [LEFT, RIGHT], LEFT)
    }}
  />
));

stories.add("borderless button", () => (
  <LeButton
    name="borderless"
    callback={action("button-click")}
    disabled={boolean(labelState, defaultStateValue)}
    config={{
      classNames: select(
        labelColors,
        ["borderless-button"],
        "borderless-button"
      ),
      icon: text("config.icon", "fa fa-cloud-upload")
    }}
  />
));
