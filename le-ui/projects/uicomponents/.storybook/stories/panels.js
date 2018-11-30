import React from "../../../common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number,
  radios
} from "@storybook/addon-knobs";
import "../../../common/assets/css/font-awesome.min.css";
import "../../../common/widgets/layout/layout.scss";

import LeVPanel from "../../../common/widgets/container/le-v-panel";
import LeHPanel from "../../../common/widgets/container/le-h-panel";

const stories = storiesOf("Panels", module);

stories.addDecorator(withKnobs);

const options = {
  Left: "left",
  Center: "center",
  Right: "right"
};

const vOptions = {
  Top: "top",
  Center: "center",
  Bottom: "bottom"
};

stories.add("Horizontal panel", () => (
  <div className="container">
    <LeHPanel
      hstretch={boolean("Horizontal Stretch", false).toString()}
      vstretch={boolean("Vertical Stretch", false)}
      wrap={boolean("Wrap", false)}
    >
      <LeHPanel
        hAlignment={radios("P1 H Alignement", options, "left")}
        vAlignment={radios("P1 V Alignement", vOptions, "top")}
        classes="sub-container-h"
      >
        <span>1</span>
      </LeHPanel>
      <LeHPanel
        hAlignment={radios("P2 H Alignement", options, "left")}
        vAlignment={radios("P2 V Alignement", vOptions, "top")}
        classes="sub-container-h"
      >
        <span>2</span>
      </LeHPanel>
    </LeHPanel>
  </div>
));
stories.add("vpanel", () => (
  <div className="container">
    <LeVPanel>
      <div className="sub-container">1</div>
      <div className="sub-container">2</div>
      <div className="sub-container">3</div>
    </LeVPanel>
  </div>
));

stories.add("vpanel fill space", () => (
  <div className="container">
    <LeVPanel fillspace>
      <div className="sub-container">1</div>
      {/* <div className="sub-container">2</div>
      <div className="sub-container">3</div> */}
    </LeVPanel>
  </div>
));
