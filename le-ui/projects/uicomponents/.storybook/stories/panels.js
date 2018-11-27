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
import "../../../common/widgets/layout/layout.scss";

import LeVPanel from "../../../common/widgets/container/le-v-panel";
import LeHPanel from "../../../common/widgets/container/le-h-panel";

const stories = storiesOf("Panels", module);

stories.addDecorator(withKnobs);

stories.add("hpanel", () => (
  <div className="container">
    <LeHPanel>
      <div className="sub-container-h">1</div>
      <div className="sub-container-h">2</div>
      <div className="sub-container-h">3</div>
    </LeHPanel>
  </div>
));
stories.add("hpanel fill space", () => (
  <div className="container">
    <LeHPanel fillspace>
      <LeHPanel rightAllign classes="sub-container-h">1</LeHPanel>
      <LeHPanel classes="sub-container-h ">2</LeHPanel>
      {/* <LeHPanel classes="sub-container-h">3</LeHPanel> */}
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
