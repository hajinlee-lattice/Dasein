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
// import "../../../common/widgets/layout/layout.scss";

import LeVPanel from "../../../common/widgets/container/le-v-panel";
import LeHPanel from "../../../common/widgets/container/le-h-panel";
import LeButton from "../../../common/widgets/buttons/le-button";

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

stories.add("Vertical Panel", () => (
  <div className="container">
    <LeVPanel
      vstretch={boolean("Vertical Stretch", false).toString()}
      hstretch={boolean("Horizontal Stretch", false).toString()}
    >
      <LeVPanel className="sub-container" halignment={radios("H Alignement", options, "left")} valignment={radios("V Alignement", vOptions, "top")}>
            <LeButton
              name="borderless"
              callback={action("button-click")}
              disabled={false}
              config={{icon: "config.icon fa fa-cloud-upload"}}
            />
          <span>1.1</span>
          <span>1.2</span>
          <span>1.3</span>
          <span>1.4</span>
      </LeVPanel>
    </LeVPanel>
  </div>
));
