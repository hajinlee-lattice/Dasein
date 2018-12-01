import React from "../../../common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import { withKnobs, boolean, radios } from "@storybook/addon-knobs";
import "../../../common/assets/css/font-awesome.min.css";
import "./panel.scss";

import LeVPanel from "../../../common/widgets/container/le-v-panel";
import LeHPanel from "../../../common/widgets/container/le-h-panel";
import LeButton from "../../../common/widgets/buttons/le-button";
import {
  LEFT,
  RIGHT,
  TOP,
  BOTTOM,
  CENTER,
  SPACEAROUND,
  SPACEBETWEEN,
  SPACEEVEN
} from "../../../common/widgets/container/le-alignments";
const stories = storiesOf("Panels", module);

stories.addDecorator(withKnobs);

const options = {
  Left: LEFT,
  Center: CENTER,
  Right: RIGHT
};

const vOptions = {
  Top: TOP,
  Center: CENTER,
  Bottom: BOTTOM,
  "Space Around": SPACEAROUND,
  "Space between": SPACEBETWEEN,
  "Space even": SPACEEVEN
};

stories.add("Horizontal panel", () => (
  <div className="container">
    <LeHPanel
      hstretch={boolean("Horizontal Stretch", false).toString()}
      className="sub-container"
      halignment={radios(
        "H Alignement",
        {
          Left: LEFT,
          Center: CENTER,
          Right: RIGHT,
          "Space Around": SPACEAROUND,
          "Space between": SPACEBETWEEN,
          "Space even": SPACEEVEN
        },
        LEFT
      )}
      vstretch={boolean("Vertical Stretch", false).toString()}
      valignment={radios(
        "Vertical Alignement",
        {
          Top: TOP,
          Center: CENTER,
          Bottom: BOTTOM
        },
        TOP
      )}
      wrap={boolean("Wrap", false)}
    >
      <LeButton
        name="borderless"
        callback={action("button-click")}
        disabled={false}
        config={{ icon: "config.icon fa fa-cloud-upload" }}
      />
      <div className="container-test">
        <span>1.1</span>
      </div>
      <span>1.2</span>
      <span>1.3</span>
      <span>1.4</span>
    </LeHPanel>
  </div>
));

stories.add("Vertical Panel", () => (
  <div className="container">
    <LeVPanel
      vstretch={boolean("Vertical Stretch", false).toString()}
      valignment={radios("V Alignement", vOptions, TOP)}
      hstretch={boolean("Horizontal Stretch", false).toString()}
      className="sub-container"
      halignment={radios("H Alignement", options, LEFT)}
    >
      <LeButton
        name="borderless"
        callback={action("button-click")}
        disabled={false}
        config={{ icon: "config.icon fa fa-cloud-upload" }}
      />
      <span>1.1</span>
      <span>1.2</span>
      <span>1.3</span>
      <span>1.4</span>
    </LeVPanel>
  </div>
));
