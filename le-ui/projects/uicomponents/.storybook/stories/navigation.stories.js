import React from "common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number
} from "@storybook/addon-knobs";
import "common/assets/css/font-awesome.min.css";
import "common/widgets/layout/layout.scss";

import LeMenu from "common/widgets/menu/le-menu";
import LeMenuItem from "common/widgets/menu/le-menu-item";

import LeTabs from "common/widgets/tabs/le-tabs";
import LeTab from "common/widgets/tabs/le-tab";
import { getData } from "common/widgets/tabs/tabs-config";

const stories = storiesOf("Navigation", module);
stories.addDecorator(withKnobs);

stories.add("'meatball kabob' menu", () => (
  <LeMenu
    classNames="personalMenu"
    image={text("image", "fa fa-ellipsis-v")}
    label={text("label", "")}
    name="main"
  >
    <LeMenuItem
      name="edit"
      label="Edit"
      image="fa fa-pencil-square-o"
      callback={name => {
        console.log("NAME ", name);
      }}
    />

    <LeMenuItem
      name="duplicate"
      label="Duplicate"
      image="fa fa-files-o"
      callback={name => {
        console.log("NAME ", name);
      }}
    />

    <LeMenuItem
      name="delete"
      label="Delete"
      image="fa fa-trash-o"
      callback={name => {
        console.log("NAME ", name);
      }}
    />
  </LeMenu>
));


let data = getData();
let modelCounts = data.modelCounts;
let historyCounts = data.historyCounts;

stories.add("tabs", () => (
  <LeTabs
    justified="false"
  >
    <LeTab
      name="models"
      label="Models"
      classNames="active"
      counts={ modelCounts }
      callback={name => {
        console.log("NAME ", name);
      }}
    />
    <LeTab
      name="creationHistory"
      label="Creation History"
      classNames=""
      counts={ historyCounts }
      callback={name => {
        console.log("NAME ", name);
      }}
    />
  </LeTabs>
));
