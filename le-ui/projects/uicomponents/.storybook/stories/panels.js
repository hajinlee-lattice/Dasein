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

import LeMenu from "../../../common/widgets/menu/le-menu";
import LeMenuItem from "../../../common/widgets/menu/le-menu-item";
import LeVPanel from "../../../common/widgets/container/le-v-panel";
import LeButton from "../../../common/widgets/buttons/le-button";

const stories = storiesOf("Panels", module);

stories.addDecorator(withKnobs);

stories.add("hpanel", () => <p>TODO</p>);

stories.add("vpanel", () => <LeVPanel />);

stories.add("vpanel fill space", () => (
  <LeVPanel fillspace>
    <div>
      <LeButton
        name="rich-button"
        callback={action("button-click")}
        disabled="false"
        config={{
          label: "Test",
          classNames: "orange-button"
        }}
      />
    </div>
  </LeVPanel>
));
