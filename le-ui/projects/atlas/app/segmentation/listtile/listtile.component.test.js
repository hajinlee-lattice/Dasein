import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeListTile from "./le-listtile";

describe("<LeListTile /> rendering", () => {
  it("<LeListTile /> with label and icon", () => {
    const wrapper = mount(
      <LeListTile
        config={{}}
      />
    );
    expect(wrapper.find(LeListTile).exists()).toBeTruthy();
  });
});
