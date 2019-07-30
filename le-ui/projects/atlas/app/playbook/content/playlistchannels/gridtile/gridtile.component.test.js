import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeGridTile from "./le-gridtile";

describe("<LeGridTile /> rendering", () => {
  it("<LeGridTile /> with label and icon", () => {
    const wrapper = mount(
      <LeGridTile
        config={{}}
      />
    );
    expect(wrapper.find(LeGridTile).exists()).toBeTruthy();
  });
});
