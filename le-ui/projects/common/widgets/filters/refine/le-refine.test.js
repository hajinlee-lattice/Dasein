import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeRefine from "./le-refine";

describe("<LeRefine /> rendering", () => {
  it("<LeRefine /> with label and icon", () => {
    const wrapper = mount(
      <LeRefine
        config={{}}
      />
    );
    expect(wrapper.find(LeRefine).exists()).toBeTruthy();
  });
});
