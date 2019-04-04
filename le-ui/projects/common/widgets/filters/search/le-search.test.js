import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeSearch from "./le-search";

describe("<LeSearch /> rendering", () => {
  it("<LeSearch /> with label and icon", () => {
    const wrapper = mount(
      <LeSearch
        config={{}}
      />
    );
    expect(wrapper.find(LeSearch).exists()).toBeTruthy();
  });
});
