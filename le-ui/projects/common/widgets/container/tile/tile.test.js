import React from "common/react-vendor";
import { mount } from "enzyme";
import LeTile from "./le-tile";
import LeTileHeader from "./le-tile-header";
import LeTileBody from "./le-tile-body";
import LeTileFooter from "./le-tile-footer";

describe("<LeTile /> rendering", () => {
  it("<LeTile> structure (LeTile, LeTileHeader, LeTileBody, LeTileFooter) rendered", () => {
    const wrapper = mount(
      <LeTile>
        <LeTileHeader>
          <span className="le-tile-title">Test 1234</span>
        </LeTileHeader>
        <LeTileBody>
          <p>The description here</p>
          <span>The body</span>
        </LeTileBody>
        <LeTileFooter>
          <div className="le-flex-v-panel fill-space">
            <span>Account</span>
            <span>123</span>
          </div>
          <div className="le-flex-v-panel fill-space">
            <span>Contacts</span>
            <span>3</span>
          </div>
        </LeTileFooter>
      </LeTile>
    );
    expect(wrapper.find(LeTile)).toBeTruthy();
    expect(wrapper.find(LeTile)).toContainMatchingElement(LeTileHeader);
    expect(wrapper.find(LeTile)).toContainMatchingElement(LeTileBody);
    expect(wrapper.find(LeTile)).toContainMatchingElement(LeTileFooter);
  });
});

describe("<LeTileHeader /> rendering", () => {
  it("LeTileHeader content rendered", () => {
    const wrapper = mount(
      <LeTile>
        <LeTileHeader>
          <span className="le-tile-title">Lattice Engines Test</span>
        </LeTileHeader>
      </LeTile>
    );
    expect(wrapper.find(LeTileHeader).exists()).toBeTruthy();
    expect(wrapper.find(LeTileHeader).text()).toContain("Lattice Engines Test");
    expect(wrapper.find(LeTileBody).exists()).not.toBeTruthy();
    expect(wrapper.find(LeTileFooter).exists()).not.toBeTruthy();
  });
});

describe("<LeTileBody /> rendering", () => {
  it("LeTileBody content rendered", () => {
    const wrapper = mount(
      <LeTile>
        <LeTileBody>
          <p>The description here</p>
          <span>The body</span>
        </LeTileBody>
      </LeTile>
    );
    expect(wrapper.find(LeTileHeader).exists()).not.toBeTruthy();
    expect(wrapper.find(LeTileBody).exists()).toBeTruthy();
    expect(wrapper.find(LeTileBody).text()).toContain("The body");
    expect(wrapper.find(LeTileFooter).exists()).not.toBeTruthy();
  });
});

describe("<LeTileFooter /> rendering", () => {
  it("LeTileBody content rendered", () => {
    const wrapper = mount(
      <LeTile>
        <LeTileFooter>
          <div className="le-flex-v-panel fill-space">
            <span>Account</span>
            <span>123</span>
          </div>
          <div className="le-flex-v-panel fill-space">
            <span>Contacts</span>
            <span>3</span>
          </div>
        </LeTileFooter>
      </LeTile>
    );
    expect(wrapper.find(LeTileHeader).exists()).not.toBeTruthy();
    expect(wrapper.find(LeTileBody).exists()).not.toBeTruthy();
    expect(wrapper.find(LeTileFooter).exists()).toBeTruthy();
    expect(wrapper.find(LeTileFooter).text()).toContain("Contacts");
  });
});
