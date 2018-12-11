import React from '../../react-vendor.js';
import {shallow, mount } from 'enzyme';
import LeButton from './le-button';

describe('<LeButton />', () => {
    it('renders <LeButton /> component disabled', () => {
        const wrapper = shallow(
            <LeButton name="disable-button" 
                    config={{label: 'Test'}} 
                    disabled={true} />);
        // expect(wrapper.find('.icon-star')).to.have.lengthOf(1);
    });

  
    // it('renders an `.icon-star`', () => {
    //   const wrapper = shallow(<MyComponent />);
    //   expect(wrapper.find('.icon-star')).to.have.lengthOf(1);
    // });
  
    // it('renders children when passed in', () => {
    //   const wrapper = shallow((
    //     <MyComponent>
    //       <div className="unique" />
    //     </MyComponent>
    //   ));
    //   expect(wrapper.contains(<div className="unique" />)).to.equal(true);
    // });
  
    // it('simulates click events', () => {
    //   const onButtonClick = sinon.spy();
    //   const wrapper = shallow(<Foo onButtonClick={onButtonClick} />);
    //   wrapper.find('button').simulate('click');
    //   expect(onButtonClick).to.have.property('callCount', 1);
    // });
  });