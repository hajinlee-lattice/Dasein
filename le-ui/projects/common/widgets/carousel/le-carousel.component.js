import React, { Component, $ } from "../../react-vendor";
import LeButton from '../buttons/le-button';
import "./le-carousel.component.scss";

class LeCarousel extends Component {

  constructor(props) {
    super(props);
    
    this.state = { viewPortIndex: 0, initial: this.props.numPerViewport ? this.props.numPerViewport : 3, numPerViewport: this.props.numPerViewport ? this.props.numPerViewport : 3, prevDisabled: true, nextDisabled: false };
    this.getPrevViewPort = this.getPrevViewPort.bind(this);
    this.getNextViewPort = this.getNextViewPort.bind(this);
    this.resized = this.resized.bind(this);
    this.maxNumber = this.props.children.length;
    console.log(this.maxNumber);
    this.start = 0;
    this.end = 0;
  }


  resized() {
    let div = $(this.elementRef);
    let children = div.children()[1];
    let viewport = $(children).children()[0];
    let elements = $(viewport).children();
    let singleElement = 0;
    let minElement = $(elements[0]).css('min-width') ? $(elements[0]).css('min-width').replace(/[^-\d\.]/g, '') : 100;
    let maxElement = $(elements[0]).css('max-width') ? $(elements[0]).css('max-width').replace(/[^-\d\.]/g, '') : 100;
    let allMin = true;
    let allMax = true;
    for (let i = 0; i < elements.length; i++) {
      let el = elements[i];
      singleElement = $(el).width();
      // console.log(singleElement, minElement, maxElement);
      if (singleElement > Number(minElement)) {
        allMin = false;
      }
      if (singleElement < Number(maxElement)) {
        allMax = false;
      }
    }
    if (allMin == true && this.state.numPerViewport > 1) {
      this.setState({ numPerViewport: this.state.numPerViewport - 1 });
      return;
    }
    if (allMax == true) {
      this.setState({ numPerViewport: this.state.numPerViewport + 1 });
      return;
    }
  }

  refCallback = element => {
    if (element) {
      this.elementRef = element;
    }
  };

  componentDidMount() {
    window.addEventListener("resize", this.resized);
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.resized);
  }

  getNextViewPort() {
    this.setState({ viewPortIndex: (this.state.viewPortIndex + 1) });
  }

  getPrevViewPort() {
    if (this.state.viewPortIndex > 0) {
      this.setState({ viewPortIndex: (this.state.viewPortIndex - 1) });
    }
  }

  getElementsViewPort() {
    let childrenViewPort = [];
    if (this.props.children) {
      this.start = Number(this.state.numPerViewport * this.state.viewPortIndex);
      // console.log(start);
      this.end = Number(this.start + this.state.numPerViewport);
      // console.log(start, ' == ', end);
      for (var i = this.start; i < this.end; i++) {
        childrenViewPort.push(<LeCarouselElement elementsStyle={this.props.elementsStyle}>{this.props.children[i]}</LeCarouselElement>);
      }
    }
    // console.log(childrenViewPort.length, this.end);
    return childrenViewPort;
  }

  render() {
    return (
      <div className="le-carousel-container" ref={this.refCallback} >
        <div className="le-carousel-left-control">
          <LeButton
            disabled={this.state.viewPortIndex == 0}
            name="borderless"
            callback={this.getPrevViewPort}
            config={{
              classNames: "borderless-button",
              icon: "fa fa-chevron-left"
            }}
          />
        </div>
        <div className="le-carousel-content">
          <div className="le-carousel-viewport">
            {this.getElementsViewPort()}
          </div>
        </div>
        <div className="le-carousel-right-control">
          <LeButton
          disabled={(this.end * this.state.viewPortIndex >= this.maxNumber) || (this.end >= this.maxNumber)}
            name="borderless"
            callback={this.getNextViewPort}
            config={{
              classNames: "borderless-button",
              icon: "fa fa-chevron-right"
            }}
          />
        </div>
      </div>
    );
  }
}

export default LeCarousel;

class LeCarouselElement extends Component {

  constructor(props) {
    super(props);
    this.elementsStyle = props.elementsStyle ? props.elementsStyle : {};//{ maxWidth: '400px', border: '1px solid red'};
  }

  render() {
    return (
      <div className="le-carousel-element" style={this.elementsStyle}>
        {this.props.children}
      </div>
    );
  }
}