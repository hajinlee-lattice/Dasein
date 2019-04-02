import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./barchart.scss";

export default class LeBarchart extends Component {
  
  constructor(props) {
    super(props);
    this.defaultColors = ['#70BF4A','#33BDB7','#65B7E6','#457EBA','#CD80BC','#8E71B2'];

    this.state = {
      orientation: this.props.orientation || 'horizontal',
      children: this.props.children || null,
      childrenPlacement: this.props.childrenPlacement || 'bottom',
      colors: this.props.colors || [],
      data: this.props.data || [],
      limit: this.props.limit || 0,
      callback: this.props.callback || null
    };

    this.clickHandler = this.clickHandler.bind(this);

    if(this.props.debug) {
      console.log('LeBarchart', this.props);
    }
    if(!this.props.data) {
      console.error('Missing data for LeBarchart', this.props);
    }
  }

  clickHandler(event, item) {
    if(this.props.callback && typeof this.props.callback === 'function') {
      this.props.callback(this.props.name, this.state, event, item);
    }
  }

  getData(data) {
    var chart = {
      total: 0,
      items: []
    },
    clickHandler = this.clickHandler,
    limit = this.props.limit,
    colors = this.props.colors,
    defaultColors = this.defaultColors;

    for(var i in data) {
      var value = data[i],
          orientation = this.props.orientation || 'horizontal';
      chart.items.push(value);
      chart.total = chart.total + value.value;
    }

    var chartHorizontalTemplate = [],
        chartVerticalTemplate = {
          labels: [],
          bars: [],
          values: []
        };

    chart.items.forEach(function(item, index) {
      var percent = Math.floor((item.value / chart.total) * 100);

      chart.items[index].percent = percent

      var style = {};
      
      if(colors && colors[index] && colors[index] !== 'null') {
        style.backgroundColor = colors[index];
      } else if(defaultColors && defaultColors[index]) {
        style.backgroundColor = defaultColors[index];
      } else {
        style.backgroundColor = '#'+Math.floor(Math.random()*16777215).toString(16);
      }

      if(limit && index >= limit) {
        return false;
      }
      if(orientation === 'vertical') {
        style.maxWidth = percent + '%';
        chartVerticalTemplate.labels.push(
          <strong>{item.label}</strong>
        );
        chartVerticalTemplate.bars.push(
          <div>
            <div className="bar" style={style} data-label={item.label} data-value={item.value}></div>
            <em>{item.value.toLocaleString()}</em>
          </div>
        );
      } else {style.maxHeight = percent + '%';
        chartHorizontalTemplate.push(
          <li key={index} onClick={(event) => {return clickHandler(event, item) }}>
            <em>{item.value.toLocaleString()}</em>
            <div style={style} data-label={item.label} data-value={item.value}></div>
            <strong>{item.label}</strong>
          </li>
        );
      }
    });

    if(orientation === 'vertical') {
      return (
        <ul>
          <li>
            {chartVerticalTemplate.labels}
          </li>
          <li className="bar-container">
            {chartVerticalTemplate.bars}
          </li>
        </ul>
      );
    } else {
      return (
        <ul>
        {chartHorizontalTemplate}
        </ul>
      );
    }
  }

  render() {
    return (
      <div className={`le-chart-barchart orientation-${this.props.orientation || 'horizontal'}`}>
        {this.props.childrenPlacement === 'top' ? this.props.children : null}
        {this.getData(this.props.data)}
        {this.props.childrenPlacement === 'bottom' ? this.props.children : null}
      </div>
      );
    }
  }

  LeBarchart.propTypes = {
    orientation: propTypes.string,
    colors: propTypes.array,
    childrenPlacement: propTypes.string,
    limit: propTypes.number,
    data: propTypes.object.isRequired,
    callback: propTypes.func
  };
