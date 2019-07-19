import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./barchart.scss";

export default class LeBarchart extends Component {

    constructor(props) {
        super(props);
        this.defaultColors = ['#70BF4A','#33BDB7','#65B7E6','#457EBA','#CD80BC','#8E71B2'];

        var _labels = [];

        this.state = {
            orientation: this.props.orientation || 'horizontal',
            type: this.props.type || 'simple',
            showValues: this.props.showValues || true,
            showAdditionalInfo: this.props.showAdditionalInfo || true,
            children: this.props.children || null,
            childrenPlacement: this.props.childrenPlacement || 'bottom',
            colors: this.props.colors || [],
            labels: this.props.labels || [],
            data: this.props.data || [],
            limit: this.props.limit || 0,
            liftsPostfix: this.props.liftsPostfix || 'x',
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

    getLabel(labels, index) {
        var _labels = [];
        labels.forEach(function(label) {
            if(label === 'null') {
                label = null;
            }
            _labels.push(label);
        });
        return _labels[index];
    }

    getData(data) {
        var chart = {
            total: 0,
            items: []
        },
        clickHandler = this.clickHandler,
        type = this.props.type,
        showValues = this.props.showValues,
        showAdditionalInfo = this.props.showAdditionalInfo, 
        limit = this.props.limit,
        liftsPostfix = this.props.liftsPostfix,
        colors = this.props.colors,
        labels = this.props.labels,
        getLabel = this.getLabel,
        defaultColors = this.defaultColors;

        for(var i in data) {
            var value = data[i],
            orientation = this.props.orientation || 'horizontal';
            chart.items.push(value);
            if(type === 'lift') {
                chart.total = (value.value > chart.total ? value.value : chart.total);
            } else {
                chart.total = chart.total + value.value;
            }
        }

        var chartHorizontalTemplate = [],
        chartVerticalTemplate = {
            labels: [],
            bars: [],
            values: [],
            additionalInfo: [],
            lifts: []
        };

        chart.items.forEach(function(item, index) {
            var percent = Math.floor((item.value / chart.total) * 100) || 0;

            chart.items[index].percent = percent

            var style = {},
            valueEl = [];

            if(type !== 'plain' && type !== 'lift') {
                if (colors && colors.length && colors[index] && colors[index] !== 'null') {
                    style.backgroundColor = colors[index];
                } else if (defaultColors && defaultColors.length && defaultColors[index]) {
                    style.backgroundColor = defaultColors[index];
                } else {
                    style.backgroundColor = '#'+Math.floor(Math.random()*16777215).toString(16);
                }
            }
            if(type === 'lift') {
                if (colors && colors.length && colors[index] && colors[index] !== 'null') {
                    style.backgroundColor = colors[index];
                }
            }

            if(limit && index >= limit) {
                return false;
            }

            if(showValues) {
                valueEl.push(<em>{item.value.toLocaleString()}</em>);
            }

            if(showAdditionalInfo && item.additionalInfo) {
                var additionalInfos = [];

                item.additionalInfo.forEach(function(info, index) {
                    var labelClass = getLabel(labels, index + 2);
                    if(labelClass) {
                        labelClass = labelClass.replace(/\s/g,'-');
                    }
                    additionalInfos.push(<span className={`label-${labelClass}`} data-label={getLabel(labels, index + 2)}>{info.toLocaleString()}</span>);
                });
                chartVerticalTemplate.additionalInfo.push(<div className="additional-info">{additionalInfos}</div>);
            }

            if(orientation === 'vertical') {
                style.maxWidth = percent + '%';
                chartVerticalTemplate.labels.push(
                    <strong>{item.label}</strong>
                );
                chartVerticalTemplate.bars.push(
                    <div>
                        <div className="bar" style={style} data-label={item.label} data-value={item.value}></div>
                        {valueEl}
                    </div>
                );
            } else {
                style.maxHeight = percent + '%';
                chartHorizontalTemplate.push(
                    <li key={index} onClick={(event) => {return clickHandler(event, item) }}>
                        <div className="additionalInfo">
                            {additionalInfos}
                        </div>
                        {valueEl}
                        <div className="bar" style={style} data-label={item.label} data-value={item.value}></div>
                        <strong>{item.label}</strong>
                    </li>
                );
            }
        });

        var additionalInfoEl = [];
        if(chartVerticalTemplate.additionalInfo && chartVerticalTemplate.additionalInfo.length) {
            additionalInfoEl.push(
                <div>{chartVerticalTemplate.additionalInfo}</div>
            );
        }

        if(type === 'lift') {
            let lifts = Math.floor(chart.total);
            var i;
            for (i = 0; i < lifts + 1; i++) {
                var _lift = i,
                lift = Math.floor(_lift),
                percent = Math.floor((_lift / chart.total) * 100);

                lift = lift + (lift && liftsPostfix ? liftsPostfix : '');

                let liftStyle = {
                    left: percent + '%',
                };

                if(i === 0) {
                    lift = 0;
                }

                chartVerticalTemplate.lifts.push(<li className="lift" style={liftStyle}><span>{lift}</span></li>);
            }
        }

        if(orientation === 'vertical') {
            var labelClassLabels = getLabel(labels, 0);
            if(labelClassLabels) {
                labelClassLabels = labelClassLabels.replace(/\s/g,'-');
            }
            var labelClassBar = getLabel(labels, 0);
            if(labelClassBar) {
                labelClassBar = labelClassBar.replace(/\s/g,'-');
            }
            return (
                <ul>
                    <li className={`bar-labels label-${labelClassLabels}`} data-label={this.getLabel(this.props.labels, 0)}>
                        {chartVerticalTemplate.labels}
                    </li>
                    <li className={`bar-container label-${labelClassBar}`} data-label={this.getLabel(this.props.labels, 1)}>
                        {chartVerticalTemplate.bars}
                        <ul className="bar-lifts">
                            {chartVerticalTemplate.lifts}
                        </ul>
                    </li>
                    <li className="info-container">
                        {chartVerticalTemplate.additionalInfo}
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
            <div className={`le-chart-barchart orientation-${this.props.orientation || 'horizontal'} type-${this.props.type || 'simple'} ${(this.props.labels && this.props.labels.length ? 'has-labels' : '')}`}>
                {this.props.childrenPlacement === 'top' ? this.props.children : null}
                {this.getData(this.props.data)}
                {this.props.childrenPlacement === 'bottom' ? this.props.children : null}
            </div>
            );
    }
}

LeBarchart.propTypes = {
    orientation: propTypes.string,
    type: propTypes.string,
    showValues: propTypes.boolean,
    showAdditionalInfo: propTypes.boolean,
    colors: propTypes.array,
    childrenPlacement: propTypes.string,
    limit: propTypes.number,
    liftsPostfix: propTypes.text,
    labels: propTypes.array,
    data: propTypes.object.isRequired,
    callback: propTypes.func
};
