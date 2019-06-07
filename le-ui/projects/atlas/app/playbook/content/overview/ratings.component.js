import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeBarchart from "common/widgets/charts/le-barchart";
import { get } from 'lodash';

class RatingsComponent extends Component {
    constructor(props) {
        super(props);
    }

    barChart(play) {
        var data = {};
        if(play && play.ratingEngine && play.ratingEngine.bucketMetadata) {
            var lifts = play.ratingEngine.bucketMetadata;
            
            lifts.forEach(function(lift) {
                data[lift.bucket_name] = {
                    label: lift.bucket_name,
                    value: lift.lift,
                    additionalInfo: [
                        (lift.lift + '').replace(/(^.*?\d+)(\.\d)?.*/, '$1$2') + 'x',
                        lift.num_leads
                    ]
                }
            });

            return (
                <LeBarchart data={data} type={'lift'} orientation={'vertical'} showAdditionalInfo={true} liftsPostfix={'x'} labels={['Rank',null,'Lift','Accounts']}></LeBarchart>
            );
        }
        return (
            <div>no lift data</div>
        );
    }

    render() {
        return (
            <LeVPanel className="main-panel panel ratings-chart">
                <h2 className="panel-label"> Ratings</h2>
                <div class="lift-chart">
                    {this.barChart(this.props.play)}
                </div>
            </LeVPanel>
        );
    }
}
export default RatingsComponent;