import template from './bar-chart.component.html';

angular.module('le.widgets.barchart', []).component('leBarChart', {
  bindings: {
    vm: '=?',
    bktlist: '=',
    callback: '&?',
    config: '=',
    statcount: '=?',
    enrichment: '=?'
  },
  template: template,

  controller: function () {
    var self = this;
    function getHighestStat(stats, fieldName) {
      var highest = 0;
      stats.forEach(function (stat) {
        if (stat[fieldName] > highest) {
          highest = Number(stat[fieldName]);
        }
      })
      return highest;
    }

    function getHorizontalPercentage(stat, field, highest, limit) {
      var number = stat[field];
      var percentage = 0;
      if (number && highest) {
        percentage = ((number / highest) * 100);

        if (typeof limit != 'undefined') {
          percentage = percentage.toFixed(limit);
        }
        return Number(percentage) + '%';
      }
      return 0;
    }

    function getHorizontalPercentageSubDec(stat, field, highest, limit) {
      // var max = Math.ceil(this.highest);
      var max = Number(Math.round(highest * 2) / 2);
      if (max < highest) {
        max = Number(Math.round(max));
      }
      var val = stat[field];
      if (max && val) {
        val = Number(val);
        var percentage = (val * 100) / max;
        return Number(percentage) + '%';
      }
      return 0;
    }

    /**
     * Sort the data by sortBy value in the config object
     */
    function sortData(bktlist, sortBy) {
      if (bktlist == undefined) {
        bktlist = [];
      }
      var field = sortBy;
      if (field.startsWith('-')) {
        field = field.substring(1, field.length);
      }
      bktlist.sort(function (item1, item2) {
        var sortBy = field;
        if (item1[sortBy] < item2[sortBy])
          return -1;
        if (item1[sortBy] > item2[sortBy])
          return 1;
        return 0;
      });
      if (sortBy.startsWith('-')) {
        bktlist.reverse();
      }
    }

    function onlyTopN(bktlist, top) {
      var tmp = $filter('limitTo')(bktlist, top);
      return;
    }

    /**
     * Return the column to use to draw the chart
     */
    function getColumnForGraph(columns) {
      for (var i = 0; i < columns.length; i++) {
        if (columns[i].chart != undefined && columns[i].chart === true) {
          return columns[i];
        }
      }
      return null;
    }

    function validateConfig() {
      if (self.config == undefined) {
        self.config = {
          'data': {},
          'chart': {},
          'vlines': {},
          'columns': {}
        };
      }
      if (!self.config.data) {
        self.config.data = {};
      }
      if (!self.config.chart) {
        self.config.chart = {};
      }
      if (!self.config.vlines) {
        self.config.vlines = {};
      }
      if (!self.config.columns) {
        self.config.columns = {};
      }
    }

    /**
     * configuration:
     * top: max number of rows
     * bktlist: bucket list which containes data 
     * color: color for the rows
     * showfield: name field to show
     */
    this.$onInit = function () {


      /************************************* Config ************************************************/
      validateConfig();

      /************************Data config ***********************/
      this.tosort = this.config.data.tosort == undefined ? false : this.config.data.tosort;
      this.sortBy = this.config.data.sortBy !== undefined ? this.config.data.sortBy : '-Cnt';
      this.trimData = this.config.data.trim !== undefined ? this.config.data.trim : false;
      this.top = this.config.data.top !== undefined ? this.config.data.top : 5;

      /***********************************************************/

      /************************** Chart Config ***********************/
      this.header = this.config.chart.header !== undefined ? this.config.chart.header : 'Header';
      this.emptymsg = this.config.chart.emptymsg !== undefined ? this.config.chart.emptymsg : 'No Stats';
      this.color = this.config.chart.color !== undefined ? this.config.chart.color : '#D0D1D0';
      this.usecolor = this.config.chart.usecolor !== undefined ? Boolean(this.config.chart.usecolor) : true;
      this.mousehover = this.config.chart.mousehover !== undefined ? this.config.chart.mousehover : false;
      this.hovercolor = this.config.chart.hovercolor !== undefined ? this.config.chart.hovercolor : this.color;
      this.chartType = this.config.chart.type !== undefined ? this.config.chart.type : 'decimal';
      this.showVLines = this.config.chart.showVLines !== undefined ? Boolean(this.config.chart.showVLines) : false;
      this.maxVLines = this.config.chart.maxVLines !== undefined ? this.config.chart.maxVLines : 3;
      this.showstatcount = this.config.chart.showstatcount !== undefined ? this.config.chart.showstatcount : false;

      /***************************************************************/
      /**************************** Columns Config ***********************************/
      this.columns = this.config.columns ? this.config.columns : [];

      /*********************************************************************************************/

      /****************************** V Lines Config ******************************************/
      this.vlinesSuffix = this.config.vlines.suffix !== undefined ? this.config.vlines.suffix : '';

      /****************************************************************************************/

      this.bktlist = this.bktlist !== undefined ? this.bktlist : [];
      if (this.tosort) {
        sortData(this.bktlist, this.sortBy);
      }
      if (this.trimData && this.bktlist.length > this.top) {
        this.bktlist = onlyTopN(this.bktlist, this.top);
      }
      //*****************************************/

      this.highest = 0;
      var column = getColumnForGraph(this.columns);
      if (column !== null) {
        this.highest = getHighestStat(this.bktlist, column.field);
      }
    }

    /**
     * Return the columns after the chart
     * each column can have the following config
     *  field: 'Lift',
     *  label: 'Lifts',
     *  type: 'string',
     *  suffix: 'x',
     *  chart: true
     */
    this.getColumns = function () {
      return this.columns;
    }

    /**
     * Return the value of the specific cell based on the type of the column
     * If the type is 'string' the suffix is appended
     * @param {*} stat 
     * @param {*} column 
     */
    this.getValue = function (stat, column) {
      switch (column.type) {
        case 'number':
          {
            return stat[column.field];
          }
        case 'string':
          {
            return stat[column.field] + (column.suffix ? column.suffix : '');
          }
        default:
          return stat[column.field];
      }
    }

    /**
     * Return the value showVLines fron the config object. 
     * If not set return false
     */
    this.showVerticalLines = function () {
      return this.showVLines;
    }

    this.getBarColor = function (stat) {
      if (this.usecolor == true || this.getStatCount(stat) > 0) {
        return this.color;
      } else {
        return "#939393";
      }
    }

    this.getMouseOverColor = function () {
      if (this.mousehover) {
        return this.hovercolor;
      } else {
        return this.color;;
      }
    }



    this.getHorizontalPercentage = function (stat, limit) {
      var column = getColumnForGraph(this.columns);
      if (column == null) {
        return 0;
      }
      switch (this.chartType) {
        case 'decimal':
          {
            return getHorizontalPercentageSubDec(stat, column.field, this.highest, limit);
          }
        default:
          {
            return getHorizontalPercentage(stat, column.field, this.highest, limit);
          }
      }
    }

    this.getVerticalLines = function () {
      if (this.bktlist.length == 0) {
        return [];
      }
      if (this.vertcalLines === undefined) {
        var lines = [];
        var f = getColumnForGraph(this.columns).field;
        var max = Math.round(this.highest * 2) / 2;
        if (max < this.highest) {
          max = Number(Math.round(max));
        }
      
        if (this.bktlist.length.length == 1) {
          lines.push({
            'perc': Number(100 / 2) + '%',
            'label': (max / 2) + this.vlinesSuffix
          });
          lines.push({
            'perc': 100 + '%',
            'label': max + this.vlinesSuffix
          });
        } else {
          var intervallRange = max / this.maxVLines;
          
          for (var u = 0; u < this.maxVLines; u++) {
            var val = (intervallRange * (u+1));
            val = val.toFixed(1);
            // val = Math.round(val * 2)/2;
            var per = (100 * val)/max;
            lines.push({
              'perc': per + '%',
              'label': val + this.vlinesSuffix
            });
          }

        }
        this.vertcalLines = lines;
      }
      return this.vertcalLines;
    }

    this.getStatCount = function (stat) {
      if (this.vm) {
        var count = this.vm.getAttributeRules(this.enrichment, stat).length;
        return count;
      } else {
        return 0;
      }
    }


    /**
     * Clicked on the single row of the chart
     * @param {*} stat 
     */
    this.clicked = function (stat) {
      if (this.callback) {
        this.callback()(stat, this.enrichment);
      }

    }

  }
});