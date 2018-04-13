angular.module('le.widgets.barchart', []).component('leBarChart', {
  bindings: {
    vm: '=?',
    bktlist: '=',
    callback: '&?',
    config: '=',
    statcount: '=?',
    enrichment: '=?'
  },
  templateUrl: './components/bar-chart/bar-chart.component.html',

  controller: function () {
    function getHighestStat(stats, fieldName) {
      var highest = 0;
      stats.forEach(function (stat) {
        if (stat[fieldName] > highest) {
          highest = stat[fieldName];
        }
      })
      return highest;
    }

    function getHorizontalPercentage(stat, field, highest, limit) {
      var number = stat[field];

      if (number && highest) {
        percentage = ((number / highest) * 100);

        if (typeof limit != 'undefined') {
          percentage = percentage.toFixed(limit);
        }
        return percentage + '%';
      }
      return 0;
    }

    function getHorizontalPercentageSubDec(stat, field, highest, limit) {
      // var max = Math.ceil(this.highest);
      var max = Math.round(highest * 2) / 2;
      var val = stat[field];
      if (max && val) {
        var percentage = (val * 100) / max;
        return percentage + '%';
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
      if (this.config == undefined) {
        this.config = {
          'data': {},
          'chart': {},
          'vlines': {},
          'columns': {}
        };
      }
      if (!this.config.data) {
        this.config.data = {};
      }
      if (!this.config.chart) {
        this.config.chart = {};
      }
      if (!this.config.vlines) {
        this.config.vlines = {};
      }
      if (!this.config.columns) {
        this.config.columns = {};
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
        var top = Math.round(this.highest * 2) / 2;
        if (top == 1) {
          this.maxVLines = 2;
        }

        var lines = [];
        var intervalPerc = 100 / this.maxVLines;
        var intervalLabel = this.highest / this.maxVLines;
        intervalLabel = Math.round(intervalLabel * 2) / 2;
        for (var i = 0; i < this.maxVLines; i++) {
          var perc = (intervalPerc * (i + 1));
          var label = (intervalLabel * (i + 1));
          lines.push({
            'perc': perc + '%',
            'label': label + this.vlinesSuffix
          });
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