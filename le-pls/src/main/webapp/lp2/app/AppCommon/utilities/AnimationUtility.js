angular.module('mainApp.appCommon.utilities.AnimationUtility', [])
.service('AnimationUtility', function () {
   
   this.CalculateRgbBetweenValues = function (highColor, lowColor, scalor)  {
        if (highColor == null || lowColor == null || scalor == null || scalor < 0) {
            return null;
        }
        if (scalor > 1) {
            scalor = scalor / 100;
        }
        
        var diffRed = scalor * highColor.R + (1-scalor) * lowColor.R;
        var diffGreen = scalor * highColor.G + (1-scalor) * lowColor.G;
        var diffBlue = scalor * highColor.B + (1-scalor) * lowColor.B;
        
        diffRed = Math.round(diffRed);
        diffGreen = Math.round(diffGreen);
        diffBlue = Math.round(diffBlue);
        
        return {
            R: diffRed,
            G: diffGreen,
            B: diffBlue
        };
    };
    
    this.ConvertSingleRgbToHex = function (c) {
        if (c == null) {
            return null;
        }
        var hex = c.toString(16);
        return hex.length == 1 ? "0" + hex : hex;
    };
    
    this.ConvertRgbToHex = function (r, g, b) {
        if (r == null || g == null || b == null) {
            return null;
        }
        return "#" + this.ConvertSingleRgbToHex(r) + this.ConvertSingleRgbToHex(g) + this.ConvertSingleRgbToHex(b);
    };
    
});