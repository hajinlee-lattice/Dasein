package com.latticeengines.dataflow.runtime.cascading.cdl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.scoring.FittedConversionRateCalculatorImplV1;
import com.latticeengines.domain.exposed.cdl.scoring.FittedConversionRateCalculatorImplV2;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;

public class FittedConversionRateCalculatorImplUnitTestNG {
    @Test(groups = "unit")
    void testFitFunctionV1() {
        // alpha, beta, gamma, maxRate
        // -63472.8123838 730757.154341 100000 0.544117647059
        FitFunctionParameters params =
            new FitFunctionParameters(-63472.8123838, 730757.154341, 100000, 0.544117647059, "v1");
        FittedConversionRateCalculatorImplV1 converter =
            new FittedConversionRateCalculatorImplV1(params);
        double[] expectedResults = {
            0.00074322302073113382,
            0.00079192160699467469,
            0.00084381114944182423,
            0.00089910073765562583,
            0.00095801316250755425,
            0.0010207858133463527,
            0.0010876716350254644,
            0.0011589401471202431,
            0.0012348785301820296,
            0.0013157927824548115,
            0.0014020089539915023,
            0.0014938744595613023,
            0.0015917594790725163,
            0.0016960584501224418,
            0.0018071916556662699,
            0.001925606919552864,
            0.0020517814102674653,
            0.0021862235643002725,
            0.0023294751340830317,
            0.0024821133732497043,
            0.0026447533596625113,
            0.0028180504780456884,
            0.0030027030583316149,
            0.0031994551899452738,
            0.0034090997221766185,
            0.0036324814584033889,
            0.003870500559716526,
            0.004124116173693025,
            0.0043943502993521975,
            0.0046822919057815842,
            0.0049891013206590472,
            0.0053160149063578022,
            0.0056643500431396354,
            0.0060355104363064961,
            0.0064309917749642538,
            0.006852387759097237,
            0.0073013965230720977,
            0.0077798274758310504,
            0.0082896085978309691,
            0.0088327942086883059,
            0.0094115732429565797,
            0.010028278079994634,
            0.010685393931755664,
            0.011385568869368335,
            0.012131624491035191,
            0.012926567288887407,
            0.013773600776891623,
            0.014676138385070987,
            0.015637817229296559,
            0.01666251277092284,
            0.017754354418716874,
            0.018917742191415267,
            0.020157364435048824,
            0.021478216725632091,
            0.02288562200025443,
            0.024385252009475219,
            0.025983150175545852,
            0.027685755951810373,
            0.029499930765758225,
            0.031432985680911206,
            0.033492710859182165,
            0.035687406961162009,
            0.038025918583382542,
            0.040517669933081688,
            0.043172702780736504,
            0.046001716947906973,
            0.049016113431944557,
            0.052228040332693952,
            0.055650441852459764,
            0.059297110417646091,
            0.063182742324129196,
            0.067322996893051976,
            0.071734559660320121,
            0.076435209557304637,
            0.081443890630810381,
            0.086780788334948261,
            0.092467410919519627,
            0.098526676107600727,
            0.10498300347456546,
            0.11186241283298488,
            0.11919262913606402,
            0.12700319420924386,
            0.13532558583126328,
            0.1441933445403355,
            0.1536422089259033,
            0.16371025955873872,
            0.17443807253606783,
            0.18586888300526486,
            0.19804875944384187,
            0.21102678927009927,
            0.22485527675161582,
            0.23958995379574116,
            0.25529020460556495,
            0.27201930494176019,
            0.28984467720330437,
            0.34069927117440818,
            0.39155386514551199,
            0.44240845911661575,
            0.49326305308771962};


        for (int index = 0; index < 99; ++index) {
            int percentile = index + 1;
            double result = converter.calculate(percentile);
            double expectedResult = expectedResults[index];
            // simulate approximately equal for double numbers
            Assert.assertTrue(Math.abs(result - expectedResult) < 1e-6,
                              "result = " + result + ", expected result =" + expectedResult);
        }
    }

    @Test(groups = "unit")
    void testFitFunctionV2() {
        // alpha, beta, gamma, maxRate
        // -44.897047433156146 185.04455602025496 62.5 0.3627450980392157
        FitFunctionParameters params =
            new FitFunctionParameters(-44.897047433156146, 185.04455602025496, 62.5, 0.3627450980392157, "v2");
        FittedConversionRateCalculatorImplV2 converter =
            new FittedConversionRateCalculatorImplV2(params);
        double[] expectedResults = {
            0.000540584254457841,
            0.0005749482044864056,
            0.0006115484123674606,
            0.000650533784868845,
            0.0006920633698359091,
            0.0007363070622528559,
            0.0007834463605393218,
            0.0008336751767329517,
            0.0008872007044804594,
            0.0009442443490498715,
            0.0010050427238915156,
            0.0010698487186116036,
            0.0011389326435861987,
            0.0012125834568337082,
            0.0012911100791839433,
            0.0013748428042360968,
            0.0014641348100839556,
            0.001559363780310199,
            0.001660933642320994,
            0.0017692764316940544,
            0.0018848542918745873,
            0.0020081616192537464,
            0.002139727364429766,
            0.0022801175012653117,
            0.0024299376762411946,
            0.002589836051554509,
            0.0027605063564356343,
            0.00294269116225941,
            0.0031371853982218216,
            0.0033448401256274654,
            0.003566566590227254,
            0.003803340573524302,
            0.004056207065587129,
            0.00432628528363151,
            0.004614774062511868,
            0.004922957645274106,
            0.005252211904103793,
            0.005604011024345259,
            0.005979934686810049,
            0.006381675786316085,
            0.006811048727362029,
            0.00726999834101592,
            0.007760609470545034,
            0.00828511727701937,
            0.008845918320132389,
            0.009445582473812728,
            0.010086865740873844,
            0.010772724035993586,
            0.0115063280117835,
            0.012291079008580837,
            0.013130626214981688,
            0.014028885132999252,
            0.014990057449178919,
            0.01601865242102692,
            0.017119509896804774,
            0.01829782509612697,
            0.019559175288959986,
            0.020909548521595288,
            0.02235537455004525,
            0.02390355815415565,
            0.025561515019618175,
            0.02733721039010366,
            0.02923920070798181,
            0.031276678479717025,
            0.033459520621038505,
            0.03579834055763672,
            0.03830454437940695,
            0.04099039137043702,
            0.043869059263116655,
            0.046954714593000726,
            0.05026258856178205,
            0.05380905884895349,
            0.05761173784868842,
            0.06168956784746874,
            0.06606292370026806,
            0.07075372360882584,
            0.0757855486551812,
            0.08118377179742736,
            0.08697569709293096,
            0.09319070997742425,
            0.09986043949699613,
            0.10701893346427716,
            0.1147028475906604,
            0.12295164973401763,
            0.13180784049595926,
            0.14131719150592262,
            0.15152900284074428,
            0.16249638114979778,
            0.1742765401871876,
            0.18693112559546735,
            0.20052656594016763,
            0.21513445216296198,
            0.23083194780402846,
            0.2477022325427535,
            0.26583498182163134,
            0.2853268855524473,
            0.30628220915851806,
            0.32881340048350843,
            0.35304174639758823
        };
        for (int index = 0; index < 99; ++index) {
            int percentile = index + 1;
            double result = converter.calculate(percentile);
            double expectedResult = expectedResults[index];
            // simulate approximately equal for double numbers
            Assert.assertTrue(Math.abs(result - expectedResult) < 1e-6,
                              "result = " + result + ", expected result =" + expectedResult);
        }
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    void testBadFitFunctionParameters1() {
        FitFunctionParameters params =
            new FitFunctionParameters(Double.NEGATIVE_INFINITY, 185.04455602025496, 62.5, 0.3627450980392157, "v2");
        new FittedConversionRateCalculatorImplV2(params);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    void testBadFitFunctionParameters2() {
        FitFunctionParameters params =
            new FitFunctionParameters(1.2, Double.NaN, 62.5, 0.3627450980392157, "v2");
        new FittedConversionRateCalculatorImplV2(params);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    void testBadFitFunctionParameters3() {
        FitFunctionParameters params =
            new FitFunctionParameters(1.2, 0.0, Double.NEGATIVE_INFINITY, 0.3627450980392157, "v2");
        new FittedConversionRateCalculatorImplV2(params);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    void testBadFitFunctionParameters4() {
        FitFunctionParameters params =
            new FitFunctionParameters(1.2, 0.0, 0.0, Double.POSITIVE_INFINITY, "v2");
        new FittedConversionRateCalculatorImplV2(params);
    }
}
