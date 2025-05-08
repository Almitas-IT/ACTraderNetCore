using aCommons.Cef;
using aCommons.Utils;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace aACTrader.Utils
{
    public class FundUtils
    {
        protected static readonly string pattern = @"([a-zA-Z]+)([\s]*)([a-zA-Z]*)[*]?([+-]?[0-9]*\.?[0-9]*)";
        protected static readonly string pattern1 = @"([a-zA-Z]+)([\s]*)([a-zA-Z]*)[*]?";
        protected static readonly string pattern2 = @"[*]([+-]?[0-9]*\.?[0-9]*)";

        /// <summary>
        /// Parse proxy forumla to get list of tickers for estimating navs 
        /// </summary>
        /// <param name="proxyFormula"></param>
        /// <returns></returns>
        public static IList<FundProxy> GetProxyTickers(string fundTicker, string proxyFormula)
        {
            IList<FundProxy> proxyTickersWithCoefficients = new List<FundProxy>();
            Match result = Regex.Match(proxyFormula, pattern);
            while (result.Success)
            {
                Match result1 = Regex.Match(result.Value, pattern1);
                string ticker = result1.Value;
                string proxyTicker = ticker;
                if (ticker.Contains("*"))
                    proxyTicker = ticker.Substring(0, ticker.Length - 1);

                Match result2 = Regex.Match(result.Value, pattern2);
                string beta = result2.Value;
                string betaValue = "1.0";
                if (!string.IsNullOrEmpty(beta))
                    betaValue = beta.Substring(1, beta.Length - 1);

                double? betaValueAsDouble = DataConversionUtils.ConvertToDouble(betaValue);

                FundProxy fundProxy = new FundProxy
                {
                    ETFTicker = proxyTicker,
                    Beta = betaValueAsDouble.GetValueOrDefault()
                };
                proxyTickersWithCoefficients.Add(fundProxy);
                result = result.NextMatch();
            }
            return proxyTickersWithCoefficients;
        }
    }
}