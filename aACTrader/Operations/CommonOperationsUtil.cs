using aCommons;
using aCommons.Cef;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations
{
    public class CommonOperationsUtil
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="sedol"></param>
        /// <param name="isin"></param>
        /// <param name="securityMasterExtDict"></param>
        /// <param name="positionTickerMap"></param>
        /// <returns></returns>
        public static SecurityMasterExt GetSecurityMasterExt(string ticker, string tradingTicker, string sedol, string isin,
            IDictionary<string, SecurityMasterExt> securityMasterExtDict, IDictionary<string, string> positionTickerMap)
        {
            SecurityMasterExt securityMasterExt;
            if (!securityMasterExtDict.TryGetValue(ticker, out securityMasterExt))
            {
                if (!string.IsNullOrEmpty(tradingTicker) && !securityMasterExtDict.TryGetValue(tradingTicker, out securityMasterExt))
                {
                    if (!string.IsNullOrEmpty(sedol) && positionTickerMap.TryGetValue("Sedol|" + sedol, out string positionTicker))
                    {
                        securityMasterExtDict.TryGetValue(positionTicker, out securityMasterExt);
                    }
                }
            }
            return securityMasterExt;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="tradingTicker"></param>
        /// <param name="sedol"></param>
        /// <param name="isin"></param>
        /// <param name="securityRiskFactorDict"></param>
        /// <param name="positionTickerMap"></param>
        /// <returns></returns>
        public static SecurityRiskFactor GetSecurityRiskFactor(string ticker, string tradingTicker, string sedol, string isin,
            IDictionary<string, SecurityRiskFactor> securityRiskFactorDict, IDictionary<string, string> positionTickerMap)
        {
            SecurityRiskFactor securityRiskFactor;
            if (!securityRiskFactorDict.TryGetValue(ticker, out securityRiskFactor))
            {
                if (!string.IsNullOrEmpty(tradingTicker) && !securityRiskFactorDict.TryGetValue(tradingTicker, out securityRiskFactor))
                {
                    if (!string.IsNullOrEmpty(sedol) && positionTickerMap.TryGetValue("Sedol|" + sedol, out string positionTicker))
                    {
                        securityRiskFactorDict.TryGetValue(positionTicker, out securityRiskFactor);
                    }
                }
            }
            return securityRiskFactor;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="sedol"></param>
        /// <param name="isin"></param>
        /// <param name="positionMasterDict"></param>
        /// <param name="positionTickerMap"></param>
        /// <returns></returns>
        public static PositionMaster GetPositionDetails(string ticker, string sedol, string isin,
            IDictionary<string, PositionMaster> positionMasterDict, IDictionary<string, string> positionTickerMap)
        {
            PositionMaster positionMaster = null;
            if (!positionMasterDict.TryGetValue(ticker, out positionMaster))
            {
                //search by CT ticker if composite ticker CN is provided
                if (ticker.EndsWith(" CN", StringComparison.CurrentCultureIgnoreCase))
                {
                    string newTicker = ticker.Replace(" CN", " CT");
                    positionMasterDict.TryGetValue(newTicker, out positionMaster);
                }
            }

            //search by SEDOL
            if (positionMaster == null)
            {
                if (!string.IsNullOrEmpty(sedol)
                    && positionTickerMap.TryGetValue("Sedol|" + sedol, out string positionTicker))
                {
                    positionMasterDict.TryGetValue(positionTicker, out positionMaster);
                }
            }
            return positionMaster;
        }
    }
}
