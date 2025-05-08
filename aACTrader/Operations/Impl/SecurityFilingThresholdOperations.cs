using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using aCommons.Compliance;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class SecurityFilingThresholdOperations
    {
        private readonly ILogger<SecurityFilingThresholdOperations> _logger;
        private readonly CachingService _cache;
        private readonly SecurityAlertDao _securityAlertDao;

        public SecurityFilingThresholdOperations(ILogger<SecurityFilingThresholdOperations> logger
            , CachingService cache
            , SecurityAlertDao securityAlertDao)
        {
            _logger = logger;
            _cache = cache;
            _securityAlertDao = securityAlertDao;
            _logger.LogInformation("Initializing SecurityFilingThresholdOperations...");
        }

        public void CalculateSecurityOwnershipDetails()
        {
            IList<SecOwnership> list = new List<SecOwnership>();
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            foreach (PositionMaster position in positionDict.Values)
            {
                FundSummaryDetail fundDetail = position.FundAll;

                if (position.ShOut.HasValue)
                {
                    SecOwnership secOwnership = new SecOwnership();
                    secOwnership.RunDate = DateTime.Today;
                    secOwnership.Ticker = position.Ticker;
                    secOwnership.ALMTicker = position.SecTicker;
                    secOwnership.TotalPos = fundDetail.PosHeld;
                    if (fundDetail.SwapPos.GetValueOrDefault() != 0)
                        secOwnership.SwapPos = fundDetail.SwapPos;
                    secOwnership.CashPos = CommonUtils.SubtractNullableDoubles(fundDetail.PosHeld, fundDetail.SwapPos);
                    secOwnership.SharesOut = position.ShOut;
                    secOwnership.TotalOwnPct = CommonUtils.Pct(secOwnership.TotalPos, secOwnership.SharesOut);
                    if (secOwnership.CashPos.HasValue)
                        secOwnership.CashOwnPct = CommonUtils.Pct(secOwnership.CashPos, secOwnership.SharesOut);
                    if (secOwnership.SwapPos.HasValue)
                        secOwnership.SwapOwnPct = CommonUtils.Pct(secOwnership.SwapPos, secOwnership.SharesOut);
                    secOwnership.RptFlag = 0;

                    string secTicker = !string.IsNullOrEmpty(position.SecTicker) ? position.SecTicker : position.Ticker;
                    if (fundMasterDict.TryGetValue(secTicker, out FundMaster fundMaster))
                    {
                        secOwnership.SecType = fundMaster.SecTyp;
                        secOwnership.Country = fundMaster.Cntry;
                        secOwnership.PaymentRank = fundMaster.PayRank;
                    }

                    if (securityMasterExtDict.TryGetValue(secTicker, out SecurityMasterExt secMstExt))
                    {
                        if (string.IsNullOrEmpty(secOwnership.Country) || "Unknown".Equals(secOwnership.Country, StringComparison.CurrentCultureIgnoreCase))
                            secOwnership.Country = secMstExt.Country;

                        if (string.IsNullOrEmpty(secOwnership.SecType) || "Unknown".Equals(secOwnership.SecType, StringComparison.CurrentCultureIgnoreCase))
                            secOwnership.SecType = secMstExt.SecType;
                    }

                    if (!string.IsNullOrEmpty(secOwnership.Country))
                    {
                        if (secOwnership.Country.Equals("Australia", StringComparison.CurrentCultureIgnoreCase))
                            RunAustraliaSecurityOwnershipThresholdChecks(secOwnership);
                        else if (secOwnership.Country.Equals("Canada", StringComparison.CurrentCultureIgnoreCase))
                            RunCanadaSecurityOwnershipThresholdChecks(secOwnership);
                        else if (secOwnership.Country.Equals("United Kingdom", StringComparison.CurrentCultureIgnoreCase))
                            RunUKSecurityOwnershipThresholdChecks(secOwnership);
                        else if (secOwnership.Country.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                            RunUSSecurityOwnershipThresholdChecks(secOwnership);
                        else if (secOwnership.Country.Equals("Unknown", StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (secOwnership.Ticker.EndsWith(" AU", StringComparison.CurrentCultureIgnoreCase))
                                RunAustraliaSecurityOwnershipThresholdChecks(secOwnership);
                            else if (secOwnership.Ticker.EndsWith(" US", StringComparison.CurrentCultureIgnoreCase))
                                RunUSSecurityOwnershipThresholdChecks(secOwnership);
                            else if (secOwnership.Ticker.EndsWith(" UK", StringComparison.CurrentCultureIgnoreCase))
                                RunUKSecurityOwnershipThresholdChecks(secOwnership);
                            else if (secOwnership.Ticker.EndsWith(" CT", StringComparison.CurrentCultureIgnoreCase)
                                || secOwnership.Ticker.EndsWith(" CN", StringComparison.CurrentCultureIgnoreCase))
                                RunCanadaSecurityOwnershipThresholdChecks(secOwnership);
                            else
                                secOwnership.Notes = "Missing Country Info";
                        }
                    }
                    else
                        secOwnership.Notes = "Missing Country Info";

                    list.Add(secOwnership);
                }
            }

            _securityAlertDao.SaveSecurityOwnershipLimits(list);
        }

        private void RunAustraliaSecurityOwnershipThresholdChecks(SecOwnership secOwnership)
        {
            try
            {
                if (secOwnership.CashOwnPct.HasValue)
                {
                    if (secOwnership.CashOwnPct >= 0.05)
                    {
                        secOwnership.Notes = "> 5% ownership limit (AU)";
                        secOwnership.RptFlag = 1;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error checking Australian Ownership Limits for Ticker: " + secOwnership.Ticker, ex);
            }
        }

        private void RunCanadaSecurityOwnershipThresholdChecks(SecOwnership secOwnership)
        {
            try
            {
                if (secOwnership.TotalOwnPct.HasValue)
                {
                    if (secOwnership.CashOwnPct >= 0.10)
                    {
                        secOwnership.Notes = "> 10% ownership limit (CA)";
                        secOwnership.RptFlag = 1;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error checking Canada Ownership Limits for Ticker: " + secOwnership.Ticker, ex);
            }
        }

        private void RunUKSecurityOwnershipThresholdChecks(SecOwnership secOwnership)
        {
            try
            {
                if (secOwnership.TotalOwnPct.HasValue)
                {
                    if (secOwnership.TotalOwnPct >= 0.03)
                    {
                        secOwnership.Notes = "> 3% ownership limit (UK)";
                        secOwnership.RptFlag = 1;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error checking UK Ownership Limits for Ticker: " + secOwnership.Ticker, ex);
            }
        }

        private void RunUSSecurityOwnershipThresholdChecks(SecOwnership secOwnership)
        {
            try
            {
                if (secOwnership.TotalOwnPct.HasValue)
                {
                    if (secOwnership.TotalOwnPct >= 0.10)
                    {
                        secOwnership.Notes = "> 10% ownership limit (US)";
                        secOwnership.RptFlag = 1;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error checking US Ownership Limits for Ticker: " + secOwnership.Ticker, ex);
            }
        }
    }
}
