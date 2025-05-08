using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using aCommons.Compliance;
using aCommons.Utils;
using aCommons.Web;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace aACTrader.Compliance
{
    public class RulesEngine
    {
        private readonly ILogger<RulesEngine> _logger;
        private readonly CachingService _cache;
        private readonly ComplianceDao _complianceDao;

        private const string ERROR_LEVEL_WARNING = "Warning";
        private const string ERROR_LEVEL_RESTRICTIVE = "Restriction";
        private NumberFormatInfo nfi = new CultureInfo("en-US", false).NumberFormat;

        public RulesEngine(ILogger<RulesEngine> logger
            , CachingService cache
            , ComplianceDao complianceDao)
        {
            _logger = logger;
            _cache = cache;
            _complianceDao = complianceDao;
            _logger.LogInformation("Initializing RulesEngine...");
        }

        public void ExDividendDateCheck(string ticker
            , string almTicker
            , string fundName
            , RuleRunMst ruleRunMst
            , RuleRunSummary runRuleSummary
            , RuleMst ruleMst)
        {
            try
            {
                IDictionary<string, DividendScheduleTO> dict = _cache.Get<IDictionary<string, DividendScheduleTO>>(CacheKeys.FUNDS_EX_DVD_DATE);
                if (dict.TryGetValue(ticker, out DividendScheduleTO data))
                {
                    RuleRunDetail detail = new RuleRunDetail();
                    detail.Ticker = ticker;
                    detail.ALMTicker = almTicker;
                    detail.Fund = fundName;
                    detail.RuleMstId = ruleMst.RuleMstId;
                    detail.RuleCondition = ruleMst.RuleCondition;
                    detail.ErrorLevel = ERROR_LEVEL_WARNING;
                    detail.RuleOutput = string.Join("; ", "ExDate: " + data.ExDvdDate.GetValueOrDefault().ToString("MM/dd/yyyy"),
                        "DvdAmt: " + data.DvdAmt);
                    //"DecDate: " + data.DecDate.GetValueOrDefault().ToString("MM/dd/yyyy"),
                    //"PayDate: " + data.PayDate.GetValueOrDefault().ToString("MM/dd/yyyy"));
                    ruleRunMst.WarningSecCount++;

                    runRuleSummary.RunDetails = runRuleSummary.RunDetails + Environment.NewLine + detail.RuleOutput;
                    if (string.IsNullOrEmpty(runRuleSummary.ErrorLevel))
                        runRuleSummary.ErrorLevel = detail.ErrorLevel;
                    else if (!ERROR_LEVEL_RESTRICTIVE.Equals(runRuleSummary.ErrorLevel))
                        runRuleSummary.ErrorLevel = detail.ErrorLevel;
                    runRuleSummary.AddDetail(detail);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error in ExDividendDateCheck process for Ticker: " + ticker, ex);
            }
        }

        public void RestrictedSecurityCheck(string ticker
            , string almTicker
            , string fundName
            , RuleRunMst ruleRunMst
            , RuleRunSummary runRuleSummary
            , RuleMst ruleMst)
        {
            try
            {
                IDictionary<string, RestrictedSecurity> dict = _cache.Get<IDictionary<string, RestrictedSecurity>>(CacheKeys.RESTRICTED_SECURITY_LIST);
                if (dict.TryGetValue(ticker, out RestrictedSecurity data))
                {
                    RuleRunDetail detail = new RuleRunDetail();
                    detail.Ticker = ticker;
                    detail.ALMTicker = almTicker;
                    detail.Fund = fundName;
                    detail.RuleMstId = ruleMst.RuleMstId;
                    detail.RuleCondition = ruleMst.RuleCondition;
                    detail.RuleOutput = string.Join("; ", "Notes: " + data.Notes);
                    detail.ErrorLevel = ERROR_LEVEL_RESTRICTIVE;

                    ruleRunMst.RestrictionSecCount++;

                    runRuleSummary.RunDetails = runRuleSummary.RunDetails + Environment.NewLine + detail.RuleOutput;
                    runRuleSummary.ErrorLevel = detail.ErrorLevel;
                    runRuleSummary.AddDetail(detail);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error in RestrictedSecurityCheck process for Ticker: " + ticker, ex);
            }
        }

        public void Rule1940Check(string ticker
            , string almTicker
            , string fundName
            , FundSummaryDetail fundSummaryDetail
            , PositionMaster position
            , RuleRunMst ruleRunMst
            , RuleRunSummary runRuleSummary
            , RuleMst ruleMst)
        {
            try
            {
                IDictionary<string, string> dict = _cache.Get<IDictionary<string, string>>(CacheKeys.FUNDS_1940_ACT);
                if (dict.TryGetValue(ticker, out string data))
                {
                    double posOwnPct = fundSummaryDetail.PosOwnPct.GetValueOrDefault();
                    if (Math.Abs(posOwnPct) > 0.03)
                    {
                        RuleRunDetail detail = new RuleRunDetail();
                        detail.Ticker = ticker;
                        detail.ALMTicker = almTicker;
                        detail.Fund = fundName;
                        detail.RuleMstId = ruleMst.RuleMstId;
                        detail.RuleCondition = ruleMst.RuleCondition;
                        detail.RuleOutput = string.Join("; ", "Position: " + fundSummaryDetail.PosHeld,
                            "Shares Outstanding: " + position.ShOut,
                            "Shares Ownership: " + fundSummaryDetail.PosOwnPct.GetValueOrDefault().ToString("P", nfi));
                        detail.ErrorLevel = ERROR_LEVEL_RESTRICTIVE;

                        ruleRunMst.RestrictionSecCount++;

                        runRuleSummary.RunDetails = string.Join(Environment.NewLine, runRuleSummary.RunDetails, detail.RuleOutput);
                        runRuleSummary.ErrorLevel = detail.ErrorLevel;
                        runRuleSummary.AddDetail(detail);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error in Rule1940Check process for Ticker: " + ticker, ex);
            }
        }

        public void PositionBreakCheck(string ticker
            , string almTicker
            , string fundName
            , FundSummaryDetail fundSummaryDetail
            , RuleRunMst ruleRunMst
            , RuleRunSummary runRuleSummary
            , RuleMst ruleMst)
        {
            try
            {
                if (fundSummaryDetail.PosVariance.HasValue && fundSummaryDetail.PosVariance.GetValueOrDefault() != 0)
                {
                    RuleRunDetail detail = new RuleRunDetail();
                    detail.Ticker = ticker;
                    detail.ALMTicker = almTicker;
                    detail.Fund = fundName;
                    detail.RuleMstId = ruleMst.RuleMstId;
                    detail.RuleCondition = ruleMst.RuleCondition;
                    detail.RuleOutput = string.Join("; ", "Position: " + fundSummaryDetail.PosHeld
                        , "Derived Position: " + fundSummaryDetail.PosDerived
                        , "Variance: " + fundSummaryDetail.PosVariance);
                    detail.ErrorLevel = ERROR_LEVEL_WARNING;

                    ruleRunMst.WarningSecCount++;

                    runRuleSummary.RunDetails = runRuleSummary.RunDetails + Environment.NewLine + detail.RuleOutput;
                    if (string.IsNullOrEmpty(runRuleSummary.ErrorLevel))
                        runRuleSummary.ErrorLevel = detail.ErrorLevel;
                    else if (!ERROR_LEVEL_RESTRICTIVE.Equals(runRuleSummary.ErrorLevel))
                        runRuleSummary.ErrorLevel = detail.ErrorLevel;
                    runRuleSummary.AddDetail(detail);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error in PositionBreakCheck process for Ticker: " + ticker, ex);
            }
        }

        public void OversellCoverCheck(string ticker
            , string almTicker
            , string fundName
            , FundSummaryDetail fundSummaryDetail
            , RuleRunMst ruleRunMst
            , RuleRunSummary runRuleSummary
            , RuleMst ruleMst)
        {
            if (fundSummaryDetail.PosVariance.HasValue && fundSummaryDetail.PosVariance.GetValueOrDefault() != 0)
            {
                RuleRunDetail detail = new RuleRunDetail();
                detail.Ticker = ticker;
                detail.ALMTicker = almTicker;
                detail.Fund = fundName;
                detail.RuleMstId = ruleMst.RuleMstId;
                detail.RuleCondition = ruleMst.RuleCondition;
                detail.RuleOutput = string.Join("; ", "Ticker: " + ticker, "Position: " + fundSummaryDetail.PosHeld
                    , "Derived Position: " + fundSummaryDetail.PosDerived
                    , "Variance: " + fundSummaryDetail.PosVariance);
                detail.ErrorLevel = ERROR_LEVEL_WARNING;

                ruleRunMst.WarningSecCount++;

                runRuleSummary.RunDetails = runRuleSummary.RunDetails + Environment.NewLine + detail.RuleOutput;
                if (string.IsNullOrEmpty(runRuleSummary.ErrorLevel))
                    runRuleSummary.ErrorLevel = detail.ErrorLevel;
                else if (!ERROR_LEVEL_RESTRICTIVE.Equals(runRuleSummary.ErrorLevel))
                    runRuleSummary.ErrorLevel = detail.ErrorLevel;
                runRuleSummary.AddDetail(detail);
            }
        }

        public void SecurityOwnershipCheck(string ticker
            , string almTicker
            , string fundName
            , FundSummaryDetail fundSummaryDetail
            , PositionMaster position
            , RuleRunMst ruleRunMst
            , RuleRunSummary runRuleSummary
            , RuleMst ruleMst)
        {
            if (position.ShOut.HasValue && fundSummaryDetail.PosHeld.HasValue)
            {
                double? totalPos = null;
                double? cashPos = null;
                double? swapPos = null;
                double? totalOwnPct = null;
                double? cashOwnPct = null;
                double? swapOwnPct = null;
                double? sharesOut = null;

                totalPos = fundSummaryDetail.PosHeld;
                if (fundSummaryDetail.SwapPos.GetValueOrDefault() != 0)
                    swapPos = fundSummaryDetail.SwapPos;
                cashPos = CommonUtils.SubtractNullableDoubles(totalPos, swapPos);

                sharesOut = position.ShOut;
                totalOwnPct = CommonUtils.Pct(totalPos, sharesOut);
                if (cashPos.HasValue)
                    cashOwnPct = CommonUtils.Pct(cashPos, sharesOut);
                if (swapPos.HasValue)
                    swapOwnPct = CommonUtils.Pct(swapPos, sharesOut);

                RuleRunDetail detail = new RuleRunDetail();
                detail.Ticker = ticker;
                detail.ALMTicker = almTicker;
                detail.Fund = fundName;
                detail.RuleMstId = ruleMst.RuleMstId;
                detail.RuleCondition = ruleMst.RuleCondition;
                detail.RuleOutput = string.Join("; ", "Ticker: " + ticker, "Position: " + fundSummaryDetail.PosHeld
                    , "Derived Position: " + fundSummaryDetail.PosDerived
                    , "Variance: " + fundSummaryDetail.PosVariance);
                detail.ErrorLevel = ERROR_LEVEL_WARNING;

                ruleRunMst.WarningSecCount++;

                runRuleSummary.RunDetails = runRuleSummary.RunDetails + Environment.NewLine + detail.RuleOutput;
                if (string.IsNullOrEmpty(runRuleSummary.ErrorLevel))
                    runRuleSummary.ErrorLevel = detail.ErrorLevel;
                else if (!ERROR_LEVEL_RESTRICTIVE.Equals(runRuleSummary.ErrorLevel))
                    runRuleSummary.ErrorLevel = detail.ErrorLevel;
                runRuleSummary.AddDetail(detail);
            }
        }
    }
}
