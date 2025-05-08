using aACTrader.DAO.Interface;
using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class FundForecastDao : IFundForecastDao
    {
        private readonly ILogger<FundForecastDao> _logger;

        private static readonly DateTime TodaysDate = DateTime.Now.Date;
        private const string DATEFORMAT = "yyyy-MM-dd";
        private const string DELIMITER = ",";

        public FundForecastDao(ILogger<FundForecastDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing FundForecastDao...");
        }

        /// <summary>
        /// Truncate Table
        /// </summary>
        /// <param name="tableName"></param>
        public void TruncateTable(string tableName)
        {
            try
            {
                string sql = "TRUNCATE TABLE " + tableName;
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error truncating table");
                throw;
            }
        }

        /// <summary>
        /// Move Data from Staging table to Target table
        /// </summary>
        /// <param name="procedureName"></param>
        public void MoveDataToTargetTable(string procedureName)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(procedureName, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error moving data to target table");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundForecastDict"></param>
        /// <param name="fundMasterDict"></param>
        /// <param name="fundAlphaModelScoresDict"></param>
        /// <param name="securityPriceDict"></param>
        /// <param name="priceTickerMap"></param>
        public void SaveFundForecasts(
            IDictionary<string, FundForecast> fundForecastDict
            , IDictionary<string, FundMaster> fundMasterDict
            , IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap)
        {
            try
            {
                TruncateTable(StgFundForecastTableName);
                SaveFundForecastsStg(fundForecastDict, fundMasterDict, fundAlphaModelScoresDict, securityPriceDict, priceTickerMap);
                MoveDataToTargetTable(SaveFundForecastsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }


        public void SaveFundPDStats(IDictionary<string, FundPDStats> fundPDStatsDict)
        {
            try
            {
                TruncateTable(StgFundPDStatsTableName);
                SaveFundPDStatsStg(fundPDStatsDict);
                MoveDataToTargetTable(SaveFundPDStatsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving PD Stats data");
            }

        }

        public void SaveFundPDStatsStg(IDictionary<string, FundPDStats> fundPDStatsDict)
        {
            StringBuilder sb = new StringBuilder();
            string ticker = string.Empty;
            DateTime effectiveDate = TodaysDate;
            string effectiveDateAsString = DateUtils.ConvertDate(effectiveDate, DATEFORMAT, "0000-00-00");

            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgFundPDStats " +
                    "(Ticker, EffectiveDate, LastNavDt, PubNav, PubPD, EstNav, " +
                    "EstPD, Avg1M, Avg3M, Avg6M, Avg1Y, Avg2Y, Avg3Y, Avg5Y, Avg10Y, Avg25Y, " +
                    "PRnk1M, PRnk3M, PRnk6M, PRnk1Y, PRnk2Y, PRnk3Y, PRnk5Y, PRnk10Y, PRnk25Y) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        List<string> Rows = new List<string>();
                        foreach (FundPDStats data in fundPDStatsDict.Values)
                        {
                            //Ticker
                            sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);

                            //EffectiveDate
                            sb.Append(string.Concat("'", effectiveDateAsString, "'")).Append(DELIMITER);

                            //LastNavDt
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.LastNavDt, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);

                            //PubNav
                            if (data.PubNav.HasValue)
                                sb.Append(data.PubNav).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Pub PD
                            if (data.PubPD.HasValue)
                                sb.Append(data.PubPD).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EstimatedNav
                            if (data.EstNav.HasValue)
                                sb.Append(data.EstNav).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EstimatedPD
                            if (data.EstPD.HasValue)
                                sb.Append(data.EstPD).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg1M
                            if (data.Avg1M.HasValue)
                                sb.Append(data.Avg1M).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg3M
                            if (data.Avg3M.HasValue)
                                sb.Append(data.Avg3M).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg6M
                            if (data.Avg6M.HasValue)
                                sb.Append(data.Avg6M).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg1Y
                            if (data.Avg1Y.HasValue)
                                sb.Append(data.Avg1Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg2Y
                            if (data.Avg2Y.HasValue)
                                sb.Append(data.Avg2Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg3Y
                            if (data.Avg3Y.HasValue)
                                sb.Append(data.Avg3Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg5Y
                            if (data.Avg5Y.HasValue)
                                sb.Append(data.Avg5Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg10Y
                            if (data.Avg10Y.HasValue)
                                sb.Append(data.Avg10Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Avg25Y
                            if (data.Avg25Y.HasValue)
                                sb.Append(data.Avg25Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk1M
                            if (data.PRnk1M.HasValue)
                                sb.Append(data.PRnk1M).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk3M
                            if (data.PRnk3M.HasValue)
                                sb.Append(data.PRnk3M).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk6M
                            if (data.PRnk6M.HasValue)
                                sb.Append(data.PRnk6M).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk1Y
                            if (data.PRnk1Y.HasValue)
                                sb.Append(data.PRnk1Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk2Y
                            if (data.PRnk2Y.HasValue)
                                sb.Append(data.PRnk2Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk3Y
                            if (data.PRnk3Y.HasValue)
                                sb.Append(data.PRnk3Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk5Y
                            if (data.PRnk5Y.HasValue)
                                sb.Append(data.PRnk5Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk10Y
                            if (data.PRnk10Y.HasValue)
                                sb.Append(data.PRnk10Y).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PRnk25Y
                            if (data.PRnk25Y.HasValue)
                                sb.Append(data.PRnk25Y);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }
                        sCommand.Append(string.Join(DELIMITER, Rows));
                        sCommand.Append(";");

                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Fund PD Stats: " + ticker, ex);
                throw;
            }

        }
        /// <summary>
        /// Saves Fund Forecasts (after market closes) to Staging Table
        /// </summary>
        /// <param name="fundForecastDict"></param>
        /// <param name="fundMasterDict"></param>
        /// <param name="fundAlphaModelScoresDict"></param>
        /// <param name="securityPriceDict"></param>
        /// <param name="priceTickerMap"></param>
        public void SaveFundForecastsStg(
            IDictionary<string, FundForecast> fundForecastDict
            , IDictionary<string, FundMaster> fundMasterDict
            , IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap)
        {
            string ticker = string.Empty;

            try
            {
                StringBuilder sb = new StringBuilder();
                DateTime effectiveDate = TodaysDate;
                string effectiveDateAsString = DateUtils.ConvertDate(effectiveDate, DATEFORMAT, "0000-00-00");

                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgFundForecast " +
                    "(FIGI, Ticker, EffectiveDate," +
                    "PublishedNav, PublishedAdjNav, PublishedNavSource, PublishedNavDate," +
                    "EstimatedNav, EstimatedNavRtn, EstimatedNavSource, HoldingsRtn, ETFRegRtn, ProxyRtn," +
                    "ExpectedAlphaRtn, ExpectedAlphaDscnt, ExpectedAlphaDvd, ExpectedAlphaExpense," +
                    "IRRLastPrice, IRRBidPrice, IRRAskPrice," +
                    "PublishedDscnt, DsnctChange, DscntToLastPrice, DscntToAskPrice, DscntToBidPrice," +
                    "UnlevDscntToLastPrice, UnlevDscntToAskPrice, UnlevDscntToBidPrice," +
                    "ZScore1W, ZScore1M, ZScore3M, ZScore6M, ZScore12M, ZScore24M, ZScore36M, ZScore60M, ZScoreLife," +
                    "DScore1W, DScore1M, DScore3M, DScore6M, DScore12M, DScore24M, DScore36M, DScore60M, DScoreLife," +
                    "HoldingsAdjRtn, HoldingsNav, ETFRegNav, ProxyNav, Leverage, ExpenseRatio," +
                    "ZScore2W, DScore2W, AccrualRate, AccruedInterest," +
                    "EAActivistScore, EALiquidityCost, EAShareBuyback, EAExtraAdjustment, EADiscountRtn, EADiscountGroup," +
                    "EALTAvgSectorDiscount, EADScoreMovement, EAActivistScoreAdj, EABoardVotingAdj, EABoardTermAdj, EADiscountChange," +
                    "EAExpectedDiscount, EAForecastedDvd, EAForecastedDvdYield, EADividendRtn, EAExpenseDrag," +
                    "EANavAccretionShareBuyback, EAAlpha, EAFinalAlpha," +
                    "LastPrice, BidPrice, AskPrice, ProxyFormula, ETFReg3MFormula, ETFReg6MFormula, ETFReg12MFormula," +
                    "ETFReg24MFormula, LeverageSrc," +
                    "ExpenseRatioSrc, ETFReg36MFormula, ETFReg60MFormula, ETFRegLife," +
                    "RSquared3M, RSquared6M, RSquared12M, RSquared24M, RSquared36M, RSquared60M, RSquaredLife, CoinsPerUnit," +
                    "GeneralProxy, GeneralProxyRtn, GeneralProxyNav, Condition1Formula, Condition1Proxy, Condition1Rtn, Condition1Nav," +
                    "Condition2Formula, Condition2Proxy, Condition2Rtn, Condition2Nav, ConditionProxyType" +
                    ") values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        List<string> Rows = new List<string>();
                        foreach (KeyValuePair<string, FundForecast> kvp in fundForecastDict)
                        {
                            FundForecast data = kvp.Value;
                            ticker = data.Ticker;

                            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(data.Ticker, priceTickerMap, securityPriceDict);

                            double? fundEstimatedNav = FundForecastOperations.GetEstimatedNav(data);
                            if (fundEstimatedNav.HasValue)
                            {
                                if (data.SecType.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase)
                                    || data.SecType.StartsWith("BDC", StringComparison.CurrentCultureIgnoreCase)
                                    || data.SecType.Equals("Holding Companies", StringComparison.CurrentCultureIgnoreCase)
                                    || (!string.IsNullOrEmpty(data.NavEstMthd) && data.NavEstMthd.Equals("CryptoNav", StringComparison.CurrentCultureIgnoreCase))
                                    )
                                {
                                    //records++;

                                    FundMaster fundMaster = null;
                                    fundMasterDict.TryGetValue(data.Ticker, out fundMaster);

                                    FundAlphaModelScores fundAlphaModelScores = null;
                                    fundAlphaModelScoresDict.TryGetValue(data.Ticker, out fundAlphaModelScores);

                                    //FIGI
                                    sb.Append(string.Concat("'", data.FIGI, "'")).Append(DELIMITER);

                                    //Ticker
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);

                                    //EffectiveDate
                                    sb.Append(string.Concat("'", effectiveDateAsString, "'")).Append(DELIMITER);

                                    //PublishedNav
                                    if (data.LastNav.HasValue)
                                        sb.Append(data.LastNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //PublishedAdjNav
                                    if (data.LastDvdAdjNav.HasValue)
                                        sb.Append(data.LastDvdAdjNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //PublishedNavSource
                                    sb.Append(string.Concat("'", data.LastNavSrc, "'")).Append(DELIMITER);

                                    //PublishedNavDate
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.LastNavDt, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);

                                    //EstimatedNav
                                    if (data.EstNav.HasValue)
                                        sb.Append(data.EstNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EstimatedNavRtn
                                    if (data.EstRtn.HasValue)
                                        sb.Append(data.EstRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EstimatedNavSource
                                    if (!string.IsNullOrEmpty(data.NavEstMthd))
                                        sb.Append(string.Concat("'", data.NavEstMthd, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //HoldingsRtn
                                    if (data.PortRtn.HasValue)
                                        sb.Append(data.PortRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFRegRtn
                                    if (data.ETFRtn.HasValue)
                                        sb.Append(data.ETFRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ProxyRtn
                                    if (data.AdjProxyRtn.HasValue)
                                        sb.Append(data.AdjProxyRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ExpectedAlphaRtn
                                    sb.Append("null").Append(DELIMITER);

                                    //ExpectedAlphaDscnt
                                    sb.Append("null").Append(DELIMITER);

                                    //ExpectedAlphaDvd
                                    sb.Append("null").Append(DELIMITER);

                                    //ExpectedAlphaExpense
                                    sb.Append("null").Append(DELIMITER);

                                    //IRRLastPrice
                                    if (data.IRRLastPrc.HasValue)
                                        sb.Append(data.IRRLastPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //IRRBidPrice
                                    if (data.IRRBidPrc.HasValue)
                                        sb.Append(data.IRRBidPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //IRRAskPrice
                                    if (data.IRRAskPrc.HasValue)
                                        sb.Append(data.IRRAskPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //PublishedDscnt
                                    if (data.LastPD.HasValue)
                                        sb.Append(data.LastPD).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DsnctChange
                                    if (data.PDChng.HasValue)
                                        sb.Append(data.PDChng).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DscntToLastPrice
                                    if (data.PDLastPrc.HasValue)
                                        sb.Append(data.PDLastPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DscntToAskPrice
                                    if (data.PDAskPrc.HasValue)
                                        sb.Append(data.PDAskPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DscntToBidPrice
                                    if (data.PDBidPrc.HasValue)
                                        sb.Append(data.PDBidPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //UnlevDscntToLastPrice
                                    if (data.PDLastPrcUnLev.HasValue)
                                        sb.Append(data.PDLastPrcUnLev).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //UnlevDscntToAskPrice
                                    if (data.PDAskPrcUnLev.HasValue)
                                        sb.Append(data.PDAskPrcUnLev).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //UnlevDscntToBidPrice
                                    if (data.PDBidPrcUnLev.HasValue)
                                        sb.Append(data.PDBidPrcUnLev).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore1W
                                    if (data.ZS1W.HasValue)
                                        sb.Append(data.ZS1W).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore1M
                                    if (data.ZS1M.HasValue)
                                        sb.Append(data.ZS1M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore3M
                                    if (data.ZS3M.HasValue)
                                        sb.Append(data.ZS3M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore6M
                                    if (data.ZS6M.HasValue)
                                        sb.Append(data.ZS6M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore12M
                                    if (data.ZS12M.HasValue)
                                        sb.Append(data.ZS12M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore24M
                                    if (data.ZS24M.HasValue)
                                        sb.Append(data.ZS24M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore36M
                                    if (data.ZS36M.HasValue)
                                        sb.Append(data.ZS36M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore60M
                                    if (data.ZS60M.HasValue)
                                        sb.Append(data.ZS60M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScoreLife
                                    if (data.ZSLife.HasValue)
                                        sb.Append(data.ZSLife).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore1W
                                    if (data.DS1W.HasValue)
                                        sb.Append(data.DS1W).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore1M
                                    if (data.DS1M.HasValue)
                                        sb.Append(data.DS1M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore3M
                                    if (data.DS3M.HasValue)
                                        sb.Append(data.DS3M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore6M
                                    if (data.DS6M.HasValue)
                                        sb.Append(data.DS6M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore12M
                                    if (data.DS12M.HasValue)
                                        sb.Append(data.DS12M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore24M
                                    if (data.DS24M.HasValue)
                                        sb.Append(data.DS24M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore36M
                                    if (data.DS36M.HasValue)
                                        sb.Append(data.DS36M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore60M
                                    if (data.DS60M.HasValue)
                                        sb.Append(data.DS60M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScoreLife
                                    if (data.DSLife.HasValue)
                                        sb.Append(data.DSLife).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //HoldingsAdjRtn
                                    if (data.AdjPortRtn.HasValue)
                                        sb.Append(data.AdjPortRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //HoldingsNav
                                    if (data.PortNav.HasValue)
                                        sb.Append(data.PortNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFRegNav
                                    if (data.ETFNav.HasValue)
                                        sb.Append(data.ETFNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ProxyNav
                                    if (data.ProxyNav.HasValue)
                                        sb.Append(data.ProxyNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Leverage
                                    if (data.LevRatio.HasValue)
                                        sb.Append(data.LevRatio).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ExpenseRatio
                                    if (data.ExpRatio.HasValue)
                                        sb.Append(data.ExpRatio).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ZScore2W
                                    if (data.ZS2W.HasValue)
                                        sb.Append(data.ZS2W).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //DScore2W
                                    if (data.DS2W.HasValue)
                                        sb.Append(data.DS2W).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //AccrualRate
                                    if (data.AccrRate.HasValue)
                                        sb.Append(data.AccrRate).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //AccruedInterest
                                    if (data.AI.HasValue)
                                        sb.Append(data.AI).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAActivistScore
                                    if (data.EAAScore.HasValue)
                                        sb.Append(data.EAAScore).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EALiquidityCost
                                    if (data.LiqCost.HasValue)
                                        sb.Append(data.LiqCost).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAShareBuyback
                                    if (data.ShareBB.HasValue)
                                        sb.Append(data.ShareBB).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAExtraAdjustment
                                    if (data.ExpAlphaAdjFactor.HasValue)
                                        sb.Append(data.ExpAlphaAdjFactor).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EADiscountRtn
                                    if (data.EADscntRtn.HasValue)
                                        sb.Append(data.EADscntRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EADiscountGroup
                                    if (!string.IsNullOrEmpty(data.FundDscntGrp))
                                        sb.Append(string.Concat("'", data.FundDscntGrp, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EALTAvgSectorDiscount
                                    if (data.EALTAvgSectorDscnt.HasValue)
                                        sb.Append(data.EALTAvgSectorDscnt).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EADScoreMovement
                                    if (data.EADScoreMvmt.HasValue)
                                        sb.Append(data.EADScoreMvmt).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAActivistScoreAdj
                                    if (data.EAAScore.HasValue)
                                        sb.Append(data.EAAScore).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EABoardVotingAdj
                                    if (data.EABoardVoting.HasValue)
                                        sb.Append(data.EABoardVoting).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EABoardTermAdj
                                    if (data.EABoardTerm.HasValue)
                                        sb.Append(data.EABoardTerm).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EADiscountChange
                                    if (data.EADscntChng.HasValue)
                                        sb.Append(data.EADscntChng).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAExpectedDiscount
                                    if (data.EAExpDscnt.HasValue)
                                        sb.Append(data.EAExpDscnt).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAForecastedDvd
                                    if (data.EADvdFcst.HasValue)
                                        sb.Append(data.EADvdFcst).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAForecastedDvdYield
                                    if (data.EADvdFcstYld.HasValue)
                                        sb.Append(data.EADvdFcstYld).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EADividendRtn
                                    if (data.EADvdRtn.HasValue)
                                        sb.Append(data.EADvdRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAExpenseDrag
                                    if (data.EAExpDrag.HasValue)
                                        sb.Append(data.EAExpDrag).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EANavAccretionShareBuyback
                                    if (data.EANavAccrShareBB.HasValue)
                                        sb.Append(data.EANavAccrShareBB).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAAlpha
                                    if (data.EAAlpha.HasValue)
                                        sb.Append(data.EAAlpha).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //EAFinalAlpha
                                    if (data.EAFinalAlpha.HasValue)
                                        sb.Append(data.EAFinalAlpha).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //LastPrice
                                    if (securityPrice != null && securityPrice.LastPrc.HasValue)
                                        sb.Append(securityPrice.LastPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //BidPrice
                                    if (securityPrice != null && securityPrice.BidPrc.HasValue)
                                        sb.Append(securityPrice.BidPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //AskPrice
                                    if (securityPrice != null && securityPrice.AskPrc.HasValue)
                                        sb.Append(securityPrice.AskPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ProxyFormula
                                    if (!string.IsNullOrEmpty(data.ProxyForm))
                                        sb.Append(string.Concat("'", data.ProxyForm, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFReg3MFormula
                                    if (!string.IsNullOrEmpty(fundMaster.RegExp3M))
                                        sb.Append(string.Concat("'", fundMaster.RegExp3M, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFReg6MFormula
                                    if (!string.IsNullOrEmpty(fundMaster.RegExp6M))
                                        sb.Append(string.Concat("'", fundMaster.RegExp6M, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFReg12MFormula
                                    if (!string.IsNullOrEmpty(fundMaster.RegExp12M))
                                        sb.Append(string.Concat("'", fundMaster.RegExp12M, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFReg24MFormula
                                    if (!string.IsNullOrEmpty(fundMaster.RegExp24M))
                                        sb.Append(string.Concat("'", fundMaster.RegExp24M, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //LeverageSrc
                                    if (!string.IsNullOrEmpty(fundMaster.LevRatioSrc))
                                        sb.Append(string.Concat("'", fundMaster.LevRatioSrc, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ExpenseRatioSrc
                                    if (!string.IsNullOrEmpty(fundMaster.ExpRatioSrc))
                                        sb.Append(string.Concat("'", fundMaster.ExpRatioSrc, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFReg36MFormula
                                    if (!string.IsNullOrEmpty(fundMaster.RegExp36M))
                                        sb.Append(string.Concat("'", fundMaster.RegExp36M, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFReg60MFormula
                                    if (!string.IsNullOrEmpty(fundMaster.RegExp60M))
                                        sb.Append(string.Concat("'", fundMaster.RegExp60M, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ETFRegLife
                                    if (!string.IsNullOrEmpty(fundMaster.RegExpLife))
                                        sb.Append(string.Concat("'", fundMaster.RegExpLife, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RSquared3M
                                    if (fundMaster.RSqrd3M.HasValue)
                                        sb.Append(fundMaster.RSqrd3M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RSquared6M
                                    if (fundMaster.RSqrd6M.HasValue)
                                        sb.Append(fundMaster.RSqrd6M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RSquared12M
                                    if (fundMaster.RSqrd12M.HasValue)
                                        sb.Append(fundMaster.RSqrd12M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RSquared24M
                                    if (fundMaster.RSqrd24M.HasValue)
                                        sb.Append(fundMaster.RSqrd24M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RSquared36M
                                    if (fundMaster.RSqrd36M.HasValue)
                                        sb.Append(fundMaster.RSqrd36M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RSquared60M
                                    if (fundMaster.RSqrd60M.HasValue)
                                        sb.Append(fundMaster.RSqrd60M).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RSquaredLife
                                    if (fundMaster.RSqrdLife.HasValue)
                                        sb.Append(fundMaster.RSqrdLife).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //CoinsPerUnit
                                    if (data.CoinsPerUnit.HasValue)
                                        sb.Append(data.CoinsPerUnit).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //GeneralProxy
                                    if (fundMaster.FundCondProxyTO != null && !string.IsNullOrEmpty(fundMaster.FundCondProxyTO.ProxyFormula))
                                        sb.Append(string.Concat("'", fundMaster.FundCondProxyTO.ProxyFormula, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //GeneralProxyRtn
                                    if (data.GenProxyRtn.HasValue)
                                        sb.Append(data.GenProxyRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //GeneralProxyNav
                                    if (data.GenProxyNav.HasValue)
                                        sb.Append(data.GenProxyNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition1Formula
                                    if (fundMaster.FundCondProxyTO != null && !string.IsNullOrEmpty(fundMaster.FundCondProxyTO.Cond1Formula))
                                        sb.Append(string.Concat("'", fundMaster.FundCondProxyTO.Cond1Formula, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition1Proxy
                                    if (fundMaster.FundCondProxyTO != null && !string.IsNullOrEmpty(fundMaster.FundCondProxyTO.Cond1ProxyFormula))
                                        sb.Append(string.Concat("'", fundMaster.FundCondProxyTO.Cond1ProxyFormula, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition1Rtn
                                    if (data.Cond1ProxyRtn.HasValue)
                                        sb.Append(data.Cond1ProxyRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition1Nav
                                    if (data.Cond1ProxyNav.HasValue)
                                        sb.Append(data.Cond1ProxyNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition2Formula
                                    if (fundMaster.FundCondProxyTO != null && !string.IsNullOrEmpty(fundMaster.FundCondProxyTO.Cond2Formula))
                                        sb.Append(string.Concat("'", fundMaster.FundCondProxyTO.Cond2Formula, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition2Proxy
                                    if (fundMaster.FundCondProxyTO != null && !string.IsNullOrEmpty(fundMaster.FundCondProxyTO.Cond2ProxyFormula))
                                        sb.Append(string.Concat("'", fundMaster.FundCondProxyTO.Cond2ProxyFormula, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition2Rtn
                                    if (data.Cond2ProxyRtn.HasValue)
                                        sb.Append(data.Cond2ProxyRtn).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Condition2Nav
                                    if (data.Cond2ProxyNav.HasValue)
                                        sb.Append(data.Cond2ProxyNav).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ConditionProxyType
                                    if (!string.IsNullOrEmpty(data.CondProxyFlag))
                                        sb.Append(string.Concat("'", data.CondProxyFlag, "'"));
                                    else
                                        sb.Append("null");

                                    string row = sb.ToString();
                                    Rows.Add(string.Concat("(", row, ")"));
                                    sb.Clear();

                                    //if (records > 10)
                                    //    break;
                                }
                            }
                        }
                        sCommand.Append(string.Join(DELIMITER, Rows));
                        sCommand.Append(";");

                        //_logger.LogDebug(sCommand.ToString());

                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Fund Forecasts into database: " + ticker, ex);
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundForecastList"></param>
        public void SaveCAFundForecasts(IList<CAFundForecast> fundForecastList)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                DateTime effectiveDate = TodaysDate;
                string effectiveDateAsString = DateUtils.ConvertDate(effectiveDate, DATEFORMAT, "0000-00-00");

                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.CAFundForecast" +
                    "(Ticker, EffectiveDate, PublishedNav, PublishedAdjNav, PublishedNavSource, PublishedNavDate," +
                    " EstimatedNav, EstimatedNavRtn, EstimatedNavRtnLocal, EstimatedCurrencyInclLevRtn," +
                    " EstimatedCurrencyHedgedRtn, EstimatedNavSource,PublishedDscnt, DsnctChange," +
                    " DscntToLastPrice, DscntToBidPrice, DscntToAskPrice, IRRLastPrice, IRRBidPrice, IRRAskPrice," +
                    " Leverage, ExpenseRatio, ExtraAdjustment, LastPrice, BidPrice, AskPrice," +
                    " PfdTicker, PfdSedol, PfdRedemptionValue, PfdRedemptionInt, PfdLastPrice, PfdBidPrice, PfdAskPrice," +
                    " PfdRedemptionValueInclInNav, PfdRedemptionValueAdjCommonPfdRatio, PfdTakenOutAtRedemptionPrice," +
                    " NumPfdsPerCommonSplitTrusts, PfdRedemptionFreq, PfdLeverageFactor, NavUpdateFreq," +
                    " FirstRedemptionDate, NextRedemptionDate, NextNotificationDate, SettlementDate, NextRedemptionPaymentDate," +
                    " ExpensesToRedemptionTerm, LastPriceIRR, BidPriceIRR, AskPriceIRR, NavLessExpenses," +
                    " FundType, NavUpdateDate, CommissionPerShare, FundRedemptionFee, FundRedemptionFixedFee," +
                    " PfdRedemptionFee, MaxAnnualTender, FundStructure, RedemptionDaysMonthEnd, RedemptionDayType," +
                    " NotificationDaysMonthEnd, PaymentDayType, PaymentDaysMonthEnd, PriceChng" +
                    ") values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.CAFundForecast where EffectiveDate = '" + effectiveDateAsString + "'");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.CAFundForecast where EffectiveDate = '" + effectiveDateAsString + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        //int records = 0;
                        List<string> Rows = new List<string>();
                        foreach (CAFundForecast data in fundForecastList)
                        {
                            //Ticker
                            sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);

                            //EffectiveDate
                            sb.Append(string.Concat("'", effectiveDateAsString, "'")).Append(DELIMITER);

                            //PublishedNav
                            if (data.LastNav.HasValue)
                                sb.Append(data.LastNav).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PublishedAdjNav
                            if (data.LastDvdAdjNav.HasValue)
                                sb.Append(data.LastDvdAdjNav).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PublishedNavSource
                            sb.Append(string.Concat("'", data.LastNavSource, "'")).Append(DELIMITER);

                            //PublishedNavDate
                            sb.Append(string.Concat("'", data.LastNavDateAsString, "'")).Append(DELIMITER);

                            //EstimatedNav
                            if (data.EstimatedNav.HasValue)
                                sb.Append(data.EstimatedNav).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EstimatedNavRtn
                            if (data.EstimatedNavRtn.HasValue)
                                sb.Append(data.EstimatedNavRtn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EstimatedNavRtnLocal
                            if (data.EstimatedNavRtnLocal.HasValue)
                                sb.Append(data.EstimatedNavRtnLocal).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EstimatedCurrencyInclLevRtn
                            if (data.EstimatedCurrencyInclLevRtn.HasValue)
                                sb.Append(data.EstimatedCurrencyInclLevRtn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EstimatedCurrencyHedgedRtn
                            if (data.EstimatedCurrencyHedgedRtn.HasValue)
                                sb.Append(data.EstimatedCurrencyHedgedRtn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EstimatedNavSource
                            sb.Append(string.Concat("'", data.EstimatedNavSource, "'")).Append(DELIMITER);

                            //PublishedDscnt
                            if (data.LastPctPremium.HasValue)
                                sb.Append(data.LastPctPremium).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DsnctChange
                            if (data.DiscountChange.HasValue)
                                sb.Append(data.DiscountChange).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DscntToLastPrice
                            if (data.DiscountToLastPrice.HasValue)
                                sb.Append(data.DiscountToLastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DscntToBidPrice
                            if (data.DiscountToBidPrice.HasValue)
                                sb.Append(data.DiscountToBidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DscntToAskPrice
                            if (data.DiscountToAskPrice.HasValue)
                                sb.Append(data.DiscountToAskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //IRRLastPrice
                            if (data.IRRToLastPrice.HasValue)
                                sb.Append(data.IRRToLastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //IRRBidPrice
                            if (data.IRRToBidPrice.HasValue)
                                sb.Append(data.IRRToBidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //IRRAskPrice
                            if (data.IRRToAskPrice.HasValue)
                                sb.Append(data.IRRToAskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Leverage
                            if (data.LeverageRatio.HasValue)
                                sb.Append(data.LeverageRatio).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ExpenseRatio
                            if (data.ExpenseRatio.HasValue)
                                sb.Append(data.ExpenseRatio).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ExtraAdjustment
                            if (data.RedempExtraAdjustment.HasValue)
                                sb.Append(data.RedempExtraAdjustment).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //LastPrice
                            if (data.LastPrice.HasValue)
                                sb.Append(data.LastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //BidPrice
                            if (data.BidPrice.HasValue)
                                sb.Append(data.BidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //AskPrice
                            if (data.AskPrice.HasValue)
                                sb.Append(data.AskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdTicker
                            sb.Append(string.Concat("'", data.PreferredShareTicker, "'")).Append(DELIMITER);

                            //PfdSedol
                            sb.Append(string.Concat("'", data.PreferredShareSedol, "'")).Append(DELIMITER);

                            //PfdRedemptionValue
                            if (data.PreferredRedemptionValue.HasValue)
                                sb.Append(data.PreferredRedemptionValue).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdRedemptionInt
                            if (data.PreferredRedemptionInt.HasValue)
                                sb.Append(data.PreferredRedemptionInt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdLastPrice
                            if (data.PreferredLastPrice.HasValue)
                                sb.Append(data.PreferredLastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdBidPrice
                            if (data.PreferredBidPrice.HasValue)
                                sb.Append(data.PreferredBidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdAskPrice
                            if (data.PreferredAskPrice.HasValue)
                                sb.Append(data.PreferredAskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdRedemptionValueInclInNav
                            sb.Append(string.Concat("'", data.PfdRedemptionValueInclInNav, "'")).Append(DELIMITER);

                            //PfdRedemptionValueAdjCommonPfdRatio
                            if (data.PfdRedemptionValueAdjCommonPfdRatio.HasValue)
                                sb.Append(data.PfdRedemptionValueAdjCommonPfdRatio).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdTakenOutAtRedemptionPrice
                            sb.Append(string.Concat("'", data.PfdTakenOutAtRedemptionPrice, "'")).Append(DELIMITER);

                            //NumPfdsPerCommonSplitTrusts
                            if (data.NumPfdsPerCommonSplitTrusts.HasValue)
                                sb.Append(data.NumPfdsPerCommonSplitTrusts).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdRedemptionFreq
                            if (data.PfdRedemptionFreq.HasValue)
                                sb.Append(data.PfdRedemptionFreq).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdLeverageFactor
                            if (data.PfdLeverageFactor.HasValue)
                                sb.Append(data.PfdLeverageFactor).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //NavUpdateFreq
                            sb.Append(string.Concat("'", data.NavUpdateFreq, "'")).Append(DELIMITER);

                            //FirstRedemptionDate
                            sb.Append(string.Concat("'", data.FirstRedemptionDateAsString, "'")).Append(DELIMITER);

                            //NextRedemptionDate
                            sb.Append(string.Concat("'", data.NextRedemptionDateAsString, "'")).Append(DELIMITER);

                            //NextNotificationDate
                            sb.Append(string.Concat("'", data.NextNotificationDateAsString, "'")).Append(DELIMITER);

                            //SettlementDate
                            sb.Append(string.Concat("'", data.SettlementDateAsString, "'")).Append(DELIMITER);

                            //NextRedemptionPaymentDate
                            sb.Append(string.Concat("'", data.NextRedemptionPaymentDateAsString, "'")).Append(DELIMITER);

                            //ExpensesToRedemptionTerm
                            if (data.ExpensesToRedemptionTerm.HasValue)
                                sb.Append(data.ExpensesToRedemptionTerm).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //LastPriceIRR
                            if (data.LastPriceIRR.HasValue)
                                sb.Append(data.LastPriceIRR).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //BidPriceIRR
                            if (data.BidPriceIRR.HasValue)
                                sb.Append(data.BidPriceIRR).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //AskPriceIRR
                            if (data.AskPriceIRR.HasValue)
                                sb.Append(data.AskPriceIRR).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //NavLessExpenses
                            if (data.NavLessExpenses.HasValue)
                                sb.Append(data.NavLessExpenses).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundType
                            sb.Append(string.Concat("'", data.FundType, "'")).Append(DELIMITER);

                            //NavUpdateDate
                            sb.Append(string.Concat("'", data.NavUpdateDate, "'")).Append(DELIMITER);

                            //CommissionPerShare
                            if (data.CommissionPerShare.HasValue)
                                sb.Append(data.CommissionPerShare).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundRedemptionFee
                            if (data.FundRedemptionFee.HasValue)
                                sb.Append(data.FundRedemptionFee).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundRedemptionFixedFee
                            if (data.FundRedemptionFixedFee.HasValue)
                                sb.Append(data.FundRedemptionFixedFee).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdRedemptionFee
                            if (data.PfdRedemptionFee.HasValue)
                                sb.Append(data.PfdRedemptionFee).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //MaxAnnualTender
                            if (data.MaxAnnualTender.HasValue)
                                sb.Append(data.MaxAnnualTender).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundStructure
                            sb.Append(string.Concat("'", data.FundStructure, "'")).Append(DELIMITER);

                            //RedemptionDaysMonthEnd
                            if (data.RedemptionDaysMonthEnd.HasValue)
                                sb.Append(data.RedemptionDaysMonthEnd).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //RedemptionDayType
                            sb.Append(string.Concat("'", data.RedemptionDayType, "'")).Append(DELIMITER);

                            //NotificationDaysMonthEnd
                            if (data.NotificationDaysMonthEnd.HasValue)
                                sb.Append(data.NotificationDaysMonthEnd).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PaymentDayType
                            sb.Append(string.Concat("'", data.PaymentDayType, "'")).Append(DELIMITER);

                            //PaymentDaysMonthEnd
                            if (data.PaymentDaysMonthEnd.HasValue)
                                sb.Append(data.PaymentDaysMonthEnd).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PriceChng
                            if (data.PriceChng.HasValue)
                                sb.Append(data.PriceChng);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();

                            //if (records > 10)
                            //    break;
                        }
                        sCommand.Append(string.Join(DELIMITER, Rows));
                        sCommand.Append(";");

                        //_logger.LogDebug(sCommand.ToString());

                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving CA Fund Forecasts into database");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundHoldingsReturnList"></param>
        public void SavePortHoldingReturns(IList<FundHoldingsReturn> fundHoldingsReturnList)
        {
            try
            {
                TruncateTable(StgFundForecastPortRtnsTableName);
                SavePortHoldingReturnsStg(fundHoldingsReturnList);
                MoveDataToTargetTable(SaveFundForecastPortRtnsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundHoldingsReturnList"></param>
        public void SavePortHoldingReturnsStg(IList<FundHoldingsReturn> fundHoldingsReturnList)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgFundForecastPortRtnHist" +
                    " (Ticker, EffectiveDate, TotalRtn, CashRtn, EquityRtn, FIRtn, FxHedgeRtn," +
                    " TotalWt, CashWt, EquityWt, FIWt, FxHedgeWt, EquityPriceCoverage) values ");

                DateTime effectiveDate = TodaysDate;
                string effectiveDateAsString = DateUtils.ConvertDate(effectiveDate, DATEFORMAT, "0000-00-00");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (FundHoldingsReturn data in fundHoldingsReturnList)
                        {
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EffectiveDate
                            sb.Append(string.Concat("'", effectiveDateAsString, "'")).Append(DELIMITER);

                            //TotalRtn
                            if (data.TotalReturn.HasValue)
                                sb.Append(data.TotalReturn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //CashRtn
                            if (data.CashReturn.HasValue)
                                sb.Append(data.CashReturn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EquityRtn
                            if (data.EquityReturn.HasValue)
                                sb.Append(data.EquityReturn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FIRtn
                            if (data.FIReturn.HasValue)
                                sb.Append(data.FIReturn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FxHedgeRtn
                            if (data.FXHedgeReturn.HasValue)
                                sb.Append(data.FXHedgeReturn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //TotalWt
                            if (data.Weight.HasValue)
                                sb.Append(data.Weight).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //CashWt
                            if (data.CashWeight.HasValue)
                                sb.Append(data.CashWeight).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EquityWt
                            if (data.EquityWeight.HasValue)
                                sb.Append(data.EquityWeight).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FIWt
                            if (data.FIWeight.HasValue)
                                sb.Append(data.FIWeight).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FxHedgeWt
                            if (data.FXHedgeWeight.HasValue)
                                sb.Append(data.FXHedgeWeight).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EquityPriceCoverage
                            if (data.EquityPriceCoverage.HasValue)
                                sb.Append(data.EquityPriceCoverage);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        //_logger.LogDebug(sCommand.ToString());

                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Fund Port Holding returns");
                throw;
            }
        }

        public IDictionary<string, FundCondProxy> GetFundConditionalProxies()
        {
            IDictionary<string, FundCondProxy> dict = new Dictionary<string, FundCondProxy>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundConditionalProxiesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCondProxy data = new FundCondProxy();
                                data.Ticker = reader["Ticker"] as string;
                                data.ProxyFormula = reader["Proxy"] as string;
                                data.Cond1Formula = reader["Condition1"] as string;
                                data.Cond1ProxyFormula = reader["Condition1Proxy"] as string;
                                data.Cond2Formula = reader["Condition2"] as string;
                                data.Cond2ProxyFormula = reader["Condition2Proxy"] as string;
                                dict.Add(data.Ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return dict;
        }

        private const string GetFundConditionalProxiesQuery = "select * from almitasc_ACTradingBBGData.FundProxyMst";
        private const string StgFundForecastTableName = "almitasc_ACTradingBBGData.StgFundForecast";
        private const string StgFundPDStatsTableName = "almitasc_ACTradingBBGData.StgFundPDStats";
        private const string SaveFundForecastsQuery = "spPopulateFundForecasts";
        private const string SaveFundPDStatsQuery = "spPopulateFundPDStats";
        private const string StgFundForecastPortRtnsTableName = "almitasc_ACTradingBBGData.StgFundForecastPortRtnHist";
        private const string SaveFundForecastPortRtnsQuery = "spPopulateFundForecastPortRtns";
    }
}