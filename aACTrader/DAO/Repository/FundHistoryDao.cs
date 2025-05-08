using aACTrader.DAO.Interface;
using aCommons;
using aCommons.DTO.CEFA;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class FundHistoryDao : IFundHistoryDao
    {
        private readonly ILogger<FundHistoryDao> _logger;

        public FundHistoryDao(ILogger<FundHistoryDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing FundHistoryDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<FundHistory> GetFundHistory(string ticker)
        {
            IList<FundHistory> list = new List<FundHistory>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHistory data = new FundHistory
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    ClosingPrice = (reader.IsDBNull(reader.GetOrdinal("ClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosePrice")),
                                    VWAPPrice = (reader.IsDBNull(reader.GetOrdinal("VWAPPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VWAPPrice")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    NavInterpolated = (reader.IsDBNull(reader.GetOrdinal("NavInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavInterpolated")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    PDInterpolated = (reader.IsDBNull(reader.GetOrdinal("PDInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDInterpolated")),
                                    Volume = (reader.IsDBNull(reader.GetOrdinal("Volume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Volume")),
                                    PriceReturn = (reader.IsDBNull(reader.GetOrdinal("PriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceRtn")),
                                    VWAPPriceReturn = (reader.IsDBNull(reader.GetOrdinal("VWAPPriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VWAPPriceRtn")),
                                    NavReturn = (reader.IsDBNull(reader.GetOrdinal("NavRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavRtn")),
                                    DiscountReturn = (reader.IsDBNull(reader.GetOrdinal("DvdRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdRtn")),
                                    CashDividend = (reader.IsDBNull(reader.GetOrdinal("CashDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashDvd")),
                                    StockDividend = (reader.IsDBNull(reader.GetOrdinal("StockDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockDvd")),
                                    StockSplit = (reader.IsDBNull(reader.GetOrdinal("StockSplit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockSplit")),
                                    CorpAction = reader["CorpAction"] as string,
                                    DividendIndicative = (reader.IsDBNull(reader.GetOrdinal("DvdIndicative"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdIndicative")),
                                    DividendTrailing = (reader.IsDBNull(reader.GetOrdinal("DvdTrailing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdTrailing")),

                                    CEFASharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("CEFASharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFASharesOutstanding")),
                                    CEFAExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("CEFANonLevExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFANonLevExpenseRatio")),
                                    CEFALeverageRatio = (reader.IsDBNull(reader.GetOrdinal("CEFALeverageRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFALeverageRatio")),
                                    CEFAIncomeYield = (reader.IsDBNull(reader.GetOrdinal("CEFAIncYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFAIncYield")),
                                    CEFANavYield = (reader.IsDBNull(reader.GetOrdinal("CEFANAVYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFANAVYield")),
                                    CEFATotalYield = (reader.IsDBNull(reader.GetOrdinal("CEFATotalYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFATotalYield")),

                                    FundCurrency = reader["FundCurrency"] as string,
                                    NumisNavCurrency = reader["NumisNavCurrency"] as string,
                                    PHNavCurrency = reader["PHCurrency"] as string,
                                    BBGNavCurrency = reader["BBGCurrency"] as string,
                                    NumisNavFrequency = reader["NumisNavFreq"] as string,

                                    NumisPubNavDate = (reader.IsDBNull(reader.GetOrdinal("NumisNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NumisNavDate")),
                                    BBGPubNavDate = (reader.IsDBNull(reader.GetOrdinal("BBGNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BBGNavDate")),

                                    NumisPubNav = (reader.IsDBNull(reader.GetOrdinal("NumisPubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPubNav")),
                                    PHCumFairNav = (reader.IsDBNull(reader.GetOrdinal("PHCumFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHCumFairNav")),
                                    PHCumParNav = (reader.IsDBNull(reader.GetOrdinal("PHCumParNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHCumParNav")),
                                    BBGNav = (reader.IsDBNull(reader.GetOrdinal("BBGPubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPubNav")),
                                    NumisEstNav = (reader.IsDBNull(reader.GetOrdinal("NumisEstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisEstNav")),

                                    MSCumFairNav = (reader.IsDBNull(reader.GetOrdinal("MSCumFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSCumFairNav"))
                                };

                                data.MSCumFairNav = (reader.IsDBNull(reader.GetOrdinal("MSCumFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSCumFairNav"));
                                data.MSExFairNav = (reader.IsDBNull(reader.GetOrdinal("MSExFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSExFairNav"));
                                data.MSExParNav = (reader.IsDBNull(reader.GetOrdinal("MSExParNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSExParNav"));

                                data.NumisPD = (reader.IsDBNull(reader.GetOrdinal("NumisPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPD"));
                                data.BBGPD = (reader.IsDBNull(reader.GetOrdinal("BBGPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPD"));
                                data.PHPD = (reader.IsDBNull(reader.GetOrdinal("PHPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHPD"));
                                data.MSPD = (reader.IsDBNull(reader.GetOrdinal("MSPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSPD"));

                                data.PubNav = (reader.IsDBNull(reader.GetOrdinal("PubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubNav"));
                                data.PubNavAdj = (reader.IsDBNull(reader.GetOrdinal("PubNavAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubNavAdj"));
                                data.PubNavSource = reader["PubNavSource"] as string;
                                data.PubNavDate = (reader.IsDBNull(reader.GetOrdinal("PubNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PubNavDate"));

                                data.EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav"));
                                data.EstNavSource = reader["EstNavSource"] as string;
                                data.PortNav = (reader.IsDBNull(reader.GetOrdinal("PortNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortNav"));
                                data.ETFRegNav = (reader.IsDBNull(reader.GetOrdinal("ETFRegNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ETFRegNav"));
                                data.ProxyNav = (reader.IsDBNull(reader.GetOrdinal("ProxyNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProxyNav"));

                                data.PubDiscount = (reader.IsDBNull(reader.GetOrdinal("PubDscnt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubDscnt"));
                                data.DiscountToLastPrice = (reader.IsDBNull(reader.GetOrdinal("DscntToLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DscntToLastPrice"));
                                data.DiscountToBidPrice = (reader.IsDBNull(reader.GetOrdinal("DscntToBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DscntToBidPrice"));
                                data.DiscountToAskPrice = (reader.IsDBNull(reader.GetOrdinal("DscntToAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DscntToAskPrice"));
                                data.UnLeveragedDiscountToLastPrice = (reader.IsDBNull(reader.GetOrdinal("UnlevDscntToLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnlevDscntToLastPrice"));
                                data.UnLeveragedDiscountToBidPrice = (reader.IsDBNull(reader.GetOrdinal("UnlevDscntToBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnlevDscntToBidPrice"));
                                data.UnLeveragedDiscountToAskPrice = (reader.IsDBNull(reader.GetOrdinal("UnlevDscntToAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnlevDscntToAskPrice"));

                                data.ExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio"));
                                data.LeverageRatio = (reader.IsDBNull(reader.GetOrdinal("Leverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leverage"));
                                data.AccrualRate = (reader.IsDBNull(reader.GetOrdinal("AccrualRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccrualRate"));
                                data.AccruedInterest = (reader.IsDBNull(reader.GetOrdinal("AccruedInterest"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedInterest"));
                                data.IRRToLastPrice = (reader.IsDBNull(reader.GetOrdinal("IRRLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRLastPrice"));
                                data.IRRToBidPrice = (reader.IsDBNull(reader.GetOrdinal("IRRBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRBidPrice"));
                                data.IRRToAskPrice = (reader.IsDBNull(reader.GetOrdinal("IRRAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRAskPrice"));

                                data.LiveZScore1M = (reader.IsDBNull(reader.GetOrdinal("ZScore1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore1M"));
                                data.LiveZScore3M = (reader.IsDBNull(reader.GetOrdinal("ZScore3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore3M"));
                                data.LiveZScore6M = (reader.IsDBNull(reader.GetOrdinal("ZScore6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore6M"));
                                data.LiveZScore12M = (reader.IsDBNull(reader.GetOrdinal("ZScore12M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore12M"));
                                data.LiveZScore24M = (reader.IsDBNull(reader.GetOrdinal("ZScore24M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore24M"));
                                data.LiveZScore36M = (reader.IsDBNull(reader.GetOrdinal("ZScore36M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore36M"));
                                data.LiveZScore60M = (reader.IsDBNull(reader.GetOrdinal("ZScore60M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore60M"));
                                data.LiveZScoreLife = (reader.IsDBNull(reader.GetOrdinal("ZScoreLife"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScoreLife"));

                                data.LiveDScore1M = (reader.IsDBNull(reader.GetOrdinal("DScore1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScore1M"));
                                data.LiveDScore3M = (reader.IsDBNull(reader.GetOrdinal("DScore3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScore3M"));
                                data.LiveDScore6M = (reader.IsDBNull(reader.GetOrdinal("DScore6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScore6M"));
                                data.LiveDScore12M = (reader.IsDBNull(reader.GetOrdinal("DScore12M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScore12M"));
                                data.LiveDScore24M = (reader.IsDBNull(reader.GetOrdinal("DScore24M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScore24M"));
                                data.LiveDScore36M = (reader.IsDBNull(reader.GetOrdinal("DScore36M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScore36M"));
                                data.LiveDScore60M = (reader.IsDBNull(reader.GetOrdinal("DScore60M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScore60M"));
                                data.LiveDScoreLife = (reader.IsDBNull(reader.GetOrdinal("DScoreLife"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DScoreLife"));

                                data.EADiscountGroup = reader["EADiscountGroup"] as string;
                                data.EAFinalAlpha = (reader.IsDBNull(reader.GetOrdinal("EAFinalAlpha"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAFinalAlpha"));
                                data.EAAlpha = (reader.IsDBNull(reader.GetOrdinal("EAAlpha"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAAlpha"));
                                data.EAExtraAdjustment = (reader.IsDBNull(reader.GetOrdinal("EAExtraAdjustment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAExtraAdjustment"));
                                data.EADiscountRtn = (reader.IsDBNull(reader.GetOrdinal("EADiscountRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EADiscountRtn"));
                                data.EADividendRtn = (reader.IsDBNull(reader.GetOrdinal("EADividendRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EADividendRtn"));
                                data.EAExpenseDrag = (reader.IsDBNull(reader.GetOrdinal("EAExpenseDrag"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAExpenseDrag"));
                                data.EANavAccretionShareBuyback = (reader.IsDBNull(reader.GetOrdinal("EANavAccretionShareBuyback"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EANavAccretionShareBuyback"));
                                data.EAExpectedDiscount = (reader.IsDBNull(reader.GetOrdinal("EAExpectedDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAExpectedDiscount"));
                                data.EADiscountChange = (reader.IsDBNull(reader.GetOrdinal("EADiscountChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EADiscountChange"));
                                data.EAActivistScore = (reader.IsDBNull(reader.GetOrdinal("EAActivistScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAActivistScore"));
                                data.EALiquidityCost = (reader.IsDBNull(reader.GetOrdinal("EALiquidityCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EALiquidityCost"));
                                data.EAShareBuyback = (reader.IsDBNull(reader.GetOrdinal("EAShareBuyback"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAShareBuyback"));
                                data.EALongTermAvgSectorDiscount = (reader.IsDBNull(reader.GetOrdinal("EALTAvgSectorDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EALTAvgSectorDiscount"));
                                data.EADScoreMovement = (reader.IsDBNull(reader.GetOrdinal("EADScoreMovement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EADScoreMovement"));
                                data.EAActivistScore = (reader.IsDBNull(reader.GetOrdinal("EAActivistScoreAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAActivistScoreAdj"));
                                data.EABoardVoting = (reader.IsDBNull(reader.GetOrdinal("EABoardVotingAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EABoardVotingAdj"));
                                data.EABoardTerm = (reader.IsDBNull(reader.GetOrdinal("EABoardTermAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EABoardTermAdj"));
                                data.EAForecastedDividend = (reader.IsDBNull(reader.GetOrdinal("EAForecastedDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAForecastedDvd"));

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.PubNavDateAsString = DateUtils.ConvertDate(data.PubNavDate, "yyyy-MM-dd");
                                data.NumisPubNavDateAsString = DateUtils.ConvertDate(data.NumisPubNavDate, "yyyy-MM-dd");
                                data.BBGPubNavDateAsString = DateUtils.ConvertDate(data.BBGPubNavDate, "yyyy-MM-dd");

                                list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<CAFundHistory> GetCAFundHistory(string ticker)
        {
            IList<CAFundHistory> list = new List<CAFundHistory>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetCAFundHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CAFundHistory data = new CAFundHistory
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    FundCurrency = reader["FundCurrency"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),

                                    ClosingPrice = (reader.IsDBNull(reader.GetOrdinal("ClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosePrice")),
                                    VWAPPrice = (reader.IsDBNull(reader.GetOrdinal("VWAPPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VWAPPrice")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    NavInterpolated = (reader.IsDBNull(reader.GetOrdinal("NavInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavInterpolated")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    PDInterpolated = (reader.IsDBNull(reader.GetOrdinal("PDInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDInterpolated")),
                                    Volume = (reader.IsDBNull(reader.GetOrdinal("Volume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Volume")),
                                    PriceReturn = (reader.IsDBNull(reader.GetOrdinal("PriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceRtn")),
                                    VWAPPriceReturn = (reader.IsDBNull(reader.GetOrdinal("VWAPPriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VWAPPriceRtn")),
                                    NavReturn = (reader.IsDBNull(reader.GetOrdinal("NavRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavRtn")),
                                    DiscountReturn = (reader.IsDBNull(reader.GetOrdinal("DvdRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdRtn")),
                                    CashDividend = (reader.IsDBNull(reader.GetOrdinal("CashDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashDvd")),
                                    StockDividend = (reader.IsDBNull(reader.GetOrdinal("StockDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockDvd")),
                                    StockSplit = (reader.IsDBNull(reader.GetOrdinal("StockSplit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockSplit")),
                                    CorpAction = reader["CorpAction"] as string,
                                    DividendIndicative = (reader.IsDBNull(reader.GetOrdinal("DvdIndicative"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdIndicative")),
                                    DividendTrailing = (reader.IsDBNull(reader.GetOrdinal("DvdTrailing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdTrailing")),

                                    PubNav = (reader.IsDBNull(reader.GetOrdinal("PublishedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedNav")),
                                    PubNavAdj = (reader.IsDBNull(reader.GetOrdinal("PublishedAdjNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedAdjNav")),
                                    PubNavSource = reader["PubNavSource"] as string,
                                    PubNavDate = (reader.IsDBNull(reader.GetOrdinal("PublishedNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PublishedNavDate")),

                                    //Estimated Navs
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstimatedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNav")),
                                    EstNavSource = reader["EstimatedNavSource"] as string,
                                    EstNavRtn = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNavRtn")),
                                    EstNavRtnLocal = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavRtnLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNavRtnLocal")),
                                    EstNavCurrencyHedgedRtn = (reader.IsDBNull(reader.GetOrdinal("EstimatedCurrencyHedgedRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedCurrencyHedgedRtn")),

                                    //Discounts
                                    PubDiscount = (reader.IsDBNull(reader.GetOrdinal("PublishedDscnt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedDscnt")),
                                    DiscountToLastPrice = (reader.IsDBNull(reader.GetOrdinal("DscntToLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DscntToLastPrice")),
                                    DiscountToBidPrice = (reader.IsDBNull(reader.GetOrdinal("DscntToBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DscntToBidPrice")),
                                    DiscountToAskPrice = (reader.IsDBNull(reader.GetOrdinal("DscntToAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DscntToAskPrice")),

                                    ExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio")),
                                    LeverageRatio = (reader.IsDBNull(reader.GetOrdinal("Leverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leverage")),

                                    //IRR
                                    IRRToLastPrice = (reader.IsDBNull(reader.GetOrdinal("IRRLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRLastPrice")),
                                    IRRToBidPrice = (reader.IsDBNull(reader.GetOrdinal("IRRBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRBidPrice")),
                                    IRRToAskPrice = (reader.IsDBNull(reader.GetOrdinal("IRRAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRAskPrice")),

                                    //Preferred Security Details
                                    PfdSedol = reader["PfdSedol"] as string,
                                    PfdRedemptionFreq = reader["PfdRedemptionFreq"] as string,
                                    PfdRedemptionValue = (reader.IsDBNull(reader.GetOrdinal("PfdRedemptionValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PfdRedemptionValue")),
                                    PfdRedemptionInt = (reader.IsDBNull(reader.GetOrdinal("PfdRedemptionInt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PfdRedemptionInt")),
                                    ExpensesToRedemptionTerm = (reader.IsDBNull(reader.GetOrdinal("ExpensesToRedemptionTerm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpensesToRedemptionTerm")),
                                    FirstRedemptionDate = reader.IsDBNull(reader.GetOrdinal("FirstRedemptionDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FirstRedemptionDate")),
                                    NextRedemptionDate = reader.IsDBNull(reader.GetOrdinal("NextRedemptionDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NextRedemptionDate")),
                                    NextNotificationDate = reader.IsDBNull(reader.GetOrdinal("NextNotificationDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NextNotificationDate")),
                                    NextRedemptionPaymentDate = reader.IsDBNull(reader.GetOrdinal("NextRedemptionPaymentDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NextRedemptionPaymentDate"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.PubNavDateAsString = DateUtils.ConvertDate(data.PubNavDate, "yyyy-MM-dd");
                                data.FirstRedemptionDateAsString = DateUtils.ConvertDate(data.FirstRedemptionDate, "yyyy-MM-dd");
                                data.NextRedemptionDateAsString = DateUtils.ConvertDate(data.NextRedemptionDate, "yyyy-MM-dd");
                                data.NextNotificationDateAsString = DateUtils.ConvertDate(data.NextNotificationDate, "yyyy-MM-dd");
                                data.NextRedemptionPaymentDateAsString = DateUtils.ConvertDate(data.NextRedemptionPaymentDate, "yyyy-MM-dd");

                                list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundHistoryTO> GetFundHistoryDetails(string ticker, DateTime startDate, DateTime endDate, string navFeq)
        {
            IList<FundHistoryTO> list = new List<FundHistoryTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundHistoryDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);
                        command.Parameters.AddWithValue("p_NavFreq", navFeq);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHistoryTO data = new FundHistoryTO()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Date = reader.IsDBNull(reader.GetOrdinal("Date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Date")),
                                    ClsPrc = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    VWAPPrc = (reader.IsDBNull(reader.GetOrdinal("VWAPPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VWAPPrice")),
                                    Vol = (reader.IsDBNull(reader.GetOrdinal("Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Vol")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    NavIntp = (reader.IsDBNull(reader.GetOrdinal("NavInterp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavInterp")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    PDIntrp = (reader.IsDBNull(reader.GetOrdinal("PDInterp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDInterp")),
                                    CashDvd = (reader.IsDBNull(reader.GetOrdinal("CashDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashDvd")),
                                    StkDvd = (reader.IsDBNull(reader.GetOrdinal("StockDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockDvd")),
                                    StkSplit = (reader.IsDBNull(reader.GetOrdinal("StockSplit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockSplit")),
                                    PrcRtn = (reader.IsDBNull(reader.GetOrdinal("ClsPriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPriceRtn")),
                                    NavRtn = (reader.IsDBNull(reader.GetOrdinal("NavRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavRtn")),
                                    DvdInd = (reader.IsDBNull(reader.GetOrdinal("DvdInd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdInd")),
                                    DvdTrg = (reader.IsDBNull(reader.GetOrdinal("DvdTrg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdTrg")),
                                    ShOut = (reader.IsDBNull(reader.GetOrdinal("ShOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShOut")),
                                };
                                list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="effectiveDate"></param>
        /// <returns></returns>
        public FundHistoryScalar GetFundHistoryScalar(string ticker, DateTime effectiveDate, int lookbackDays)
        {
            FundHistoryScalar data = new FundHistoryScalar();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundHistoryScalarQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Date", effectiveDate);
                        command.Parameters.AddWithValue("p_LookbackDays", lookbackDays);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                data = new FundHistoryScalar()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    LastNavDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    LastNav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    LastPctPremium = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    ClosePrice = (reader.IsDBNull(reader.GetOrdinal("PriceClose"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceClose")),
                                    VWAPPrice = (reader.IsDBNull(reader.GetOrdinal("PriceVWAP"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceVWAP")),
                                };
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
            return data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="dvdDateField"></param>
        /// <returns></returns>
        public FundDividendScalar GetFundDividendScalar(string ticker, DateTime startDate, DateTime endDate, string dvdDateField)
        {
            FundDividendScalar data = new FundDividendScalar();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundDividendScalarQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);
                        command.Parameters.AddWithValue("p_DateField", dvdDateField);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                data = new FundDividendScalar()
                                {
                                    DvdAmt = (reader.IsDBNull(reader.GetOrdinal("DvdAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmt")),
                                };
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
            return data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundNavReportTO> GetFundNavReportDetails(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<FundNavReportTO> list = new List<FundNavReportTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundNavEstReportQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavReportTO data = new FundNavReportTO()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Date = reader.IsDBNull(reader.GetOrdinal("EffDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffDate")),
                                    MDate = reader.IsDBNull(reader.GetOrdinal("EffDate")) ? (string)null : reader.GetDateTime(reader.GetOrdinal("EffDate")).ToString("MM/dd/yyyy"),
                                    PubNavDt = reader.IsDBNull(reader.GetOrdinal("PubNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PubNavDate")),
                                    NavSrc = reader["NavSrc"] as string,
                                    EstNavSrc = reader["EstNavSrc"] as string,
                                    NavFreq = reader["NavFreq"] as string,
                                    LevSrc = reader["LeverageSrc"] as string,

                                    PubAdjNav = (reader.IsDBNull(reader.GetOrdinal("PubAdjNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubAdjNav")),
                                    PubNav = (reader.IsDBNull(reader.GetOrdinal("PubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubNav")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    PortNav = (reader.IsDBNull(reader.GetOrdinal("PortNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortNav")),
                                    ETFNav = (reader.IsDBNull(reader.GetOrdinal("ETFNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ETFNav")),
                                    ProxyNav = (reader.IsDBNull(reader.GetOrdinal("ProxyNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProxyNav")),

                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    PubPD = (reader.IsDBNull(reader.GetOrdinal("PubPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubPD")),
                                    PDChng = (reader.IsDBNull(reader.GetOrdinal("PDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDChng")),
                                    EstPD = (reader.IsDBNull(reader.GetOrdinal("EstPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstPD")),
                                    Lev = (reader.IsDBNull(reader.GetOrdinal("Leverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leverage")),

                                    ZS1M = (reader.IsDBNull(reader.GetOrdinal("ZScore1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore1M")),
                                    ZS3M = (reader.IsDBNull(reader.GetOrdinal("ZScore3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore3M")),
                                    ZS6M = (reader.IsDBNull(reader.GetOrdinal("ZScore6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore6M")),
                                    ZS12M = (reader.IsDBNull(reader.GetOrdinal("ZScore12M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore12M")),
                                    ZS24M = (reader.IsDBNull(reader.GetOrdinal("ZScore24M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore24M")),
                                    ZS36M = (reader.IsDBNull(reader.GetOrdinal("ZScore36M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore36M")),
                                };
                                list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<FundNavReportTO> GetLatestFundNavReportDetails(string country, DateTime asofDate)
        {
            IList<FundNavReportTO> list = new List<FundNavReportTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundNavEstLatestReportQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Country", country);
                        command.Parameters.AddWithValue("p_AsOfDate", asofDate);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavReportTO data = new FundNavReportTO()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Date = reader.IsDBNull(reader.GetOrdinal("EffDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffDate")),
                                    PubNavDt = reader.IsDBNull(reader.GetOrdinal("PubNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PubNavDate")),
                                    NavSrc = reader["NavSrc"] as string,
                                    EstNavSrc = reader["EstNavSrc"] as string,
                                    NavFreq = reader["NavFreq"] as string,
                                    LevSrc = reader["LeverageSrc"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    AssetClassLevel1 = reader["AssetClassLevel1"] as string,
                                    AssetClassLevel2 = reader["AssetClassLevel2"] as string,
                                    AssetClassLevel3 = reader["AssetClassLevel3"] as string,
                                    GeoLevel1 = reader["GeoLevel1"] as string,
                                    GeoLevel2 = reader["GeoLevel2"] as string,
                                    GeoLevel3 = reader["GeoLevel3"] as string,

                                    PubAdjNav = (reader.IsDBNull(reader.GetOrdinal("PubAdjNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubAdjNav")),
                                    PubNav = (reader.IsDBNull(reader.GetOrdinal("PubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubNav")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    PortNav = (reader.IsDBNull(reader.GetOrdinal("PortNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortNav")),
                                    ETFNav = (reader.IsDBNull(reader.GetOrdinal("ETFNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ETFNav")),
                                    ProxyNav = (reader.IsDBNull(reader.GetOrdinal("ProxyNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProxyNav")),

                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    PubPD = (reader.IsDBNull(reader.GetOrdinal("PubPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubPD")),
                                    PDChng = (reader.IsDBNull(reader.GetOrdinal("PDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDChng")),
                                    EstPD = (reader.IsDBNull(reader.GetOrdinal("EstPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstPD")),
                                    Lev = (reader.IsDBNull(reader.GetOrdinal("Leverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leverage")),
                                };
                                list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundHistoryTO> GetREITHistoryDetails(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<FundHistoryTO> list = new List<FundHistoryTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetREITHistoryDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHistoryTO data = new FundHistoryTO()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Date = reader.IsDBNull(reader.GetOrdinal("Date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Date")),
                                    ClsPrc = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    VWAPPrc = (reader.IsDBNull(reader.GetOrdinal("VWAPPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VWAPPrice")),
                                    Vol = (reader.IsDBNull(reader.GetOrdinal("Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Vol")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("PubBV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubBV")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PubPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubPD")),
                                    EstBV = (reader.IsDBNull(reader.GetOrdinal("EstBV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstBV")),
                                    EstPD = (reader.IsDBNull(reader.GetOrdinal("EstPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstPD")),
                                    CashDvd = (reader.IsDBNull(reader.GetOrdinal("CashDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashDvd")),
                                    StkDvd = (reader.IsDBNull(reader.GetOrdinal("StockDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockDvd")),
                                    StkSplit = (reader.IsDBNull(reader.GetOrdinal("StockSplit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockSplit")),
                                    PrcRtn = (reader.IsDBNull(reader.GetOrdinal("PriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceRtn")),
                                    NavRtn = (reader.IsDBNull(reader.GetOrdinal("NavRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavRtn")),
                                };
                                list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, IDictionary<DateTime, FundHistoryScalar>> GetFundHistoryFull()
        {
            IDictionary<string, IDictionary<DateTime, FundHistoryScalar>> dict = new Dictionary<string, IDictionary<DateTime, FundHistoryScalar>>();
            string ticker = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundHistoryFullQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHistoryScalar data = new FundHistoryScalar()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    LastNavDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    LastNav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    LastPctPremium = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    ClosePrice = (reader.IsDBNull(reader.GetOrdinal("PriceClose"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceClose")),
                                    VWAPPrice = (reader.IsDBNull(reader.GetOrdinal("PriceVWAP"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceVWAP")),
                                };

                                ticker = data.Ticker;
                                IDictionary<DateTime, FundHistoryScalar> fundHistDict;
                                if (dict.TryGetValue(data.Ticker, out fundHistDict))
                                {
                                    if (!fundHistDict.ContainsKey(data.LastNavDate.GetValueOrDefault()))
                                        fundHistDict.Add(data.LastNavDate.GetValueOrDefault(), data);
                                }
                                else
                                {
                                    fundHistDict = new Dictionary<DateTime, FundHistoryScalar>();
                                    fundHistDict.Add(data.LastNavDate.GetValueOrDefault(), data);
                                    dict.Add(data.Ticker, fundHistDict);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query for Ticker: " + ticker);
                throw;
            }
            return dict;
        }

        public IList<CEFAFieldMstTO> GetCEFAFieldMaster(string fieldName)
        {
            IList<CEFAFieldMstTO> list = new List<CEFAFieldMstTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetCEFAFieldMasterQuery + " where FieldName = '" + fieldName + "'";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CEFAFieldMstTO data = new CEFAFieldMstTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecType = reader["SecType"] as string,
                                    RowCount = (reader.IsDBNull(reader.GetOrdinal("RowCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowCount")),
                                };
                                list.Add(data);
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
            return list;
        }

        public IList<CEFAFieldDetTO> GetCEFAFieldHistory(string ticker, string fieldName)
        {
            IList<CEFAFieldDetTO> list = new List<CEFAFieldDetTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetCEFAFieldDetailsQuery + " where Ticker = '" + ticker + "' and FieldName = '" + fieldName + "' order by AsOfDate desc";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CEFAFieldDetTO data = new CEFAFieldDetTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FieldVal = (reader.IsDBNull(reader.GetOrdinal("FieldVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FieldVal")),
                                    PrevFieldVal = (reader.IsDBNull(reader.GetOrdinal("PrevVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevVal")),
                                    ValChng = (reader.IsDBNull(reader.GetOrdinal("ValChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ValChng")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                };
                                list.Add(data);
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
            return list;
        }

        public IList<FundHistoryTO> GetBBGFundHistory(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<FundHistoryTO> list = new List<FundHistoryTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetBBGFundHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHistoryTO data = new FundHistoryTO()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Date = reader.IsDBNull(reader.GetOrdinal("Date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Date")),
                                    ClsPrc = (reader.IsDBNull(reader.GetOrdinal("Px_Last"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Px_Last")),
                                    VWAPPrc = (reader.IsDBNull(reader.GetOrdinal("Px_Vwap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Px_Vwap")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    PrcRtn = (reader.IsDBNull(reader.GetOrdinal("TrrPx"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TrrPx")),
                                    Vol = (reader.IsDBNull(reader.GetOrdinal("Volume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Volume")),
                                    DvdTrg = (reader.IsDBNull(reader.GetOrdinal("DvdTrailing12m"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdTrailing12m")),
                                    DvdFcst = (reader.IsDBNull(reader.GetOrdinal("DvdForecast12m"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdForecast12m")),
                                    NavTickerDate = reader.IsDBNull(reader.GetOrdinal("NavTickerDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavTickerDate")),
                                    NavTicker = (reader.IsDBNull(reader.GetOrdinal("NavTickerValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavTickerValue")),
                                };
                                list.Add(data);
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
            return list;
        }

        public IDictionary<string, IList<FundPDHistory>> GetCEFFundHistoryFull()
        {
            IDictionary<string, IList<FundPDHistory>> dict = new Dictionary<string, IList<FundPDHistory>>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetCEFFundHistoryFullQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundPDHistory data = new FundPDHistory()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    DaysSince = reader.GetInt32(reader.GetOrdinal("DaysSince")),
                                    Ind1M = reader.GetInt32(reader.GetOrdinal("P1M")),
                                    Ind3M = reader.GetInt32(reader.GetOrdinal("P3M")),
                                    Ind6M = reader.GetInt32(reader.GetOrdinal("P6M")),
                                    Ind1Y = reader.GetInt32(reader.GetOrdinal("P1Y")),
                                    Ind2Y = reader.GetInt32(reader.GetOrdinal("P2Y")),
                                    Ind3Y = reader.GetInt32(reader.GetOrdinal("P3Y")),
                                    Ind5Y = reader.GetInt32(reader.GetOrdinal("P5Y")),
                                    Ind10Y = reader.GetInt32(reader.GetOrdinal("P10Y")),
                                    Ind25Y = reader.GetInt32(reader.GetOrdinal("P25Y")),
                                };

                                if (!dict.TryGetValue(data.Ticker, out IList<FundPDHistory> list))
                                {
                                    list = new List<FundPDHistory>();
                                    dict.Add(data.Ticker, list);
                                }
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query", ex);
                throw;
            }
            return dict;
        }

        private const string GetFundNavEstReportQuery = "spGetFundNavEstReport";
        private const string GetFundNavEstLatestReportQuery = "spGetLatestFundNavEstReportTest";
        private const string GetFundHistoryQuery = "spGetFundHistoryNew";
        private const string GetFundHistoryDetailsQuery = "spGetFundHistoryWeb";
        private const string GetCAFundHistoryQuery = "spGetCAFundHistory";
        private const string GetFundHistoryScalarQuery = "spGetFundHistoryScalar";
        private const string GetFundDividendScalarQuery = "spGetFundDividendScalar";
        private const string GetREITHistoryDetailsQuery = "spGetREITHistory";
        private const string GetFundHistoryFullQuery = "spGetFundFullHistory";
        private const string GetCEFAFieldMasterQuery = "select * from almitasc_ACTradingBBGLink.CEFAFieldMst";
        private const string GetCEFAFieldDetailsQuery = "select * from almitasc_ACTradingBBGLink.CEFAFieldDet";
        private const string GetBBGFundHistoryQuery = "Reporting.spGetBBGFundHistory";
        private const string GetCEFFundHistoryFullQuery = "Reporting.spGetCEFFundHistory";
    }
}