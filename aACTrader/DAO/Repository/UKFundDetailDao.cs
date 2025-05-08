using aACTrader.DAO.Interface;
using aCommons.DTO;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class UKFundDetailDao : IUKFundDetailsDao
    {
        private readonly ILogger<WebDao> _logger;

        public UKFundDetailDao(ILogger<WebDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing UKFundDetailDao...");
        }

        public IList<NumisDetailTO> GetNumisDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<NumisDetailTO> list = new List<NumisDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_ticker", ticker);
                        command.Parameters.AddWithValue("p_source", source);
                        command.Parameters.AddWithValue("p_startDate", startDate);
                        command.Parameters.AddWithValue("p_endDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                NumisDetailTO data = new NumisDetailTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    BBGTicker = reader["BBGTicker"] as string,
                                    FundName = reader["FundName"] as string,
                                    FundGroup = reader["FundGroup"] as string,
                                    Currency = reader["Currency"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    LastNav = (reader.IsDBNull(reader.GetOrdinal("LastNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastNav")),
                                    NavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    NavFreq = reader["NavFreq"] as string,
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    PDAvg12M = (reader.IsDBNull(reader.GetOrdinal("PDAvg12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDAvg12M")),
                                    PDHigh12M = (reader.IsDBNull(reader.GetOrdinal("PDHigh12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDHigh12M")),
                                    PDLow12M = (reader.IsDBNull(reader.GetOrdinal("PDLow12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDLow12M")),
                                    ZScore13Wk = (reader.IsDBNull(reader.GetOrdinal("ZScore13Wk"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ZScore13Wk")),
                                    ZScore1Yr = (reader.IsDBNull(reader.GetOrdinal("ZScore1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ZScore1Yr")),
                                    NavAdjDebt = (reader.IsDBNull(reader.GetOrdinal("NavAdjDebt"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavAdjDebt")),
                                    NavAdjIncome = (reader.IsDBNull(reader.GetOrdinal("NavAdjIncome"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavAdjIncome")),
                                    MktCap = (reader.IsDBNull(reader.GetOrdinal("MktCap"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("MktCap")),
                                    NetAssets = (reader.IsDBNull(reader.GetOrdinal("NetAssets"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NetAssets")),
                                    GrossAssets = (reader.IsDBNull(reader.GetOrdinal("GrossAssets"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GrossAssets")),
                                    SharesOut = (reader.IsDBNull(reader.GetOrdinal("SharesOut"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("SharesOut")),
                                    AvgTradingVol = (reader.IsDBNull(reader.GetOrdinal("AvgTradingVol"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AvgTradingVol")),
                                    BidOfferSpread = (reader.IsDBNull(reader.GetOrdinal("BidOfferSpread"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("BidOfferSpread")),
                                    DvdYield = (reader.IsDBNull(reader.GetOrdinal("DvdYield"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("DvdYield")),
                                    DvdAmount = (reader.IsDBNull(reader.GetOrdinal("DvdAmount"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("DvdAmount")),
                                    DvdFreq = reader["DvdFreq"] as string,
                                    DvdExDate = (reader.IsDBNull(reader.GetOrdinal("DvdExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdExDate")),
                                    Listing = reader["Listing"] as string,
                                    Country = reader["Country"] as string,
                                    InceptionDate = (reader.IsDBNull(reader.GetOrdinal("InceptionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("InceptionDate")),
                                    PriceRtnYTD = (reader.IsDBNull(reader.GetOrdinal("PriceRtnYTD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtnYTD")),
                                    PriceRtn1M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1M")),
                                    PriceRtn3M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn3M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn3M")),
                                    PriceRtn6M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn6M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn6M")),
                                    PriceRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1Yr")),
                                    PriceRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn3Yr")),
                                    PriceRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn5Yr")),
                                    PriceRtn10Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn10Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn10Yr")),
                                    NavRtnYTD = (reader.IsDBNull(reader.GetOrdinal("NavRtnYTD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtnYTD")),
                                    NavRtn1M = (reader.IsDBNull(reader.GetOrdinal("NavRtn1M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn1M")),
                                    NavRtn3M = (reader.IsDBNull(reader.GetOrdinal("NavRtn3M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn3M")),
                                    NavRtn6M = (reader.IsDBNull(reader.GetOrdinal("NavRtn6M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn6M")),
                                    NavRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn1Yr")),
                                    NavRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn3Yr")),
                                    NavRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn5Yr")),
                                    NavRtn10Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn10Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn10Yr")),
                                    Benchmark = reader["Benchmark"] as string,
                                    PriceVol1Yr = (reader.IsDBNull(reader.GetOrdinal("PriceVol1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceVol1Yr")),
                                    NavVol1Yr = (reader.IsDBNull(reader.GetOrdinal("NavVol1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavVol1Yr")),
                                    BenchmarkVol1Yr = (reader.IsDBNull(reader.GetOrdinal("BenchmarkVol1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("BenchmarkVol1Yr")),
                                    PriceSharpeRatio1Yr = (reader.IsDBNull(reader.GetOrdinal("PriceSharpeRatio1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceSharpeRatio1Yr")),
                                    NavSharpeRatio1Yr = (reader.IsDBNull(reader.GetOrdinal("NavSharpeRatio1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavSharpeRatio1Yr")),
                                    NaCorr1Yr = (reader.IsDBNull(reader.GetOrdinal("NaCorr1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NaCorr1Yr")),
                                    NavBeta1Yr = (reader.IsDBNull(reader.GetOrdinal("NavBeta1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavBeta1Yr")),
                                    NavAlpha1Yr = (reader.IsDBNull(reader.GetOrdinal("NavAlpha1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavAlpha1Yr")),
                                    PriceVol3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceVol3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceVol3Yr")),
                                    NavVol3Yr = (reader.IsDBNull(reader.GetOrdinal("NavVol3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavVol3Yr")),
                                    BenchmarkVol3Yr = (reader.IsDBNull(reader.GetOrdinal("BenchmarkVol3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("BenchmarkVol3Yr")),
                                    PriceSharpeRatio3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceSharpeRatio3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceSharpeRatio3Yr")),
                                    NavSharpeRatio3Yr = (reader.IsDBNull(reader.GetOrdinal("NavSharpeRatio3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavSharpeRatio3Yr")),
                                    NavCorr3Yr = (reader.IsDBNull(reader.GetOrdinal("NavCorr3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavCorr3Yr")),
                                    NavBeta3Yr = (reader.IsDBNull(reader.GetOrdinal("NavBeta3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavBeta3Yr")),
                                    NavAlpha3Yr = (reader.IsDBNull(reader.GetOrdinal("NavAlpha3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavAlpha3Yr")),
                                    NumHoldings = (reader.IsDBNull(reader.GetOrdinal("NumHoldings"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumHoldings")),
                                    LargestHoldingPct = (reader.IsDBNull(reader.GetOrdinal("LargestHoldingPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("LargestHoldingPct")),
                                    Top10HoldingsPct = (reader.IsDBNull(reader.GetOrdinal("Top10HoldingsPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Top10HoldingsPct")),
                                    AssetExpListedEquitiesPct = (reader.IsDBNull(reader.GetOrdinal("AssetExpListedEquitiesPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpListedEquitiesPct")),
                                    AssetExpUnlistedEquitiesPct = (reader.IsDBNull(reader.GetOrdinal("AssetExpUnlistedEquitiesPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpUnlistedEquitiesPct")),
                                    AssetExpBondsPct = (reader.IsDBNull(reader.GetOrdinal("AssetExpBondsPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpBondsPct")),
                                    AssetExpPropertyPct = (reader.IsDBNull(reader.GetOrdinal("AssetExpPropertyPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpPropertyPct")),
                                    AssetExpHedgeFundsPct = (reader.IsDBNull(reader.GetOrdinal("AssetExpHedgeFundsPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpHedgeFundsPct")),
                                    FXHedge = reader["FXHedge"] as string,
                                    GeoExpUK = (reader.IsDBNull(reader.GetOrdinal("GeoExpUK"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpUK")),
                                    GeoExpEurope = (reader.IsDBNull(reader.GetOrdinal("GeoExpEurope"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpEurope")),
                                    GeoExpNorthAmerica = (reader.IsDBNull(reader.GetOrdinal("GeoExpNorthAmerica"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpNorthAmerica")),
                                    GeoExpJapan = (reader.IsDBNull(reader.GetOrdinal("GeoExpJapan"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpJapan")),
                                    GeoExpOtherAsiaPacific = (reader.IsDBNull(reader.GetOrdinal("GeoExpOtherAsiaPacific"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpOtherAsiaPacific")),
                                    GeoExpOtherEmg = (reader.IsDBNull(reader.GetOrdinal("GeoExpOtherEmg"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpOtherEmg")),
                                    GeoExpGlobal = (reader.IsDBNull(reader.GetOrdinal("GeoExpGlobal"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpGlobal")),
                                    PortfolioDt = (reader.IsDBNull(reader.GetOrdinal("PortfolioDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortfolioDt")),
                                    MgmtGroup = reader["MgmtGroup"] as string,
                                    InvestmentAdvisor = reader["InvestmentAdvisor"] as string,
                                    FundManager = reader["FundManager"] as string,
                                    ManagerStartDt = (reader.IsDBNull(reader.GetOrdinal("ManagerStartDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ManagerStartDt")),
                                    CoreInvestmentStrategy = reader["CoreInvestmentStrategy"] as string,
                                    BaseFee = reader["BaseFee"] as string,
                                    BaseFeeBasedOn = reader["BaseFeeBasedOn"] as string,
                                    PerfFeeOn = reader["PerfFeeOn"] as string,
                                    PerfFeeBasedOn = reader["PerfFeeBasedOn"] as string,
                                    PerfFeeTimePeriod = reader["PerfFeeTimePeriod"] as string,
                                    PriceHighLow = reader["PriceHighLow"] as string,
                                    ExpRatioExLev = (reader.IsDBNull(reader.GetOrdinal("ExpRatioExLev"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ExpRatioExLev")),
                                    ExpRatioExLevExPerfFee = (reader.IsDBNull(reader.GetOrdinal("ExpRatioExLevExPerfFee"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ExpRatioExLevExPerfFee")),
                                    BuybackPolicy = reader["BuybackPolicy"] as string,
                                    PDDiscountTarget = reader["PDDiscountTarget"] as string,
                                    BuybackPolicyDetails = reader["BuybackPolicyDetails"] as string,
                                    ContinuationVoteDate = reader["ContinuationVoteDate"] as string,
                                    ContinuationVoteDetails = reader["ContinuationVoteDetails"] as string,
                                    GeoAllocation = reader["GeoAllocation"] as string,
                                    AssetAllocation = reader["AssetAllocation"] as string,
                                    PriceVol5Yr = (reader.IsDBNull(reader.GetOrdinal("PriceVol5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceVol5Yr")),
                                    NavVol5Yr = (reader.IsDBNull(reader.GetOrdinal("NavVol5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavVol5Yr")),
                                    BenchmarkVol5Yr = (reader.IsDBNull(reader.GetOrdinal("BenchmarkVol5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("BenchmarkVol5Yr")),
                                    PriceSharpeRatio5Yr = (reader.IsDBNull(reader.GetOrdinal("PriceSharpeRatio5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceSharpeRatio5Yr")),
                                    NavSharpeRatio5Yr = (reader.IsDBNull(reader.GetOrdinal("NavSharpeRatio5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavSharpeRatio5Yr")),
                                    NavCorr5Yr = (reader.IsDBNull(reader.GetOrdinal("NavCorr5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavCorr5Yr")),
                                    NavBeta5Yr = (reader.IsDBNull(reader.GetOrdinal("NavBeta5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavBeta5Yr")),
                                    NavAlpha5Yr = (reader.IsDBNull(reader.GetOrdinal("NavAlpha5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavAlpha5Yr")),
                                    Broker = reader["Broker"] as string,
                                    KeyHoldings = reader["KeyHoldings"] as string,
                                    KeyHolders = reader["KeyHolders"] as string,
                                    Notes = reader["Notes"] as string,
                                    Buybacks = (reader.IsDBNull(reader.GetOrdinal("Buybacks"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Buybacks")),
                                    Tenders = (reader.IsDBNull(reader.GetOrdinal("Tenders"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Tenders")),
                                    GearingLimit = reader["GearingLimit"] as string,
                                    EffectiveGearing = (reader.IsDBNull(reader.GetOrdinal("EffectiveGearing"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("EffectiveGearing"))
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

        public IList<PeelHuntDetailTO> GetPeelHuntDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<PeelHuntDetailTO> list = new List<PeelHuntDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_ticker", ticker);
                        command.Parameters.AddWithValue("p_source", source);
                        command.Parameters.AddWithValue("p_startDate", startDate);
                        command.Parameters.AddWithValue("p_endDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PeelHuntDetailTO data = new PeelHuntDetailTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    BBGTicker = reader["BBGTicker"] as string,
                                    FundGroup = reader["FundGroup"] as string,
                                    FundName = reader["FundName"] as string,
                                    Currency = reader["Currency"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    Spread = (reader.IsDBNull(reader.GetOrdinal("Spread"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Spread")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("EstNav")),
                                    NavCumPar = (reader.IsDBNull(reader.GetOrdinal("NavCumPar"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavCumPar")),
                                    NavCumFair = (reader.IsDBNull(reader.GetOrdinal("NavCumFair"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavCumFair")),
                                    NavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    PDAvg12M = (reader.IsDBNull(reader.GetOrdinal("PDAvg12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDAvg12M")),
                                    PDZScore1Yr = (reader.IsDBNull(reader.GetOrdinal("PDZScore1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDZScore1Yr")),
                                    PDZScore3Yr = (reader.IsDBNull(reader.GetOrdinal("PDZScore3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDZScore3Yr")),
                                    NavChng = (reader.IsDBNull(reader.GetOrdinal("NavChng"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavChng")),
                                    MktCap = (reader.IsDBNull(reader.GetOrdinal("MktCap"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("MktCap")),
                                    GrossAssets = (reader.IsDBNull(reader.GetOrdinal("GrossAssets"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GrossAssets")),
                                    NetAssets = (reader.IsDBNull(reader.GetOrdinal("NetAssets"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NetAssets")),
                                    NetGearing = (reader.IsDBNull(reader.GetOrdinal("NetGearing"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NetGearing")),
                                    DvdLastDt = (reader.IsDBNull(reader.GetOrdinal("DvdLastDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdLastDt")),
                                    DvdFreq = reader["DvdFreq"] as string,
                                    GeoExpUK = (reader.IsDBNull(reader.GetOrdinal("GeoExpUK"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpUK")),
                                    GeoExpUS = (reader.IsDBNull(reader.GetOrdinal("GeoExpUS"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpUS")),
                                    GeoExpEurope = (reader.IsDBNull(reader.GetOrdinal("GeoExpEurope"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpEurope")),
                                    GeoExpAsia = (reader.IsDBNull(reader.GetOrdinal("GeoExpAsia"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpAsia")),
                                    GeoExpJapan = (reader.IsDBNull(reader.GetOrdinal("GeoExpJapan"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpJapan")),
                                    GeoExpEmgMarket = (reader.IsDBNull(reader.GetOrdinal("GeoExpEmgMarket"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpEmgMarket")),
                                    GeoExpLatinAmerica = (reader.IsDBNull(reader.GetOrdinal("GeoExpLatinAmerica"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpLatinAmerica")),
                                    GeoExpOther = (reader.IsDBNull(reader.GetOrdinal("GeoExpOther"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpOther")),
                                    SectorExpCyclical = (reader.IsDBNull(reader.GetOrdinal("SectorExpCyclical"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("SectorExpCyclical")),
                                    SectorExpDefensive = (reader.IsDBNull(reader.GetOrdinal("SectorExpDefensive"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("SectorExpDefensive")),
                                    SectorExpSensitive = (reader.IsDBNull(reader.GetOrdinal("SectorExpSensitive"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("SectorExpSensitive")),
                                    FixedIncome = (reader.IsDBNull(reader.GetOrdinal("FixedIncome"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("FixedIncome")),
                                    Domicile = reader["Domicile"] as string,
                                    BrandingName = reader["BrandingName"] as string,
                                    ManagerName = reader["ManagerName"] as string,
                                    Broker = reader["Broker"] as string,
                                    MgmtFee = (reader.IsDBNull(reader.GetOrdinal("MgmtFee"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("MgmtFee")),
                                    PerfFee = (reader.IsDBNull(reader.GetOrdinal("PerfFee"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PerfFee")),
                                    OCF = (reader.IsDBNull(reader.GetOrdinal("OCF"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("OCF")),
                                    OCFWPerfFee = (reader.IsDBNull(reader.GetOrdinal("OCFWPerfFee"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("OCFWPerfFee")),
                                    OCFExPerfFee = (reader.IsDBNull(reader.GetOrdinal("OCFExPerfFee"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("OCFExPerfFee")),
                                    Benchmark = reader["Benchmark"] as string,
                                    NumHoldings = (reader.IsDBNull(reader.GetOrdinal("NumHoldings"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumHoldings")),
                                    Top10HoldingsPct = (reader.IsDBNull(reader.GetOrdinal("Top10HoldingsPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Top10HoldingsPct")),
                                    TurnoverRatio = (reader.IsDBNull(reader.GetOrdinal("TurnoverRatio"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("TurnoverRatio")),
                                    CashPct = (reader.IsDBNull(reader.GetOrdinal("CashPct"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("CashPct")),
                                    SharesOut = (reader.IsDBNull(reader.GetOrdinal("SharesOut"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("SharesOut")),
                                    PriceRtn1M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1M")),
                                    PriceRtn3M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn3M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn3M")),
                                    PriceRtn6M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn6M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn6M")),
                                    PriceRtnYTD = (reader.IsDBNull(reader.GetOrdinal("PriceRtnYTD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtnYTD")),
                                    PriceRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1Yr")),
                                    PriceRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn3Yr")),
                                    PriceRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn5Yr")),
                                    NAVRtn1M = (reader.IsDBNull(reader.GetOrdinal("NAVRtn1M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn1M")),
                                    NAVRtn3M = (reader.IsDBNull(reader.GetOrdinal("NAVRtn3M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn3M")),
                                    NAVRtn6M = (reader.IsDBNull(reader.GetOrdinal("NAVRtn6M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn6M")),
                                    NAVRtnYTD = (reader.IsDBNull(reader.GetOrdinal("NAVRtnYTD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtnYTD")),
                                    NAVRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("NAVRtn1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn1Yr")),
                                    NAVRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("NAVRtn3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn3Yr")),
                                    NAVRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("NAVRtn5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn5Yr")),
                                    NAVAlpha3Yr = (reader.IsDBNull(reader.GetOrdinal("NAVAlpha3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVAlpha3Yr")),
                                    PriceAlpha3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceAlpha3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceAlpha3Yr")),
                                    NAVBeta3Yr = (reader.IsDBNull(reader.GetOrdinal("NAVBeta3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVBeta3Yr")),
                                    PriceBeta3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceBeta3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceBeta3Yr")),
                                    NAVCorr3Yr = (reader.IsDBNull(reader.GetOrdinal("NAVCorr3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVCorr3Yr")),
                                    PriceCorr3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceCorr3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceCorr3Yr")),
                                    NAVVol3Yr = (reader.IsDBNull(reader.GetOrdinal("NAVVol3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVVol3Yr")),
                                    PriceVol3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceVol3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceVol3Yr")),
                                    NAVVol = (reader.IsDBNull(reader.GetOrdinal("NAVVol"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVVol")),
                                    PriceVol = (reader.IsDBNull(reader.GetOrdinal("PriceVol"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceVol")),
                                    NAVPriceRtn1M = (reader.IsDBNull(reader.GetOrdinal("NAVPriceRtn1M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVPriceRtn1M")),
                                    NAVPriceRtn3M = (reader.IsDBNull(reader.GetOrdinal("NAVPriceRtn3M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVPriceRtn3M")),
                                    NAVPriceRtn6M = (reader.IsDBNull(reader.GetOrdinal("NAVPriceRtn6M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVPriceRtn6M")),
                                    NAVPriceRtnYTD = (reader.IsDBNull(reader.GetOrdinal("NAVPriceRtnYTD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVPriceRtnYTD")),
                                    NAVPriceRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("NAVPriceRtn1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVPriceRtn1Yr")),
                                    Style = reader["Style"] as string,
                                    NAVYield = (reader.IsDBNull(reader.GetOrdinal("NAVYield"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVYield")),
                                    IncOnlyYield = (reader.IsDBNull(reader.GetOrdinal("IncOnlyYield"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("IncOnlyYield")),
                                    PortDate = (reader.IsDBNull(reader.GetOrdinal("PortDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortDate")),
                                    AnnReportDate = (reader.IsDBNull(reader.GetOrdinal("AnnReportDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AnnReportDate"))
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

        public IList<JefferiesDetailTO> GetJefferiesDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<JefferiesDetailTO> list = new List<JefferiesDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_ticker", ticker);
                        command.Parameters.AddWithValue("p_source", source);
                        command.Parameters.AddWithValue("p_startDate", startDate);
                        command.Parameters.AddWithValue("p_endDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JefferiesDetailTO data = new JefferiesDetailTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    BBGTicker = reader["BBGTicker"] as string,
                                    FundName = reader["FundName"] as string,
                                    FundGroup = reader["FundGroup"] as string,
                                    Currency = reader["Currency"] as string,
                                    PotentialEventCatalyst = reader["PotentialEventCatalyst"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    LastNav = (reader.IsDBNull(reader.GetOrdinal("LastNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastNav")),
                                    NavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    PDAvg12M = (reader.IsDBNull(reader.GetOrdinal("PDAvg12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDAvg12M")),
                                    ZScore1Yr = (reader.IsDBNull(reader.GetOrdinal("ZScore1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ZScore1Yr")),
                                    MktCap = (reader.IsDBNull(reader.GetOrdinal("MktCap"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("MktCap")),
                                    NavCap = (reader.IsDBNull(reader.GetOrdinal("NavCap"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavCap")),
                                    DvdYield12M = (reader.IsDBNull(reader.GetOrdinal("DvdYield12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("DvdYield12M")),
                                    DvdGrowth12M = (reader.IsDBNull(reader.GetOrdinal("DvdGrowth12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("DvdGrowth12M")),
                                    DvdGrowth3Yr = (reader.IsDBNull(reader.GetOrdinal("DvdGrowth3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("DvdGrowth3Yr")),
                                    AvgVolume12M = (reader.IsDBNull(reader.GetOrdinal("AvgVolume12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AvgVolume12M")),
                                    GeoExpUK = (reader.IsDBNull(reader.GetOrdinal("GeoExpUK"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpUK")),
                                    GeoExpUSA = (reader.IsDBNull(reader.GetOrdinal("GeoExpUSA"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpUSA")),
                                    GeoExpAsia = (reader.IsDBNull(reader.GetOrdinal("GeoExpAsia"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpAsia")),
                                    GeoExpLatinAmerica = (reader.IsDBNull(reader.GetOrdinal("GeoExpLatinAmerica"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpLatinAmerica")),
                                    GeoExpEuro = (reader.IsDBNull(reader.GetOrdinal("GeoExpEuro"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpEuro")),
                                    SharpeRatio12M = (reader.IsDBNull(reader.GetOrdinal("SharpeRatio12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("SharpeRatio12M")),
                                    Beta12M = (reader.IsDBNull(reader.GetOrdinal("Beta12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Beta12M")),
                                    Alpha12M = (reader.IsDBNull(reader.GetOrdinal("Alpha12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Alpha12M")),
                                    NetGearing = (reader.IsDBNull(reader.GetOrdinal("NetGearing"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NetGearing")),
                                    AssetExpCash = (reader.IsDBNull(reader.GetOrdinal("AssetExpCash"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpCash")),
                                    AssetExpEquity = (reader.IsDBNull(reader.GetOrdinal("AssetExpEquity"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpEquity")),
                                    AssetExpBonds = (reader.IsDBNull(reader.GetOrdinal("AssetExpBonds"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AssetExpBonds")),
                                    ManagementFee = (reader.IsDBNull(reader.GetOrdinal("ManagementFee"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ManagementFee")),
                                    PerformanceFee = (reader.IsDBNull(reader.GetOrdinal("PerformanceFee"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PerformanceFee")),
                                    PerformanceFeeActualDate = (reader.IsDBNull(reader.GetOrdinal("PerformanceFeeActualDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PerformanceFeeActualDate")),
                                    PriceRtn6M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn6M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn6M")),
                                    PriceRtn6MCorrelation = (reader.IsDBNull(reader.GetOrdinal("PriceRtn6MCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn6MCorrelation")),
                                    PriceRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1Yr")),
                                    PriceRtn1YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1YrCorrelation")),
                                    PriceRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn3Yr")),
                                    PriceRtn3YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("PriceRtn3YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn3YrCorrelation")),
                                    PriceRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn5Yr")),
                                    PriceRtn5YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("PriceRtn5YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn5YrCorrelation")),
                                    PriceRtn10Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn10Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn10Yr")),
                                    PriceRtn10YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("PriceRtn10YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn10YrCorrelation")),
                                    NavRtn6M = (reader.IsDBNull(reader.GetOrdinal("NavRtn6M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn6M")),
                                    NavRtn6MCorrelation = (reader.IsDBNull(reader.GetOrdinal("NavRtn6MCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn6MCorrelation")),
                                    NavRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn1Yr")),
                                    NavRtn1YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("NavRtn1YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn1YrCorrelation")),
                                    NavRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn3Yr")),
                                    NavRtn3YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("NavRtn3YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn3YrCorrelation")),
                                    NavRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn5Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn5Yr")),
                                    NavRtn5YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("NavRtn5YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn5YrCorrelation")),
                                    NavRtn10Yr = (reader.IsDBNull(reader.GetOrdinal("NavRtn10Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn10Yr")),
                                    NavRtn10YrCorrelation = (reader.IsDBNull(reader.GetOrdinal("NavRtn10YrCorrelation"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavRtn10YrCorrelation")),
                                    Exchange = reader["Exchange"] as string,
                                    Domicile = reader["Domicile"] as string,
                                    FirmName = reader["FirmName"] as string,
                                    WindupProvisions = reader["WindupProvisions"] as string,
                                    WindupProvisionCode = reader["WindupProvisionCode"] as string,
                                    LifeTriggerEventDate = (reader.IsDBNull(reader.GetOrdinal("LifeTriggerEventDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LifeTriggerEventDate")),
                                    OtherEvents = reader["OtherEvents"] as string,
                                    OtherEventsCode = reader["OtherEventsCode"] as string,
                                    EventTriggerDate = (reader.IsDBNull(reader.GetOrdinal("EventTriggerDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EventTriggerDate")),
                                    TargetPDNormalMarketConditions = (reader.IsDBNull(reader.GetOrdinal("TargetPDNormalMarketConditions"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("TargetPDNormalMarketConditions"))
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

        public IList<JPMDetailTO> GetJPMDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<JPMDetailTO> list = new List<JPMDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_ticker", ticker);
                        command.Parameters.AddWithValue("p_source", source);
                        command.Parameters.AddWithValue("p_startDate", startDate);
                        command.Parameters.AddWithValue("p_endDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMDetailTO data = new JPMDetailTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundGroup = reader["FundGroup"] as string,
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    IndexName = reader["IndexName"] as string,
                                    JPMRecc = reader["JPMRecc"] as string,
                                    Currency = reader["Currency"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    NAVCumFair = (reader.IsDBNull(reader.GetOrdinal("NAVCumFair"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVCumFair")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    SizeMC = (reader.IsDBNull(reader.GetOrdinal("SizeMC"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("SizeMC")),
                                    SizeNA = (reader.IsDBNull(reader.GetOrdinal("SizeNA"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("SizeNA")),
                                    SizeTA = (reader.IsDBNull(reader.GetOrdinal("SizeTA"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("SizeTA")),
                                    NetGearing = (reader.IsDBNull(reader.GetOrdinal("NetGearing"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NetGearing")),
                                    DiscountCumFairZScore = (reader.IsDBNull(reader.GetOrdinal("DiscountCumFairZScore"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("DiscountCumFairZScore")),
                                    DiscountCumFairAvg = (reader.IsDBNull(reader.GetOrdinal("DiscountCumFairAvg"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("DiscountCumFairAvg")),
                                    DiscountCumFairHigh = (reader.IsDBNull(reader.GetOrdinal("DiscountCumFairHigh"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("DiscountCumFairHigh")),
                                    DiscountCumFairLow = (reader.IsDBNull(reader.GetOrdinal("DiscountCumFairLow"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("DiscountCumFairLow")),
                                    NDY = (reader.IsDBNull(reader.GetOrdinal("NDY"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NDY")),
                                    NAVRtn1M = (reader.IsDBNull(reader.GetOrdinal("NAVRtn1M"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn1M")),
                                    NAVRtn6M = (reader.IsDBNull(reader.GetOrdinal("NAVRtn6M"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn6M")),
                                    NAVRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("NAVRtn1Yr"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn1Yr")),
                                    NAVRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("NAVRtn3Yr"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn3Yr")),
                                    NAVRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("NAVRtn5Yr"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVRtn5Yr")),
                                    NAVRtnRank1M = reader["NAVRtnRank1M"] as string,
                                    NAVRtnRank6M = reader["NAVRtnRank6M"] as string,
                                    NAVRtnRank1Yr = reader["NAVRtnRank1Yr"] as string,
                                    NAVRtnRank3Yr = reader["NAVRtnRank3Yr"] as string,
                                    NAVRtnRank5Yr = reader["NAVRtnRank5Yr"] as string,
                                    PriceRtn1M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1M"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1M")),
                                    PriceRtn6M = (reader.IsDBNull(reader.GetOrdinal("PriceRtn6M"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn6M")),
                                    PriceRtn1Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn1Yr"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn1Yr")),
                                    PriceRtn3Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn3Yr"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn3Yr")),
                                    PriceRtn5Yr = (reader.IsDBNull(reader.GetOrdinal("PriceRtn5Yr"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceRtn5Yr")),
                                    PriceRtnRank1M = reader["PriceRtnRank1M"] as string,
                                    PriceRtnRank6M = reader["PriceRtnRank6M"] as string,
                                    PriceRtnRank1Yr = reader["PriceRtnRank1Yr"] as string,
                                    PriceRtnRank3Yr = reader["PriceRtnRank3Yr"] as string,
                                    PriceRtnRank5Yr = reader["PriceRtnRank5Yr"] as string,
                                    GeoEXPEurope = (reader.IsDBNull(reader.GetOrdinal("GeoEXPEurope"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoEXPEurope")),
                                    GeoEXPUK = (reader.IsDBNull(reader.GetOrdinal("GeoEXPUK"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoEXPUK")),
                                    GeoExpNorthAmerica = (reader.IsDBNull(reader.GetOrdinal("GeoExpNorthAmerica"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpNorthAmerica")),
                                    GeoExpAsia = (reader.IsDBNull(reader.GetOrdinal("GeoExpAsia"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoExpAsia")),
                                    GeoEXPJapan = (reader.IsDBNull(reader.GetOrdinal("GeoEXPJapan"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoEXPJapan")),
                                    GeoEXPOther = (reader.IsDBNull(reader.GetOrdinal("GeoEXPOther"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("GeoEXPOther")),
                                    GepEXPDate = (reader.IsDBNull(reader.GetOrdinal("GepEXPDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("GepEXPDate")),
                                    Benchmark = reader["Benchmark"] as string,
                                    IPODate = (reader.IsDBNull(reader.GetOrdinal("IPODate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("IPODate")),
                                    Dom = reader["Dom"] as string,
                                    ManagementGroup = reader["ManagementGroup"] as string,
                                    LeadManager = reader["LeadManager"] as string,
                                    AppointedOn = (reader.IsDBNull(reader.GetOrdinal("AppointedOn"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AppointedOn")),
                                    KIDRIY1Yr = (reader.IsDBNull(reader.GetOrdinal("KIDRIY1Yr"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("KIDRIY1Yr")),
                                    OC = (reader.IsDBNull(reader.GetOrdinal("OC"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("OC")),
                                    MgtFee = reader["MgtFee"] as string,
                                    PerformanceFee = reader["PerformanceFee"] as string,
                                    BenchmarkFee = reader["BenchmarkFee"] as string,
                                    PerformanceFeePeriod = reader["PerformanceFeePeriod"] as string,
                                    Hurdle = reader["Hurdle"] as string,
                                    HWM = reader["HWM"] as string,
                                    PerformanceFeeCap = reader["PerformanceFeeCap"] as string,
                                    CarryForwardInExcessOfCap = reader["CarryForwardInExcessOfCap"] as string,
                                    EarnsWhenNAVDecline = reader["EarnsWhenNAVDecline"] as string,
                                    MgtFeeChargeToCapital = (reader.IsDBNull(reader.GetOrdinal("MgtFeeChargeToCapital"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("MgtFeeChargeToCapital")),
                                    NoOfHoldings = (reader.IsDBNull(reader.GetOrdinal("NoOfHoldings"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NoOfHoldings")),
                                    NoOfHoldingsDate = (reader.IsDBNull(reader.GetOrdinal("NoOfHoldingsDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NoOfHoldingsDate")),
                                    PTO = (reader.IsDBNull(reader.GetOrdinal("PTO"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("PTO")),
                                    IncomeDataNDY = (reader.IsDBNull(reader.GetOrdinal("IncomeDataNDY"))) ? (decimal?)null : reader.GetDecimal(reader.GetOrdinal("IncomeDataNDY")),
                                    IncomeDataDvndFreq = reader["IncomeDataDvndFreq"] as string,
                                    IncomeDataDvndGrowth5Y = (reader.IsDBNull(reader.GetOrdinal("IncomeDataDvndGrowth5Y"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("IncomeDataDvndGrowth5Y")),
                                    PermissionToDistributeFrmCptl = reader["PermissionToDistributeFrmCptl"] as string,
                                    DCMTerms = reader["DCMTerms"] as string,
                                    ContinuationVote = reader["ContinuationVote"] as string

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

        public IList<JefferiesDetailTO> GetJefferiesData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<JefferiesDetailTO> list = new List<JefferiesDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundDataHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Source", source);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JefferiesDetailTO data = new JefferiesDetailTO
                                {
                                    BBGTicker = reader["BBGTicker"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Currency = reader["Currency"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    LastNav = (reader.IsDBNull(reader.GetOrdinal("LastNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastNav")),
                                    NavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    PDAvg12M = (reader.IsDBNull(reader.GetOrdinal("PDAvg12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDAvg12M")),
                                    ZScore1Yr = (reader.IsDBNull(reader.GetOrdinal("ZScore1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ZScore1Yr")),
                                };

                                data.AsOfDateAsString = DateUtils.ConvertDate(data.AsOfDate, "yyyy-MM-dd");
                                data.NavDateAsString = DateUtils.ConvertDate(data.NavDate, "yyyy-MM-dd");

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

        public IList<NumisDetailTO> GetNumisData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<NumisDetailTO> list = new List<NumisDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundDataHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Source", source);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                NumisDetailTO data = new NumisDetailTO
                                {
                                    BBGTicker = reader["BBGTicker"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Currency = reader["Currency"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    LastNav = (reader.IsDBNull(reader.GetOrdinal("LastNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastNav")),
                                    NavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    NavFreq = reader["NavFreq"] as string,
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    PDAvg12M = (reader.IsDBNull(reader.GetOrdinal("PDAvg12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDAvg12M")),
                                    PDHigh12M = (reader.IsDBNull(reader.GetOrdinal("PDHigh12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDHigh12M")),
                                    PDLow12M = (reader.IsDBNull(reader.GetOrdinal("PDLow12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDLow12M")),
                                    ZScore13Wk = (reader.IsDBNull(reader.GetOrdinal("ZScore13Wk"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ZScore13Wk")),
                                    ZScore1Yr = (reader.IsDBNull(reader.GetOrdinal("ZScore1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ZScore1Yr")),
                                };

                                data.AsOfDateAsString = DateUtils.ConvertDate(data.AsOfDate, "yyyy-MM-dd");
                                data.NavDateAsString = DateUtils.ConvertDate(data.NavDate, "yyyy-MM-dd");

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

        public IList<PeelHuntDetailTO> GetPeelHuntData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<PeelHuntDetailTO> list = new List<PeelHuntDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundDataHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Source", source);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PeelHuntDetailTO data = new PeelHuntDetailTO
                                {
                                    BBGTicker = reader["BBGTicker"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    Currency = reader["Currency"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("EstNav")),
                                    NavCumPar = (reader.IsDBNull(reader.GetOrdinal("NavCumPar"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavCumPar")),
                                    NavCumFair = (reader.IsDBNull(reader.GetOrdinal("NavCumFair"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavCumFair")),
                                    NavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    PDAvg12M = (reader.IsDBNull(reader.GetOrdinal("PDAvg12M"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDAvg12M")),
                                    PDZScore1Yr = (reader.IsDBNull(reader.GetOrdinal("PDZScore1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDZScore1Yr")),
                                    PDZScore3Yr = (reader.IsDBNull(reader.GetOrdinal("PDZScore3Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PDZScore3Yr")),
                                    NavChng = (reader.IsDBNull(reader.GetOrdinal("NavChng"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NavChng")),
                                };

                                data.FileDateAsString = DateUtils.ConvertDate(data.FileDate, "yyyy-MM-dd");
                                data.NavDateAsString = DateUtils.ConvertDate(data.NavDate, "yyyy-MM-dd");

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

        public IList<BBGFundDetailTO> GetBloombergData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<BBGFundDetailTO> list = new List<BBGFundDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundDataHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Source", source);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BBGFundDetailTO data = new BBGFundDetailTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    EffDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    PriceVWAP = (reader.IsDBNull(reader.GetOrdinal("PriceVWAP"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PriceVWAP")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Nav")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    Vol = (reader.IsDBNull(reader.GetOrdinal("Vol"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Vol")),
                                };

                                data.EffDateAsString = DateUtils.ConvertDate(data.EffDate, "yyyy-MM-dd");

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

        public IList<JPMDetailTO> GetJPMData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            IList<JPMDetailTO> list = new List<JPMDetailTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetUKFundDataHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Source", source);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMDetailTO data = new JPMDetailTO
                                {
                                    BBGTicker = reader["BBGTicker"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    Currency = reader["Currency"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("Price")),
                                    NAVCumFair = (reader.IsDBNull(reader.GetOrdinal("NAVCumFair"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("NAVCumFair")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("PD")),
                                    DiscountCumFairZScore = (reader.IsDBNull(reader.GetOrdinal("ZScore1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("ZScore1Yr")),
                                    DiscountCumFairAvg = (reader.IsDBNull(reader.GetOrdinal("AvgPD1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("AvgPD1Yr")),
                                    DiscountCumFairHigh = (reader.IsDBNull(reader.GetOrdinal("HighPD1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("HighPD1Yr")),
                                    DiscountCumFairLow = (reader.IsDBNull(reader.GetOrdinal("LowPD1Yr"))) ? (Decimal?)null : reader.GetDecimal(reader.GetOrdinal("LowPD1Yr")),
                                };

                                data.FileDateAsString = DateUtils.ConvertDate(data.FileDate, "yyyy-MM-dd");

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

        private const string GetUKFundHistoryQuery = "almitasc_ACTradingBBGLink.spGetUKFundData";
        private const string GetUKFundDataHistoryQuery = "almitasc_ACTradingBBGLink.spGetUKFundDataHistory";
    }
}
