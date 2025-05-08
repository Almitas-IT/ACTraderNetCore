using aACTrader.DAO.Interface;
using aCommons;
using aCommons.DTO;
using aCommons.DTO.BAML;
using aCommons.DTO.BMO;
using aCommons.DTO.EDF;
using aCommons.DTO.Fidelity;
using aCommons.DTO.IB;
using aCommons.DTO.JPM;
using aCommons.DTO.MorganStanley;
using aCommons.DTO.TD;
using aCommons.Fund;
using aCommons.Trading;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using FidelityTaxLotDetailsTO = aCommons.DTO.Fidelity.TaxLotDetailsTO;
using TaxLotDetailsTO = aCommons.DTO.TD.TaxLotDetailsTO;

namespace aACTrader.DAO.Repository
{
    public class BrokerDataDao : IBrokerDataDao
    {
        private readonly ILogger<BrokerDataDao> _logger;

        public BrokerDataDao(ILogger<BrokerDataDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing BrokerDataDao...");
        }

        public IList<SecurityMarginDetail> GetJPMSecurityMarginDetails(string fundName, string ticker, DateTime startDate, DateTime endDate)
        {
            IList<SecurityMarginDetail> list = new List<SecurityMarginDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityMarginHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", "JPM");
                        command.Parameters.AddWithValue("p_FundName", fundName);
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMarginDetail data = new SecurityMarginDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    SecType = reader["SecType"] as string,
                                    Currency = reader["Currency"] as string,

                                    Position = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),

                                    //Amounts
                                    MarginRequirement = (reader.IsDBNull(reader.GetOrdinal("MarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirement")),
                                    BaselineAmt = (reader.IsDBNull(reader.GetOrdinal("BaselineEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaselineEquity")),
                                    LiquidityAmt = (reader.IsDBNull(reader.GetOrdinal("AddOnLiquidity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnLiquidity")),
                                    AddOnEquityOwnership = (reader.IsDBNull(reader.GetOrdinal("AddOnEquityOwnership"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnEquityOwnership")),
                                    AddOnNetIndustryConcentration = (reader.IsDBNull(reader.GetOrdinal("AddOnNetIndustryConcentration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnNetIndustryConcentration")),
                                    AddOnNetIssuerConcentration = (reader.IsDBNull(reader.GetOrdinal("AddOnNetIssuerConcentration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnNetIssuerConcentration")),
                                    NonMarginableSecurity = (reader.IsDBNull(reader.GetOrdinal("NonMarginableSecurity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonMarginableSecurity")),
                                    OtherAmt = (reader.IsDBNull(reader.GetOrdinal("Other"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Other")),

                                    //Rates
                                    MarginRate = (reader.IsDBNull(reader.GetOrdinal("MarginRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRate")),
                                    BaselineRate = (reader.IsDBNull(reader.GetOrdinal("BaselineEquityRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaselineEquityRate")),
                                    LiquidityRate = (reader.IsDBNull(reader.GetOrdinal("AddOnLiquidityRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnLiquidityRate")),
                                    AddOnEquityOwnershipRate = (reader.IsDBNull(reader.GetOrdinal("AddOnEquityOwnershipRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnEquityOwnershipRate")),
                                    AddOnNetIndustryConcentrationRate = (reader.IsDBNull(reader.GetOrdinal("AddOnNetIndustryConcentrationRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnNetIndustryConcentrationRate")),
                                    AddOnNetIssuerConcentrationRate = (reader.IsDBNull(reader.GetOrdinal("AddOnNetIssuerConcentrationRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnNetIssuerConcentrationRate")),
                                    NonMarginableSecurityRate = (reader.IsDBNull(reader.GetOrdinal("NonMarginableSecurityRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonMarginableSecurityRate")),
                                    OtherRate = (reader.IsDBNull(reader.GetOrdinal("OtherRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OtherRate")),
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

        public IList<SecurityMargin> GetFidelitySecurityMarginDetails(string fundName, string ticker, DateTime startDate, DateTime endDate)
        {
            IList<SecurityMargin> list = new List<SecurityMargin>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityMarginHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", "Fidelity");
                        command.Parameters.AddWithValue("p_FundName", fundName);
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMargin data = new SecurityMargin
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    IssuerDesc = reader["IssuerDesc"] as string,
                                    IndustryGroup = reader["IndustryGroup"] as string,
                                    IssuerSpecificRisk = (reader.IsDBNull(reader.GetOrdinal("IssuerSpecificRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IssuerSpecificRisk")),
                                    SystematicMove = (reader.IsDBNull(reader.GetOrdinal("SystematicMove"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SystematicMove")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("ExpAdjGMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjGMV")),
                                    DeltaAdjQty = (reader.IsDBNull(reader.GetOrdinal("DeltaAdjQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DeltaAdjQty")),
                                    ClosingPrice = (reader.IsDBNull(reader.GetOrdinal("ClosingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosingPrice")),
                                    GrossMV = (reader.IsDBNull(reader.GetOrdinal("ExpAdjGMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjGMV")),
                                    NetMV = (reader.IsDBNull(reader.GetOrdinal("ExpAdjNMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjNMV")),
                                    MarginPct = (reader.IsDBNull(reader.GetOrdinal("MarginRequirementPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirementPct")),
                                    MarginRequirement = (reader.IsDBNull(reader.GetOrdinal("MarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirement"))
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

        public IList<SecurityBorrowRate> GetJPMSecurityBorrowRates(string ticker)
        {
            IList<SecurityBorrowRate> list = new List<SecurityBorrowRate>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetJPMSecurityBorrowRatesQuery;
                    if (!string.IsNullOrEmpty(ticker))
                        sql += " and Ticker = '" + ticker + "'";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityBorrowRate data = new SecurityBorrowRate
                                {
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Ticker = reader["Ticker"] as string,
                                    SecurityDesc = reader["SecurityDesc"] as string,
                                    SecurityType = reader["SecurityType"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Country = reader["Country"] as string,
                                    RegulationShortSecurity = reader["RegulationShoSecurity"] as string,
                                    IndicativeAvailQty = (reader.IsDBNull(reader.GetOrdinal("IndicativeAvailQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndicativeAvailQty")),
                                    IndicativeBorrowRate = (reader.IsDBNull(reader.GetOrdinal("IndicativeBorrowRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndicativeBorrowRate")),
                                    IndicativeBorrowFee = (reader.IsDBNull(reader.GetOrdinal("IndicativeBorrowFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndicativeBorrowFee")),
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

        public IList<SCMarginDetailsTO> GetSCMarginDetails(string StartDate, string EndDate)
        {
            IList<SCMarginDetailsTO> detailslist = new List<SCMarginDetailsTO>();
            string sql = GetSCMarginDetailQuery;
            sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SCMarginDetailsTO listitem = new SCMarginDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    InstrName = reader["InstrName"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Side = reader["Side"] as string,
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SwapId")),
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    RptCurr = reader["RptCurr"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Position")),
                                    AvgCost = (reader.IsDBNull(reader.GetOrdinal("AvgCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgCost")),
                                    NotionalCost = (reader.IsDBNull(reader.GetOrdinal("NotionalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCost")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    NotionalAmt = (reader.IsDBNull(reader.GetOrdinal("NotionalAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalAmt")),
                                    IASwapCurrAmt = (reader.IsDBNull(reader.GetOrdinal("IASwapCurrAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IASwapCurrAmt")),
                                    IAPct = (reader.IsDBNull(reader.GetOrdinal("IAPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAPct")),
                                    IAReportCurrAmt = (reader.IsDBNull(reader.GetOrdinal("IAReportCurrAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAReportCurrAmt")),
                                    VariationSwapCurrAmt = (reader.IsDBNull(reader.GetOrdinal("VariationSwapCurrAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VariationSwapCurrAmt")),
                                    VariationRptCurrAmt = (reader.IsDBNull(reader.GetOrdinal("VariationRptCurrAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VariationRptCurrAmt")),
                                };
                                detailslist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing SCMargin details ");
                throw;
            }
            return detailslist;
        }

        public IList<SCMarginSummaryTO> GetSCMarginSummary(string StartDate, string EndDate)
        {
            IList<SCMarginSummaryTO> marginlist = new List<SCMarginSummaryTO>();
            try
            {
                string sql = GetSCMarginSummaryQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SCMarginSummaryTO listitem = new SCMarginSummaryTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    Curr = reader["Curr"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    TotalIA = (reader.IsDBNull(reader.GetOrdinal("TotalIA"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalIA")),
                                    Variation = (reader.IsDBNull(reader.GetOrdinal("Variation"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Variation")),
                                    TotalCashHeld = (reader.IsDBNull(reader.GetOrdinal("TotalCashHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCashHeld")),
                                    ExcessDeficit = (reader.IsDBNull(reader.GetOrdinal("ExcessDeficit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessDeficit")),
                                    UnSettledCash = (reader.IsDBNull(reader.GetOrdinal("UnSettledCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnSettledCash")),
                                };
                                marginlist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Scotia Margin Summary ");
                throw;
            }
            return marginlist;
        }


        public IList<SCMTMDetailsTO> GetSCMTMDetails(string StartDate, string EndDate)
        {
            IList<SCMTMDetailsTO> fulllist = new List<SCMTMDetailsTO>();
            try
            {
                string sql = GetSCMTMDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SCMTMDetailsTO listitem = new SCMTMDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    UnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("UnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingPrice")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    SwapPrice = (reader.IsDBNull(reader.GetOrdinal("SwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPrice")),
                                    Side = reader["Side"] as string,
                                    TradeDatedPos = (reader.IsDBNull(reader.GetOrdinal("TradeDatedPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDatedPos")),
                                    ValueDatedPos = (reader.IsDBNull(reader.GetOrdinal("ValueDatedPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ValueDatedPos")),
                                    AvgCost = (reader.IsDBNull(reader.GetOrdinal("AvgCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgCost")),
                                    NotionalCost = (reader.IsDBNull(reader.GetOrdinal("NotionalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCost")),
                                    Notional = (reader.IsDBNull(reader.GetOrdinal("Notional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Notional")),
                                    MTMAmt = (reader.IsDBNull(reader.GetOrdinal("MTMAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTMAmt")),
                                    AccruedComm = (reader.IsDBNull(reader.GetOrdinal("AccruedComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedComm")),
                                    AccruedTradingPL = (reader.IsDBNull(reader.GetOrdinal("AccruedTradingPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedTradingPL")),
                                    AccruedDividends = (reader.IsDBNull(reader.GetOrdinal("AccruedDividends"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedDividends")),
                                    AccruedNotionalFinancing = (reader.IsDBNull(reader.GetOrdinal("AccruedNotionalFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedNotionalFinancing")),
                                    AccuredBorrowCost = (reader.IsDBNull(reader.GetOrdinal("AccuredBorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccuredBorrowCost")),
                                    AccruedCashFinancing = (reader.IsDBNull(reader.GetOrdinal("AccruedCashFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedCashFinancing")),
                                    AccruedReset = (reader.IsDBNull(reader.GetOrdinal("AccruedReset"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedReset")),
                                    TotalPL = (reader.IsDBNull(reader.GetOrdinal("TotalPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalPL")),
                                    AvgSpread = (reader.IsDBNull(reader.GetOrdinal("AvgSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgSpread")),
                                    AvgBorrowCost = (reader.IsDBNull(reader.GetOrdinal("AvgBorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgBorrowCost")),
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    CountryCd = reader["CountryCd"] as string,
                                    PrimaryInstrIdentifier = reader["PrimaryInstrIdentifier"] as string,
                                    InstrType = reader["InstrType"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Scotia Margin Details ");
                throw;
            }
            return fulllist;
        }

        public IList<SCSecurityResetTO> GetSCSecurityReset(string StartDate, string EndDate)
        {
            IList<SCSecurityResetTO> fulllist = new List<SCSecurityResetTO>();
            try
            {
                string sql = GetSCSecurityResetQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SCSecurityResetTO listitem = new SCSecurityResetTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    ResetQty = (reader.IsDBNull(reader.GetOrdinal("ResetQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResetQty")),
                                    CostPrice = (reader.IsDBNull(reader.GetOrdinal("CostPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostPrice")),
                                    ResetPrice = (reader.IsDBNull(reader.GetOrdinal("ResetPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResetPrice")),
                                    Side = reader["Side"] as string,
                                    ResetFxRate = (reader.IsDBNull(reader.GetOrdinal("ResetFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResetFxRate")),
                                    ResetUnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("ResetUnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResetUnderlyingPrice")),
                                    ResetCash = (reader.IsDBNull(reader.GetOrdinal("ResetCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResetCash")),
                                    Comm = (reader.IsDBNull(reader.GetOrdinal("Comm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Comm")),
                                    UnwindCash = (reader.IsDBNull(reader.GetOrdinal("UnwindCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnwindCash")),
                                    Dvds = (reader.IsDBNull(reader.GetOrdinal("Dvds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dvds")),
                                    NotionalFinancing = (reader.IsDBNull(reader.GetOrdinal("NotionalFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalFinancing")),
                                    BorrowCost = (reader.IsDBNull(reader.GetOrdinal("BorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BorrowCost")),
                                    CashFinancing = (reader.IsDBNull(reader.GetOrdinal("CashFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashFinancing")),
                                    TotalResetPayment = (reader.IsDBNull(reader.GetOrdinal("TotalResetPayment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalResetPayment")),
                                    AvgSpread = (reader.IsDBNull(reader.GetOrdinal("AvgSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgSpread")),
                                    AvgBorrowCost = (reader.IsDBNull(reader.GetOrdinal("AvgBorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgBorrowCost")),
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    CountryCd = reader["CountryCd"] as string,
                                    PrimaryInstrIdentifier = reader["PrimaryInstrIdentifier"] as string,
                                    InstrClass = reader["InstrClass"] as string,
                                    AIPerUnit = (reader.IsDBNull(reader.GetOrdinal("AIPerUnit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AIPerUnit")),
                                    PaymentCurr = reader["PaymentCurr"] as string,
                                    PaymentDate = reader.IsDBNull(reader.GetOrdinal("PaymentDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PaymentDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing SCMargin summary ");
                throw;
            }
            return fulllist;
        }

        public IList<SCSecurityResetDetailsTO> GetSCSecurityResetDetails(string StartDate, string EndDate)
        {
            IList<SCSecurityResetDetailsTO> fulllist = new List<SCSecurityResetDetailsTO>();
            try
            {
                string sql = GetSCSecurityResetDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SCSecurityResetDetailsTO listitem = new SCSecurityResetDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    Side = reader["Side"] as string,
                                    CashflowType = reader["CashflowType"] as string,
                                    CashflowSubType = reader["CashflowSubType"] as string,
                                    PaymentCurr = reader["PaymentCurr"] as string,
                                    PaymentType = reader["PaymentType"] as string,
                                    CashflowAmt = (reader.IsDBNull(reader.GetOrdinal("CashflowAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashflowAmt")),
                                    EffectiveStartDate = reader.IsDBNull(reader.GetOrdinal("EffectiveStartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveStartDate")),
                                    EffectiveEndDate = reader.IsDBNull(reader.GetOrdinal("EffectiveEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveEndDate")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    FinancingStartDate = reader.IsDBNull(reader.GetOrdinal("FinancingStartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinancingStartDate")),
                                    FinancingEndDate = reader.IsDBNull(reader.GetOrdinal("FinancingEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinancingEndDate")),
                                    ConfirmDate = reader.IsDBNull(reader.GetOrdinal("ConfirmDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ConfirmDate")),
                                    Days = (reader.IsDBNull(reader.GetOrdinal("Days"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Days")),
                                    Notional = (reader.IsDBNull(reader.GetOrdinal("Notional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Notional")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    Spread = (reader.IsDBNull(reader.GetOrdinal("Spread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Spread")),
                                    BorrowCost = (reader.IsDBNull(reader.GetOrdinal("BorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BorrowCost")),
                                    InterestRate = (reader.IsDBNull(reader.GetOrdinal("InterestRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InterestRate")),
                                    DistributionPct = (reader.IsDBNull(reader.GetOrdinal("DistributionPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DistributionPct")),
                                    DistributionRate = (reader.IsDBNull(reader.GetOrdinal("DistributionRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DistributionRate")),
                                    WithholdingTax = (reader.IsDBNull(reader.GetOrdinal("WithholdingTax"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WithholdingTax")),
                                    Cost = (reader.IsDBNull(reader.GetOrdinal("Cost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    UnderlyingAmt = (reader.IsDBNull(reader.GetOrdinal("UnderlyingAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingAmt")),
                                    UnderlyingCurr = reader["UnderlyingCurr"] as string,
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    CashflowId = (reader.IsDBNull(reader.GetOrdinal("CashflowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CashflowId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    CountryCd = reader["CountryCd"] as string,
                                    InstrType = reader["InstrType"] as string,
                                    PaymentDate = reader.IsDBNull(reader.GetOrdinal("PaymentDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PaymentDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing SCMargin summary ");
                throw;
            }
            return fulllist;
        }

        public IList<SCTradeDetailsTO> GetSCTradeDetails(string StartDate, string EndDate)
        {
            IList<SCTradeDetailsTO> fulllist = new List<SCTradeDetailsTO>();
            try
            {
                string sql = GetSCTradeDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SCTradeDetailsTO listitem = new SCTradeDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    TradeId = (reader.IsDBNull(reader.GetOrdinal("TradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeId")),
                                    TradeType = reader["TradeType"] as string,
                                    AllocationId = (reader.IsDBNull(reader.GetOrdinal("AllocationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AllocationId")),
                                    TradeStatus = reader["TradeStatus"] as string,
                                    TradeDate = reader.IsDBNull(reader.GetOrdinal("TradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    UnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("UnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingPrice")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    SwapPrice = (reader.IsDBNull(reader.GetOrdinal("SwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPrice")),
                                    Spread = (reader.IsDBNull(reader.GetOrdinal("Spread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Spread")),
                                    BorrowCost = (reader.IsDBNull(reader.GetOrdinal("BorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BorrowCost")),
                                    DvdRate = (reader.IsDBNull(reader.GetOrdinal("DvdRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdRate")),
                                    CommType = reader["CommType"] as string,
                                    Comm = (reader.IsDBNull(reader.GetOrdinal("Comm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Comm")),
                                    Side = reader["Side"] as string,
                                    UnderlyingPriceClean = (reader.IsDBNull(reader.GetOrdinal("UnderlyingPriceClean"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingPriceClean")),
                                    SwapPriceClean = (reader.IsDBNull(reader.GetOrdinal("SwapPriceClean"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPriceClean")),
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    CountryCd = reader["CountryCd"] as string,
                                    PrimaryInstrIdentifier = reader["PrimaryInstrIdentifier"] as string,
                                    InstrClass = reader["InstrClass"] as string,
                                    TradeDateMaturity = reader.IsDBNull(reader.GetOrdinal("TradeDateMaturity")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDateMaturity")),
                                    SettleDateMaturity = reader.IsDBNull(reader.GetOrdinal("SettleDateMaturity")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDateMaturity")),
                                    NotionalNet = (reader.IsDBNull(reader.GetOrdinal("NotionalNet"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalNet")),
                                    NotionalGross = (reader.IsDBNull(reader.GetOrdinal("NotionalGross"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalGross")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    TradeLogId = (reader.IsDBNull(reader.GetOrdinal("TradeLogId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeLogId")),
                                    ReportingFirm = reader["ReportingFirm"] as string,
                                    ReportingFirmLEI = reader["ReportingFirmLEI"] as string,
                                    CounterpartyLEI = reader["CounterpartyLEI"] as string,
                                    TradeTime = reader["TradeTime"] as string,
                                    SwapCFICode = reader["SwapCFICode"] as string,
                                    SwapISIN = reader["SwapISIN"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing SCMargin summary ");
                throw;
            }
            return fulllist;
        }

        public IDictionary<string, BrokerCommission> GetBrokerCommissionRates()
        {
            IDictionary<string, BrokerCommission> dict = new Dictionary<string, BrokerCommission>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetBrokerCommissionRateQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BrokerCommission data = new BrokerCommission();
                                data.Src = reader["Src"] as string;
                                data.BkrName = reader["BrokerName"] as string;
                                data.FuncName = reader["CommissionFunction"] as string;
                                data.CommRt = (reader.IsDBNull(reader.GetOrdinal("CommissionRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommissionRate"));
                                data.Notes = reader["Notes"] as string;

                                dict.Add(data.BkrName, data);
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

        public IList<BrokerTO> GetExecutedBrokers()
        {
            IList<BrokerTO> list = new List<BrokerTO>();

            try
            {
                string sql = GetExecutedBrokersQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BrokerTO data = new BrokerTO();
                                data.label = reader["BrokerName"] as string;
                                data.value = reader["BrokerName"] as string;
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

        #region Fidelity Reports section.
        public IList<FundDetailsTO> GetFDFundDetails(string StartDate, string EndDate)
        {
            IList<FundDetailsTO> fulllist = new List<FundDetailsTO>();
            try
            {
                string sql = GetFDFundDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDetailsTO listitem = new FundDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    CashBal = (reader.IsDBNull(reader.GetOrdinal("CashBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashBal")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    NetEquity = (reader.IsDBNull(reader.GetOrdinal("NetEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEquity")),
                                    GrossMV = (reader.IsDBNull(reader.GetOrdinal("GrossMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMV")),
                                    NetMV = (reader.IsDBNull(reader.GetOrdinal("NetMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetMV")),
                                    LiquidityDays = (reader.IsDBNull(reader.GetOrdinal("LiquidityDays"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LiquidityDays")),
                                    Leverage = (reader.IsDBNull(reader.GetOrdinal("Leverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leverage")),
                                    NetEquityPct = (reader.IsDBNull(reader.GetOrdinal("NetEquityPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEquityPct")),
                                    NetEquityReq = (reader.IsDBNull(reader.GetOrdinal("NetEquityReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEquityReq")),
                                    SurplusDeficit = (reader.IsDBNull(reader.GetOrdinal("SurplusDeficit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SurplusDeficit")),
                                    MinEquity = (reader.IsDBNull(reader.GetOrdinal("MinEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MinEquity")),
                                    MinEquityCall = (reader.IsDBNull(reader.GetOrdinal("MinEquityCall"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MinEquityCall")),
                                    HouseReqPct = (reader.IsDBNull(reader.GetOrdinal("HouseReqPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseReqPct")),
                                    HouseReqDelAdj = (reader.IsDBNull(reader.GetOrdinal("HouseReqDelAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseReqDelAdj")),
                                    TimsReqPct = (reader.IsDBNull(reader.GetOrdinal("TimsReqPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TimsReqPct")),
                                    TimsReq = (reader.IsDBNull(reader.GetOrdinal("TimsReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TimsReq")),
                                    FirmMinReqPct = (reader.IsDBNull(reader.GetOrdinal("FirmMinReqPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FirmMinReqPct")),
                                    FirmMinReq = (reader.IsDBNull(reader.GetOrdinal("FirmMinReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FirmMinReq")),
                                    MarginEqOpMV = (reader.IsDBNull(reader.GetOrdinal("MarginEqOpMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginEqOpMV")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing SCMargin summary ");
                throw;
            }
            return fulllist;
        }

        public IList<FundCashBalanceTO> GetFDFundCashBalance(string StartDate, string EndDate)
        {
            IList<FundCashBalanceTO> fulllist = new List<FundCashBalanceTO>();
            try
            {
                string sql = GetFDFundCashBalanceQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCashBalanceTO listitem = new FundCashBalanceTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    ValueDate = reader.IsDBNull(reader.GetOrdinal("ValueDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ValueDate")),
                                    Curr = reader["Curr"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    AssetType = reader["AssetType"] as string,
                                    TradeDateCash = (reader.IsDBNull(reader.GetOrdinal("TradeDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCash")),
                                    TradeDateCashUSD = (reader.IsDBNull(reader.GetOrdinal("TradeDateCashUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCashUSD")),
                                    SettleDateCash = (reader.IsDBNull(reader.GetOrdinal("SettleDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateCash")),
                                    SettleDateCashUSD = (reader.IsDBNull(reader.GetOrdinal("SettleDateCashUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateCashUSD")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing SCMargin summary ");
                throw;
            }
            return fulllist;
        }

        public IList<CorpActionDetailsTO> GetFDCorpActionDetails(string StartDate, string EndDate)
        {
            IList<CorpActionDetailsTO> fulllist = new List<CorpActionDetailsTO>();
            try
            {
                string sql = GetFDCorpActionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CorpActionDetailsTO listitem = new CorpActionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    CorpActId = (reader.IsDBNull(reader.GetOrdinal("CorpActId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorpActId")),
                                    CorpActType = reader["CorpActType"] as string,
                                    CorpActTypeDesc = reader["CorpActTypeDesc"] as string,
                                    CorpActCount = (reader.IsDBNull(reader.GetOrdinal("CorpActCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorpActCount")),
                                    SecDesc = reader["SecDesc"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ExDate = reader.IsDBNull(reader.GetOrdinal("ExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    PayDate = reader.IsDBNull(reader.GetOrdinal("PayDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                    RecordDate = reader.IsDBNull(reader.GetOrdinal("RecordDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RecordDate")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    BaseAmt = (reader.IsDBNull(reader.GetOrdinal("BaseAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseAmt")),
                                    LocalAmt = (reader.IsDBNull(reader.GetOrdinal("LocalAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalAmt")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FDCorpAction Details ");
                throw;
            }
            return fulllist;
        }

        public IList<IssuerMarginDetailsTO> GetFDIssuerMarginDetails(string StartDate, string EndDate)
        {
            IList<IssuerMarginDetailsTO> fulllist = new List<IssuerMarginDetailsTO>();
            try
            {
                string sql = GetFDIssuerMarginDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                IssuerMarginDetailsTO listitem = new IssuerMarginDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Ticker = reader["Ticker"] as string,
                                    IssuerDesc = reader["IssuerDesc"] as string,
                                    DeltaAdjQty = reader.IsDBNull(reader.GetOrdinal("DeltaAdjQty")) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DeltaAdjQty")),
                                    ExpAdjGMV = (reader.IsDBNull(reader.GetOrdinal("ExpAdjGMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjGMV")),
                                    ExpAdjNMV = (reader.IsDBNull(reader.GetOrdinal("ExpAdjNMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjNMV")),
                                    ClsPrice = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    LiquidityDays = (reader.IsDBNull(reader.GetOrdinal("LiquidityDays"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LiquidityDays")),
                                    StdDev2Up = (reader.IsDBNull(reader.GetOrdinal("StdDev2Up"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StdDev2Up")),
                                    StdDev2Down = (reader.IsDBNull(reader.GetOrdinal("StdDev2Down"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StdDev2Down")),
                                    ExtremeUp = (reader.IsDBNull(reader.GetOrdinal("ExtremeUp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExtremeUp")),
                                    ExtremeDown = (reader.IsDBNull(reader.GetOrdinal("ExtremeDown"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExtremeDown")),
                                    SystematicMove = (reader.IsDBNull(reader.GetOrdinal("SystematicMove"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SystematicMove")),
                                    IssuerSpecificRisk = (reader.IsDBNull(reader.GetOrdinal("IssuerSpecificRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IssuerSpecificRisk")),
                                    PortRiskCoefficient = (reader.IsDBNull(reader.GetOrdinal("PortRiskCoefficient"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortRiskCoefficient")),
                                    IdiosyncraticRisk = (reader.IsDBNull(reader.GetOrdinal("IdiosyncraticRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IdiosyncraticRisk")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD IssuerMargin Details");
                throw;
            }
            return fulllist;
        }

        public IList<MarginAttributionDetailsTO> GetFDMarginAttributionDetails(string StartDate, string EndDate)
        {
            IList<MarginAttributionDetailsTO> fulllist = new List<MarginAttributionDetailsTO>();
            try
            {
                string sql = GetFDMarginAttributionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MarginAttributionDetailsTO listitem = new MarginAttributionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    PMLongMktExp = (reader.IsDBNull(reader.GetOrdinal("PMLongMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PMLongMktExp")),
                                    PMShortMktExp = (reader.IsDBNull(reader.GetOrdinal("PMShortMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PMShortMktExp")),
                                    PMLongShortRatio = (reader.IsDBNull(reader.GetOrdinal("PMLongShortRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PMLongShortRatio")),
                                    PMGrossMktExp = (reader.IsDBNull(reader.GetOrdinal("PMGrossMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PMGrossMktExp")),
                                    PMNetMktExp = (reader.IsDBNull(reader.GetOrdinal("PMNetMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PMNetMktExp")),
                                    CashAmt = (reader.IsDBNull(reader.GetOrdinal("CashAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashAmt")),
                                    LongMktExp = (reader.IsDBNull(reader.GetOrdinal("LongMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMktExp")),
                                    ShortMktExp = (reader.IsDBNull(reader.GetOrdinal("ShortMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMktExp")),
                                    LongShortRatio = (reader.IsDBNull(reader.GetOrdinal("LongShortRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongShortRatio")),
                                    GrossMktExp = (reader.IsDBNull(reader.GetOrdinal("GrossMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMktExp")),
                                    NetMktExp = (reader.IsDBNull(reader.GetOrdinal("NetMktExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetMktExp")),
                                    IdiosyncraticRisk = (reader.IsDBNull(reader.GetOrdinal("IdiosyncraticRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IdiosyncraticRisk")),
                                    SystematicMove = (reader.IsDBNull(reader.GetOrdinal("SystematicMove"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SystematicMove")),
                                    IndustryGroupRisk = (reader.IsDBNull(reader.GetOrdinal("IndustryGroupRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndustryGroupRisk")),
                                    IntraIndustryRisk = (reader.IsDBNull(reader.GetOrdinal("IntraIndustryRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntraIndustryRisk")),
                                    MinimumReq = (reader.IsDBNull(reader.GetOrdinal("MinimumReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MinimumReq")),
                                    HouseReq = (reader.IsDBNull(reader.GetOrdinal("HouseReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseReq")),
                                    LineItemReq = (reader.IsDBNull(reader.GetOrdinal("LineItemReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LineItemReq")),
                                    NonPortMarginReq = (reader.IsDBNull(reader.GetOrdinal("NonPortMarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonPortMarginReq")),
                                    NetEquityReq = (reader.IsDBNull(reader.GetOrdinal("NetEquityReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEquityReq")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing SCMargin summary ");
                throw;
            }

            return fulllist;
        }

        public IList<MarginReqDetailsTO> GetFDMarginReqDetails(string StartDate, string EndDate)
        {
            IList<MarginReqDetailsTO> fulllist = new List<MarginReqDetailsTO>();
            try
            {
                string sql = GetFDMarginReqDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MarginReqDetailsTO listitem = new MarginReqDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RICCode = reader["RICCode"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Category = reader["Category"] as string,
                                    IssuerDesc = reader["IssuerDesc"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    GrossMV = (reader.IsDBNull(reader.GetOrdinal("GrossMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMV")),
                                    NetMV = (reader.IsDBNull(reader.GetOrdinal("NetMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetMV")),
                                    MarginReq = (reader.IsDBNull(reader.GetOrdinal("MarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginReq")),
                                    MarginReqPct = (reader.IsDBNull(reader.GetOrdinal("MarginReqPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginReqPct")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD Margin Req Details");
                throw;
            }
            return fulllist;
        }

        public IList<SecurityMarginDetailsTO> GetFDSecurityMarginDetails(string StartDate, string EndDate)
        {
            IList<SecurityMarginDetailsTO> fulllist = new List<SecurityMarginDetailsTO>();
            try
            {
                string sql = GetFDSecurityMarginDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMarginDetailsTO listitem = new SecurityMarginDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RICCode = reader["RICCode"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Isin = reader["Isin"] as string,
                                    IssuerDesc = reader["IssuerDesc"] as string,
                                    IndustryGroup = reader["IndustryGroup"] as string,
                                    DeltaAdjQty = (reader.IsDBNull(reader.GetOrdinal("DeltaAdjQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DeltaAdjQty")),
                                    ExpAdjGMV = (reader.IsDBNull(reader.GetOrdinal("ExpAdjGMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjGMV")),
                                    ExpAdjNMV = (reader.IsDBNull(reader.GetOrdinal("ExpAdjNMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjNMV")),
                                    ClsPrice = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    MarginReq = (reader.IsDBNull(reader.GetOrdinal("MarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginReq")),
                                    MarginReqPct = (reader.IsDBNull(reader.GetOrdinal("MarginReqPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginReqPct")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FDSecurity Margin details ");
                throw;
            }
            return fulllist;
        }

        public IList<FidelityTaxLotDetailsTO> GetFDTaxLotDetails(string StartDate, string EndDate)
        {
            IList<FidelityTaxLotDetailsTO> fulllist = new List<FidelityTaxLotDetailsTO>();
            try
            {
                string sql = GetFDTaxLotDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FidelityTaxLotDetailsTO listitem = new FidelityTaxLotDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AcctType = reader["AcctType"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Isin = reader["Isin"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    LocalCurr = reader["LocalCurr"] as string,
                                    BaseCurr = reader["BaseCurr"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    LotId = reader.IsDBNull(reader.GetOrdinal("LotId")) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LotId")),
                                    DateAcquired = reader.IsDBNull(reader.GetOrdinal("DateAcquired")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DateAcquired")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    ClsPrice = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    UnitCostLocal = (reader.IsDBNull(reader.GetOrdinal("UnitCostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCostLocal")),
                                    Cost = (reader.IsDBNull(reader.GetOrdinal("Cost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost")),
                                    CostLocal = (reader.IsDBNull(reader.GetOrdinal("CostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostLocal")),
                                    ClsPriceLocal = (reader.IsDBNull(reader.GetOrdinal("ClsPriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPriceLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    AccruedDividends = (reader.IsDBNull(reader.GetOrdinal("AccruedDividends"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedDividends")),
                                    AccruedDividendsLocal = (reader.IsDBNull(reader.GetOrdinal("AccruedDividendsLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedDividendsLocal")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    AILocal = (reader.IsDBNull(reader.GetOrdinal("AILocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AILocal")),
                                    UnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                    UnrealizedPLLocal = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLocal")),
                                    LongTermDays = (reader.IsDBNull(reader.GetOrdinal("LongTermDays"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LongTermDays")),
                                    SecType = reader["SecType"] as string,
                                    IssueCntry = reader["IssueCntry"] as string,
                                    Sector = reader["Sector"] as string,
                                    OptionSymbol = reader["OptionSymbol"] as string,
                                    OptionType = reader["OptionType"] as string,
                                    OptionExpDate = reader.IsDBNull(reader.GetOrdinal("OptionExpDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OptionExpDate")),
                                    ValueDate = reader.IsDBNull(reader.GetOrdinal("ValueDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ValueDate")),
                                    OptionStrikePrice = (reader.IsDBNull(reader.GetOrdinal("OptionStrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OptionStrikePrice")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FDSecurity Margin details ");
                throw;
            }
            return fulllist;
        }
        #endregion

        public IList<PositionTypeDetailsTO> GetFDPositionTypeDetails(string StartDate, string EndDate)
        {
            IList<PositionTypeDetailsTO> fulllist = new List<PositionTypeDetailsTO>();
            try
            {
                string sql = GetFDPositionTypeDetailsQuery;
                sql += " where RptDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by RptDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PositionTypeDetailsTO listitem = new PositionTypeDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    RptDate = reader.IsDBNull(reader.GetOrdinal("RptDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RptDate")),
                                    AcctType = reader["AcctType"] as string,
                                    SecType = reader["SecType"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Curr = reader["Curr"] as string,
                                    LongShortInd = reader["LongShortInd"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    UnitCostLocal = (reader.IsDBNull(reader.GetOrdinal("UnitCostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCostLocal")),
                                    ClsPrice = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    ClsPriceLocal = (reader.IsDBNull(reader.GetOrdinal("ClsPriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPriceLocal")),
                                    Cost = (reader.IsDBNull(reader.GetOrdinal("Cost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost")),
                                    CostLocal = (reader.IsDBNull(reader.GetOrdinal("CostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    AILocal = (reader.IsDBNull(reader.GetOrdinal("AILocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AILocal")),
                                    UnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                    UnrealizedPLLocal = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLocal")),
                                    NetValuePct = (reader.IsDBNull(reader.GetOrdinal("NetValuePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetValuePct")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FDSecurity Margin details ");
                throw;
            }
            return fulllist;
        }

        public IList<ValuationDetailsTO> GetFDValuationDetails(string StartDate, string EndDate)
        {
            IList<ValuationDetailsTO> fulllist = new List<ValuationDetailsTO>();
            try
            {
                string sql = GetFDValuationDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ValuationDetailsTO listitem = new ValuationDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    CashAmt = (reader.IsDBNull(reader.GetOrdinal("CashAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashAmt")),
                                    CashPct = (reader.IsDBNull(reader.GetOrdinal("CashPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashPct")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    LongMVPct = (reader.IsDBNull(reader.GetOrdinal("LongMVPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMVPct")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    ShortMVPct = (reader.IsDBNull(reader.GetOrdinal("ShortMVPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVPct")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    AIPct = (reader.IsDBNull(reader.GetOrdinal("AIPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AIPct")),
                                    TotalValue = (reader.IsDBNull(reader.GetOrdinal("TotalValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalValue")),
                                    GrossMV = (reader.IsDBNull(reader.GetOrdinal("GrossMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMV")),
                                    GrossMVPct = (reader.IsDBNull(reader.GetOrdinal("GrossMVPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMVPct")),
                                    NetMV = (reader.IsDBNull(reader.GetOrdinal("NetMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetMV")),
                                    NetMVPct = (reader.IsDBNull(reader.GetOrdinal("NetMVPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetMVPct")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD Valuation details ");
                throw;
            }
            return fulllist;
        }

        public IList<IntEarningDetailsTO> FDIntEarningDetails(string StartDate, string EndDate)
        {
            IList<IntEarningDetailsTO> fulllist = new List<IntEarningDetailsTO>();
            try
            {
                string sql = GetFDIntEarningsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                IntEarningDetailsTO listitem = new IntEarningDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    SettleCurr = reader["SettleCurr"] as string,
                                    IndexName = reader["IndexName"] as string,
                                    CashBalLocal = (reader.IsDBNull(reader.GetOrdinal("CashBalLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashBalLocal")),
                                    ShortMVLocal = (reader.IsDBNull(reader.GetOrdinal("ShortMVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVLocal")),
                                    NetBalLocal = (reader.IsDBNull(reader.GetOrdinal("NetBalLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetBalLocal")),
                                    IndexRate = (reader.IsDBNull(reader.GetOrdinal("IndexRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndexRate")),
                                    CreditRate = (reader.IsDBNull(reader.GetOrdinal("CreditRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditRate")),
                                    DebitRate = (reader.IsDBNull(reader.GetOrdinal("DebitRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitRate")),
                                    IntEarningsLocal = (reader.IsDBNull(reader.GetOrdinal("IntEarningsLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntEarningsLocal")),
                                    IntExpenseLocal = (reader.IsDBNull(reader.GetOrdinal("IntExpenseLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntExpenseLocal")),
                                    NetEarningsLocal = (reader.IsDBNull(reader.GetOrdinal("NetEarningsLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEarningsLocal")),
                                    NetEarnings = (reader.IsDBNull(reader.GetOrdinal("NetEarnings"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEarnings")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD Int Earning details ");
                throw;
            }
            return fulllist;
        }

        public IList<PositionDetailsTO> FDPositionDetails(string StartDate, string EndDate)
        {
            IList<PositionDetailsTO> fulllist = new List<PositionDetailsTO>();
            try
            {
                string sql = GetFDPositionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PositionDetailsTO listitem = new PositionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AcctType = reader["AcctType"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Isin = reader["Isin"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecType = reader["SecType"] as string,
                                    QtyTraded = (reader.IsDBNull(reader.GetOrdinal("QtyTraded"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QtyTraded")),
                                    QtySettled = (reader.IsDBNull(reader.GetOrdinal("QtySettled"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QtySettled")),
                                    AssetLiability = reader["AssetLiability"] as string,
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    ClsPrice = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    ClsPriceLocal = (reader.IsDBNull(reader.GetOrdinal("ClsPriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPriceLocal")),
                                    BaseCurr = reader["BaseCurr"] as string,
                                    LocalCurr = reader["LocalCurr"] as string,
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    AILocal = (reader.IsDBNull(reader.GetOrdinal("AILocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AILocal")),
                                    IssueCntry = reader["IssueCntry"] as string,
                                    OptionSymbol = reader["OptionSymbol"] as string,
                                    OptionExpDate = reader.IsDBNull(reader.GetOrdinal("OptionExpDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OptionExpDate")),
                                    OptionType = reader["OptionType"] as string,
                                    OptionStrikePrice = (reader.IsDBNull(reader.GetOrdinal("OptionStrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OptionStrikePrice")),
                                    MaturityDate = reader.IsDBNull(reader.GetOrdinal("MaturityDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturityDate")),
                                    CpnRate = (reader.IsDBNull(reader.GetOrdinal("CpnRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CpnRate")),
                                    MarketPlace = reader["MarketPlace"] as string,
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    UnitCostLocal = (reader.IsDBNull(reader.GetOrdinal("UnitCostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCostLocal")),
                                    Cost = (reader.IsDBNull(reader.GetOrdinal("Cost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost")),
                                    CostLocal = (reader.IsDBNull(reader.GetOrdinal("CostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostLocal")),
                                    SpecialTicker = reader["SpecialTicker"] as string,
                                    ValueDate = reader.IsDBNull(reader.GetOrdinal("ValueDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ValueDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD Position details ");
                throw;
            }
            return fulllist;
        }

        public IList<TransactionDetailsTO> FDTransactionDetails(string StartDate, string EndDate)
        {
            IList<TransactionDetailsTO> fulllist = new List<TransactionDetailsTO>();
            try
            {
                string sql = GetFDTransactionDetailsQuery;
                sql += " where RptDate between '" + StartDate + "' and '" + EndDate + "' order by RptDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TransactionDetailsTO listitem = new TransactionDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    AcctType = reader["AcctType"] as string,
                                    RptDate = reader.IsDBNull(reader.GetOrdinal("RptDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RptDate")),
                                    TransDate = reader.IsDBNull(reader.GetOrdinal("TransDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TransDate")),
                                    SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    TransDesc = reader["TransDesc"] as string,
                                    Cancelled = reader["Cancelled"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecType = reader["SecType"] as string,
                                    BaseCurr = reader["BaseCurr"] as string,
                                    LocalCurr = reader["LocalCurr"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    BaseUnitPrice = (reader.IsDBNull(reader.GetOrdinal("BaseUnitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseUnitPrice")),
                                    LocalUnitPrice = (reader.IsDBNull(reader.GetOrdinal("LocalUnitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalUnitPrice")),
                                    BaseGrossAmt = (reader.IsDBNull(reader.GetOrdinal("BaseGrossAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseGrossAmt")),
                                    LocalGrossAmt = (reader.IsDBNull(reader.GetOrdinal("LocalGrossAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalGrossAmt")),
                                    BaseAI = (reader.IsDBNull(reader.GetOrdinal("BaseAI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseAI")),
                                    LocalAI = (reader.IsDBNull(reader.GetOrdinal("LocalAI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalAI")),
                                    BaseComm = (reader.IsDBNull(reader.GetOrdinal("BaseComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseComm")),
                                    LocalComm = (reader.IsDBNull(reader.GetOrdinal("LocalComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalComm")),
                                    RegulatoryFee = (reader.IsDBNull(reader.GetOrdinal("RegulatoryFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RegulatoryFee")),
                                    BaseServiceFee = (reader.IsDBNull(reader.GetOrdinal("BaseServiceFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseServiceFee")),
                                    LocalServiceFee = (reader.IsDBNull(reader.GetOrdinal("LocalServiceFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalServiceFee")),
                                    BaseNetAmt = (reader.IsDBNull(reader.GetOrdinal("BaseNetAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseNetAmt")),
                                    LocalNetAmt = (reader.IsDBNull(reader.GetOrdinal("LocalNetAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalNetAmt")),
                                    ExecBroker = reader["ExecBroker"] as string,
                                    OptionSymbol = reader["OptionSymbol"] as string,
                                    OptionExpDate = reader["OptionExpDate"] as string,
                                    OptionType = reader["OptionType"] as string,
                                    OptionStrikePrice = reader["OptionStrikePrice"] as string,
                                    MarketPlace = reader["MarketPlace"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD Transaction details ");
                throw;
            }
            return fulllist;
        }

        public IList<SecurityRebateRateTO> FDSecurityRebateRate(string StartDate, string EndDate)
        {
            IList<SecurityRebateRateTO> fulllist = new List<SecurityRebateRateTO>();
            try
            {
                string sql = GetFDSecurityRebateRateQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityRebateRateTO listitem = new SecurityRebateRateTO
                                {
                                    FundName = reader["FundName"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SecDesc = reader["SecDesc"] as string,
                                    AccrualDate = reader.IsDBNull(reader.GetOrdinal("AccrualDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AccrualDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Curr = reader["Curr"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    IndexRate = (reader.IsDBNull(reader.GetOrdinal("IndexRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndexRate")),
                                    RebateRate = (reader.IsDBNull(reader.GetOrdinal("RebateRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebateRate")),
                                    LocalClsMark = (reader.IsDBNull(reader.GetOrdinal("LocalClsMark"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalClsMark")),
                                    LocalMV = (reader.IsDBNull(reader.GetOrdinal("LocalMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalMV")),
                                    LocalRebate = (reader.IsDBNull(reader.GetOrdinal("LocalRebate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalRebate")),
                                    BaseRebate = (reader.IsDBNull(reader.GetOrdinal("BaseRebate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseRebate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD Security Rebate Rate details ");
                throw;
            }
            return fulllist;
        }

        public IList<FDRealizedGLDetailsTO> FDRealizedGLDetails(string StartDate, string EndDate)
        {
            IList<FDRealizedGLDetailsTO> fulllist = new List<FDRealizedGLDetailsTO>();
            try
            {
                string sql = GetFDRealizedGLDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FDRealizedGLDetailsTO listitem = new FDRealizedGLDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Portfolio = reader["Portfolio"] as string,
                                    AcctNum = reader["AcctNum"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    DisposalMethod = reader["DisposalMethod"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Curr = reader["Curr"] as string,
                                    CurrDesc = reader["CurrDesc"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Symbol = reader["Symbol"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Quantity = (reader.IsDBNull(reader.GetOrdinal("Quantity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Quantity")),
                                    AcquiredDate = (reader.IsDBNull(reader.GetOrdinal("AcquiredDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AcquiredDate")),
                                    DisposedDate = (reader.IsDBNull(reader.GetOrdinal("DisposedDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DisposedDate")),
                                    BaseProceeds = (reader.IsDBNull(reader.GetOrdinal("BaseProceeds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseProceeds")),
                                    LocalProceeds = (reader.IsDBNull(reader.GetOrdinal("LocalProceeds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalProceeds")),
                                    BaseCost = (reader.IsDBNull(reader.GetOrdinal("BaseCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseCost")),
                                    LocalCost = (reader.IsDBNull(reader.GetOrdinal("LocalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalCost")),
                                    BaseShortTermGL = (reader.IsDBNull(reader.GetOrdinal("BaseShortTermGL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseShortTermGL")),
                                    BaseLongTermGL = (reader.IsDBNull(reader.GetOrdinal("BaseLongTermGL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseLongTermGL")),
                                    LocalLongTermGl = (reader.IsDBNull(reader.GetOrdinal("LocalLongTermGl"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalLongTermGl")),
                                    LocalShortTermGL = (reader.IsDBNull(reader.GetOrdinal("LocalShortTermGL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalShortTermGL")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing FD Realized GL details ");
                throw;
            }
            return fulllist;
        }

        #region TD Reports section
        public IList<CollateralDetailsTO> GetTDColleteralDetails(string StartDate, string EndDate)
        {
            IList<CollateralDetailsTO> fulllist = new List<CollateralDetailsTO>();
            try
            {
                string sql = GetTDCollateralDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CollateralDetailsTO listitem = new CollateralDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    TradeId = (reader.IsDBNull(reader.GetOrdinal("TradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeId")),
                                    Side = reader["Side"] as string,
                                    Curr = reader["Curr"] as string,
                                    Curr1 = reader["Curr1"] as string,
                                    Curr2 = reader["Curr2"] as string,
                                    ProductType = reader["ProductType"] as string,
                                    Notional1 = (reader.IsDBNull(reader.GetOrdinal("Notional1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Notional1")),
                                    Notional2 = (reader.IsDBNull(reader.GetOrdinal("Notional2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Notional2")),
                                    PrinIndependentAmt = (reader.IsDBNull(reader.GetOrdinal("PrinIndependentAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrinIndependentAmt")),
                                    CptyIndependentAmt = (reader.IsDBNull(reader.GetOrdinal("CptyIndependentAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CptyIndependentAmt")),
                                    TradeDate = reader.IsDBNull(reader.GetOrdinal("TradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    MaturityDate = reader.IsDBNull(reader.GetOrdinal("MaturityDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturityDate")),
                                    PutCall = reader["PutCall"] as string,
                                    Underlier = reader["Underlier"] as string,
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),
                                    NativeMV = (reader.IsDBNull(reader.GetOrdinal("NativeMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NativeMV")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    NativeCurr = reader["NativeCurr"] as string,
                                    MVCurr = reader["MVCurr"] as string,
                                    SrcSystem = reader["SrcSystem"] as string,
                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing TD Collateral details. ");
                throw;
            }

            return fulllist;

        }
        public IList<TaxLotDetailsTO> GetTDTaxLotDetails(string StartDate, string EndDate)
        {
            IList<TaxLotDetailsTO> fulllist = new List<TaxLotDetailsTO>();
            try
            {
                string sql = GetTDTaxLotDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TaxLotDetailsTO listitem = new TaxLotDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    DealId = (reader.IsDBNull(reader.GetOrdinal("DealId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DealId")),
                                    RefPortolio = reader["RefPortolio"] as string,
                                    InstrType = reader["InstrType"] as string,
                                    TradeDate = reader.IsDBNull(reader.GetOrdinal("TradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    ExpiryDate = reader.IsDBNull(reader.GetOrdinal("ExpiryDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpiryDate")),
                                    UnderlyingTicker = reader["UnderlyingTicker"] as string,
                                    NotionalCurr = reader["NotionalCurr"] as string,
                                    NotionalAmt = (reader.IsDBNull(reader.GetOrdinal("NotionalAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalAmt")),
                                    NumContracts = (reader.IsDBNull(reader.GetOrdinal("NumContracts"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumContracts")),
                                    CurPVLeg1 = (reader.IsDBNull(reader.GetOrdinal("CurPVLeg1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurPVLeg1")),
                                    CurPVLeg2 = (reader.IsDBNull(reader.GetOrdinal("CurPVLeg2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurPVLeg2")),
                                    PVParentCurr = reader["PVParentCurr"] as string,
                                    MTM = (reader.IsDBNull(reader.GetOrdinal("MTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTM")),
                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing TD Taxlot details. ");
                throw;
            }

            return fulllist;

        }

        #endregion

        #region IB Reports section
        public IList<IBPositionDetailsTO> GetIBPositionDetails(string StartDate, string EndDate)
        {
            IList<IBPositionDetailsTO> fulllist = new List<IBPositionDetailsTO>();
            try
            {
                string sql = GetIBPositionDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                IBPositionDetailsTO listitem = new IBPositionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    PrimaryCurr = reader["PrimaryCurr"] as string,
                                    AssetClass = reader["AssetClass"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecId = reader["SecId"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    SecIdType = reader["SecIdType"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    UnderlyingSymbol = reader["UnderlyingSymbol"] as string,
                                    Issuer = reader["Issuer"] as string,
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier")),
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),
                                    ExpiryDate = reader.IsDBNull(reader.GetOrdinal("ExpiryDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpiryDate")),
                                    PutCallInd = reader["PutCallInd"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    OpenPrice = (reader.IsDBNull(reader.GetOrdinal("OpenPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenPrice")),
                                    CostBasisPrice = (reader.IsDBNull(reader.GetOrdinal("CostBasisPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostBasisPrice")),
                                    CostBasisAmt = (reader.IsDBNull(reader.GetOrdinal("CostBasisAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostBasisAmt")),
                                    UnrealizedPnL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPnL")),
                                    Side = reader["Side"] as string,
                                    Code = reader["Code"] as string,
                                    OpenDateTime = reader.IsDBNull(reader.GetOrdinal("OpenDateTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OpenDateTime")),
                                    HoldingPeriodDateTime = reader.IsDBNull(reader.GetOrdinal("HoldingPeriodDateTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("HoldingPeriodDateTime")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing IB Position details. ");
                throw;
            }

            return fulllist;

        }
        public IList<IBTaxLotDetailsTO> GetIBTaxLotDetails(string StartDate, string EndDate)
        {
            IList<IBTaxLotDetailsTO> fulllist = new List<IBTaxLotDetailsTO>();
            try
            {
                string sql = GetIBTaxLotDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                IBTaxLotDetailsTO listitem = new IBTaxLotDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    PrimaryCurr = reader["PrimaryCurr"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    AssetClass = reader["AssetClass"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecId = reader["SecId"] as string,
                                    SecIdType = reader["SecIdType"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    UnderlyingSymbol = reader["UnderlyingSymbol"] as string,
                                    Issuer = reader["Issuer"] as string,
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier")),
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),
                                    ExpiryDate = reader.IsDBNull(reader.GetOrdinal("ExpiryDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpiryDate")),
                                    PutCallInd = reader["PutCallInd"] as string,
                                    PrinAdjustFactor = (reader.IsDBNull(reader.GetOrdinal("PrinAdjustFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrinAdjustFactor")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    OpenPrice = (reader.IsDBNull(reader.GetOrdinal("OpenPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenPrice")),
                                    CostBasisPrice = (reader.IsDBNull(reader.GetOrdinal("CostBasisPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostBasisPrice")),
                                    CostBasisAmt = (reader.IsDBNull(reader.GetOrdinal("CostBasisAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostBasisAmt")),
                                    UnrealizedPnL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPnL")),
                                    Side = reader["Side"] as string,
                                    OpenDateTime = reader.IsDBNull(reader.GetOrdinal("OpenDateTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OpenDateTime")),
                                    HoldingPeriodDateTime = reader.IsDBNull(reader.GetOrdinal("HoldingPeriodDateTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("HoldingPeriodDateTime")),
                                    Code = reader["Code"] as string,
                                    OriginatingOrderId = reader.IsDBNull(reader.GetOrdinal("OriginatingOrderId")) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OriginatingOrderId")),
                                    OriginatingTransId = reader.IsDBNull(reader.GetOrdinal("OriginatingTransId")) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OriginatingTransId")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),

                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing IB Taxlot details. ");
                throw;
            }

            return fulllist;

        }
        #endregion

        public IList<BDCGlobalResearchTO> GetBDCGlobalResearchDetails(string StartDate, string EndDate, string Ticker)
        {
            IList<BDCGlobalResearchTO> fulllist = new List<BDCGlobalResearchTO>();
            try
            {
                string sql = GetBDCGlobalResearchQuery;
                sql += " where NavDate between '" + StartDate + "' and '" + EndDate + "'";
                if (!string.IsNullOrEmpty(Ticker))
                    sql += "and Ticker= '" + Ticker.Replace(" US", "") + "'";

                sql += " order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BDCGlobalResearchTO listitem = new BDCGlobalResearchTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AssetGroup = reader["AssetGroup"] as string,
                                    MarketPrice = (reader.IsDBNull(reader.GetOrdinal("MarketPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPrice")),
                                    NAV = (reader.IsDBNull(reader.GetOrdinal("NAV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NAV")),
                                    NAVDate = (reader.IsDBNull(reader.GetOrdinal("NAVDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NAVDate")),
                                    DivFreq = reader["DivFreq"] as string,
                                    MarketYieldPct = (reader.IsDBNull(reader.GetOrdinal("MarketYieldPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketYieldPct")),
                                    IncomeYeldPct = (reader.IsDBNull(reader.GetOrdinal("IncomeYeldPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IncomeYeldPct")),
                                    LevAdjNAVYield = (reader.IsDBNull(reader.GetOrdinal("LevAdjNAVYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevAdjNAVYield")),
                                    NII_Share = (reader.IsDBNull(reader.GetOrdinal("NII_Share"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NII_Share")),
                                    NIICoverageRatioPct = (reader.IsDBNull(reader.GetOrdinal("NIICoverageRatioPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NIICoverageRatioPct")),
                                    PctStructuralLeverage = (reader.IsDBNull(reader.GetOrdinal("PctStructuralLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctStructuralLeverage")),
                                    PctFixedRateLeverage = (reader.IsDBNull(reader.GetOrdinal("PctFixedRateLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctFixedRateLeverage")),
                                    PctVariableRateLeverage = (reader.IsDBNull(reader.GetOrdinal("PctVariableRateLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctVariableRateLeverage")),
                                    DebttoEquity = (reader.IsDBNull(reader.GetOrdinal("DebttoEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebttoEquity")),
                                    ExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio")),
                                    IncentiveFee = (reader.IsDBNull(reader.GetOrdinal("IncentiveFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IncentiveFee")),
                                    PortfolioDebtPct = (reader.IsDBNull(reader.GetOrdinal("PortfolioDebtPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortfolioDebtPct")),
                                    PortfolioEquityPct = (reader.IsDBNull(reader.GetOrdinal("PortfolioEquityPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortfolioEquityPct")),
                                    TotalInvestments = (reader.IsDBNull(reader.GetOrdinal("TotalInvestments"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalInvestments")),
                                    InceptionDate = (reader.IsDBNull(reader.GetOrdinal("InceptionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("InceptionDate")),
                                    FundFocus = reader["FundFocus"] as string,
                                    _13DHoldersPct = (reader.IsDBNull(reader.GetOrdinal("13DHoldersPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("13DHoldersPct")),
                                    _13GHoldersPct = (reader.IsDBNull(reader.GetOrdinal("13GHoldersPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("13GHoldersPct")),
                                    CombinedActivistHoldersPct = (reader.IsDBNull(reader.GetOrdinal("CombinedActivistHoldersPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CombinedActivistHoldersPct")),
                                    InsidersPct = (reader.IsDBNull(reader.GetOrdinal("InsidersPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InsidersPct")),
                                    PctTotalLeverage = (reader.IsDBNull(reader.GetOrdinal("PctTotalLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctTotalLeverage")),
                                    PctLevCost = (reader.IsDBNull(reader.GetOrdinal("PctLevCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctLevCost")),
                                    CoreNII_Share = (reader.IsDBNull(reader.GetOrdinal("CoreNII_Share"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CoreNII_Share")),
                                    UNII_Share = (reader.IsDBNull(reader.GetOrdinal("UNII_Share"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNII_Share")),
                                    IncAndSCPYieldPct = (reader.IsDBNull(reader.GetOrdinal("IncAndSCPYieldPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IncAndSCPYieldPct")),
                                    SharesOustanding = (reader.IsDBNull(reader.GetOrdinal("SharesOustanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOustanding")),
                                    LastNAVAnnouncement = (reader.IsDBNull(reader.GetOrdinal("LastNAVAnnouncement"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastNAVAnnouncement")),
                                    PctSharesOwnedbyInstitutions = (reader.IsDBNull(reader.GetOrdinal("PctSharesOwnedbyInstitutions"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctSharesOwnedbyInstitutions")),
                                    Inc_DecPct = (reader.IsDBNull(reader.GetOrdinal("Inc_DecPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Inc_DecPct")),
                                    LastChange = (reader.IsDBNull(reader.GetOrdinal("LastChange"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastChange")),
                                    _1stLien_SeniorSecured = (reader.IsDBNull(reader.GetOrdinal("1stLien_SeniorSecured"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("1stLien_SeniorSecured")),
                                    _2ndLien_SeniorSubordinated = (reader.IsDBNull(reader.GetOrdinal("2ndLien_SeniorSubordinated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("2ndLien_SeniorSubordinated")),
                                    Unsecured_Subordinated = (reader.IsDBNull(reader.GetOrdinal("Unsecured_Subordinated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Unsecured_Subordinated")),
                                    CommonAndPreferredEquity = (reader.IsDBNull(reader.GetOrdinal("CommonAndPreferredEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommonAndPreferredEquity")),
                                    Warrants_Options = (reader.IsDBNull(reader.GetOrdinal("Warrants_Options"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Warrants_Options")),
                                    StructuredProducts_Other = (reader.IsDBNull(reader.GetOrdinal("StructuredProducts_Other"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StructuredProducts_Other")),
                                    VariablePortfolioDebt = (reader.IsDBNull(reader.GetOrdinal("VariablePortfolioDebt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VariablePortfolioDebt")),
                                    FixedPortfolioDebt = (reader.IsDBNull(reader.GetOrdinal("FixedPortfolioDebt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FixedPortfolioDebt")),
                                    NonLevExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("NonLevExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonLevExpenseRatio")),
                                    NonAccrualInvestmentsPct = (reader.IsDBNull(reader.GetOrdinal("NonAccrualInvestmentsPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonAccrualInvestmentsPct")),
                                    AverageMaturity = (reader.IsDBNull(reader.GetOrdinal("AverageMaturity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AverageMaturity")),
                                    NumberofBonds = (reader.IsDBNull(reader.GetOrdinal("NumberofBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumberofBonds")),
                                    PortfolioCompanies = (reader.IsDBNull(reader.GetOrdinal("PortfolioCompanies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortfolioCompanies")),
                                    Internally_ExternallyManaged = (reader.IsDBNull(reader.GetOrdinal("Internally_ExternallyManaged"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Internally_ExternallyManaged")),
                                    TenderCommencedOn = (reader.IsDBNull(reader.GetOrdinal("TenderCommencedOn"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderCommencedOn")),
                                    TenderExpirationDate = (reader.IsDBNull(reader.GetOrdinal("TenderExpirationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderExpirationDate")),
                                    TenderOfferPrice = (reader.IsDBNull(reader.GetOrdinal("TenderOfferPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderOfferPrice")),
                                    TenderIntendtobuyuptoPct = (reader.IsDBNull(reader.GetOrdinal("TenderIntendtobuyuptoPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderIntendtobuyuptoPct")),
                                    TenderedPct = (reader.IsDBNull(reader.GetOrdinal("TenderedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderedPct")),
                                    TenderPurchasedPct = (reader.IsDBNull(reader.GetOrdinal("TenderPurchasedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderPurchasedPct")),
                                    TenderPurchasePrice = (reader.IsDBNull(reader.GetOrdinal("TenderPurchasePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderPurchasePrice")),
                                    RepurchasePrCommencedon = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrCommencedon"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RepurchasePrCommencedon")),
                                    RepurchasePrExpirationDate = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrExpirationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RepurchasePrExpirationDate")),
                                    RepurchasePrIntendtobuyuptoPct = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrIntendtobuyuptoPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepurchasePrIntendtobuyuptoPct")),
                                    RepurchasePrIntendtospend = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrIntendtospend"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepurchasePrIntendtospend")),
                                    RepurchasePrTargetDiscountPct = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrTargetDiscountPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepurchasePrTargetDiscountPct")),
                                    RepurchasePrPurchasedPct = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrPurchasedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepurchasePrPurchasedPct")),
                                    SizeofLoansLessThan10M = (reader.IsDBNull(reader.GetOrdinal("SizeofLoansLessThan10M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SizeofLoansLessThan10M")),
                                    SizeofLoans1025M = (reader.IsDBNull(reader.GetOrdinal("SizeofLoans1025M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SizeofLoans1025M")),
                                    SizeofLoans2550M = (reader.IsDBNull(reader.GetOrdinal("SizeofLoans2550M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SizeofLoans2550M")),
                                    SizeofLoans50100M = (reader.IsDBNull(reader.GetOrdinal("SizeofLoans50100M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SizeofLoans50100M")),
                                    SizeofLoansMoreThan100M = (reader.IsDBNull(reader.GetOrdinal("SizeofLoansMoreThan100M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SizeofLoansMoreThan100M")),
                                    Employees = (reader.IsDBNull(reader.GetOrdinal("Employees"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Employees")),
                                    IncAndCapGnsYieldPct = (reader.IsDBNull(reader.GetOrdinal("IncAndCapGnsYieldPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IncAndCapGnsYieldPct")),
                                    GrossAssetNonLevExpRatio = (reader.IsDBNull(reader.GetOrdinal("GrossAssetNonLevExpRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossAssetNonLevExpRatio")),
                                    AdjCoreNIICoverage = (reader.IsDBNull(reader.GetOrdinal("AdjCoreNIICoverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjCoreNIICoverage")),
                                    NIITrend = (reader.IsDBNull(reader.GetOrdinal("NIITrend"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NIITrend")),
                                    AdjCoreNIITrend = (reader.IsDBNull(reader.GetOrdinal("AdjCoreNIITrend"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjCoreNIITrend")),
                                    MainLeverageType = reader["MainLeverageType"] as string,
                                    GAndAFeePct = (reader.IsDBNull(reader.GetOrdinal("GAndAFeePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GAndAFeePct")),
                                    Last4QGAndAFeePct = (reader.IsDBNull(reader.GetOrdinal("Last4QGAndAFeePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Last4QGAndAFeePct")),
                                    BaseManagementFeePct = (reader.IsDBNull(reader.GetOrdinal("BaseManagementFeePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseManagementFeePct")),
                                    Latest4QIncentiveFeePct = (reader.IsDBNull(reader.GetOrdinal("Latest4QIncentiveFeePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Latest4QIncentiveFeePct")),
                                    HurdleRatePct = (reader.IsDBNull(reader.GetOrdinal("HurdleRatePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HurdleRatePct")),
                                    HighWaterMark = reader["HighWaterMark"] as string,
                                    NumberofEquityHoldings = (reader.IsDBNull(reader.GetOrdinal("NumberofEquityHoldings"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumberofEquityHoldings")),
                                    Top10CompaniesInvestmentPct = (reader.IsDBNull(reader.GetOrdinal("Top10CompaniesInvestmentPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Top10CompaniesInvestmentPct")),
                                    AverageLoanSizemil = (reader.IsDBNull(reader.GetOrdinal("AverageLoanSizemil"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AverageLoanSizemil")),
                                    CLOExposurePct = (reader.IsDBNull(reader.GetOrdinal("CLOExposurePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CLOExposurePct")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BDC GlobalResearch details ");
                throw;
            }
            return fulllist;
        }

        public IList<BDCDataComparisionTO> GetBDCDataComparisionDetails(string StartDate, string EndDate, string Ticker)
        {
            IList<BDCDataComparisionTO> fulllist = new List<BDCDataComparisionTO>();
            try
            {
                string sql = GetBDCDataComparisionQuery;
                sql += " where NavDate between '" + StartDate + "' and '" + EndDate + "'";
                if (!string.IsNullOrEmpty(Ticker))
                    sql += "and b.Ticker= '" + Ticker + "'";

                sql += " order by NavDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BDCDataComparisionTO listitem = new BDCDataComparisionTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    NavDate = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    MDate = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (string)null : reader.GetDateTime(reader.GetOrdinal("NavDate")).ToString("MM/dd/yyyy"),
                                    CEFANav = (reader.IsDBNull(reader.GetOrdinal("CEFANav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFANav")),
                                    ALMNav = (reader.IsDBNull(reader.GetOrdinal("ALMNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMNav")),
                                    FinalNav = (reader.IsDBNull(reader.GetOrdinal("FinalNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinalNav")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BDC DataComparision details ");
                throw;
            }
            return fulllist;
        }

        #region JPM reports
        public IList<JPMSwapPositionDetailsTO> GetJPMSwapPositionDetails(string StartDate, string EndDate)
        {
            IList<JPMSwapPositionDetailsTO> fulllist = new List<JPMSwapPositionDetailsTO>();
            try
            {
                string sql = GetJPMSwapPositionDetailsQuery;
                sql += " where RptDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by RptDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMSwapPositionDetailsTO listitem = new JPMSwapPositionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    RptDate = (reader.IsDBNull(reader.GetOrdinal("RptDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RptDate")),
                                    UnderlierName = reader["UnderlierName"] as string,
                                    RptCurr = reader["RptCurr"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    SwapDealRef = reader["SwapDealRef"] as string,
                                    SwapName = reader["SwapName"] as string,
                                    UnderlierType = reader["UnderlierType"] as string,
                                    TradeType = reader["TradeType"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    TerminationDate = (reader.IsDBNull(reader.GetOrdinal("TerminationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TerminationDate")),
                                    LongShortInd = reader["LongShortInd"] as string,
                                    SyntheticBuySellInd = reader["SyntheticBuySellInd"] as string,
                                    OpenQty = (reader.IsDBNull(reader.GetOrdinal("OpenQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenQty")),
                                    SettledQty = (reader.IsDBNull(reader.GetOrdinal("SettledQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettledQty")),
                                    FinancingCurr = reader["FinancingCurr"] as string,
                                    PriceLocal = (reader.IsDBNull(reader.GetOrdinal("PriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLocal")),
                                    InitialFxRate = (reader.IsDBNull(reader.GetOrdinal("InitialFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InitialFxRate")),
                                    InitialPrice = (reader.IsDBNull(reader.GetOrdinal("InitialPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InitialPrice")),
                                    NotionalAmt = (reader.IsDBNull(reader.GetOrdinal("NotionalAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalAmt")),
                                    CurrPriceLocal = (reader.IsDBNull(reader.GetOrdinal("CurrPriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrPriceLocal")),
                                    CurrFxRate = (reader.IsDBNull(reader.GetOrdinal("CurrFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrFxRate")),
                                    CurrPricePrev = (reader.IsDBNull(reader.GetOrdinal("CurrPricePrev"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrPricePrev")),
                                    CurrPrice = (reader.IsDBNull(reader.GetOrdinal("CurrPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrPrice")),
                                    UnderlyingMV = (reader.IsDBNull(reader.GetOrdinal("UnderlyingMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingMV")),
                                    UnderlyingMVRptCurr = (reader.IsDBNull(reader.GetOrdinal("UnderlyingMVRptCurr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingMVRptCurr")),
                                    TradeHoldNotional = (reader.IsDBNull(reader.GetOrdinal("TradeHoldNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeHoldNotional")),
                                    TradeHoldNotionalRptCurr = (reader.IsDBNull(reader.GetOrdinal("TradeHoldNotionalRptCurr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeHoldNotionalRptCurr")),
                                    UnrealizedEquity1D = (reader.IsDBNull(reader.GetOrdinal("UnrealizedEquity1D"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedEquity1D")),
                                    UnrealizedEquity = (reader.IsDBNull(reader.GetOrdinal("UnrealizedEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedEquity")),
                                    RealizedEquity = (reader.IsDBNull(reader.GetOrdinal("RealizedEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedEquity")),
                                    BenchmarkFullName = reader["BenchmarkFullName"] as string,
                                    FinancingNotional = (reader.IsDBNull(reader.GetOrdinal("FinancingNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinancingNotional")),
                                    AvgBenchmarkRate = (reader.IsDBNull(reader.GetOrdinal("AvgBenchmarkRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgBenchmarkRate")),
                                    AvgFinancingSpread = (reader.IsDBNull(reader.GetOrdinal("AvgFinancingSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgFinancingSpread")),
                                    FinancingAllInRate = (reader.IsDBNull(reader.GetOrdinal("FinancingAllInRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinancingAllInRate")),
                                    FinancingStartDate = (reader.IsDBNull(reader.GetOrdinal("FinancingStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinancingStartDate")),
                                    FinancingEndDate = (reader.IsDBNull(reader.GetOrdinal("FinancingEndDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinancingEndDate")),
                                    AccrualDays = (reader.IsDBNull(reader.GetOrdinal("AccrualDays"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccrualDays")),
                                    DayCountBasis = reader["DayCountBasis"] as string,
                                    UnrealizedFinancing1D = (reader.IsDBNull(reader.GetOrdinal("UnrealizedFinancing1D"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedFinancing1D")),
                                    UnrealizedFinancing = (reader.IsDBNull(reader.GetOrdinal("UnrealizedFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedFinancing")),
                                    RealizedFinancing = (reader.IsDBNull(reader.GetOrdinal("RealizedFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedFinancing")),
                                    SwapFees = (reader.IsDBNull(reader.GetOrdinal("SwapFees"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapFees")),
                                    DvdExDate = (reader.IsDBNull(reader.GetOrdinal("DvdExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdExDate")),
                                    DvdPayDate = (reader.IsDBNull(reader.GetOrdinal("DvdPayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdPayDate")),
                                    DvdExDateOpenQty = (reader.IsDBNull(reader.GetOrdinal("DvdExDateOpenQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdExDateOpenQty")),
                                    DvdCpnCurr = reader["DvdCpnCurr"] as string,
                                    GrossDvdPerShare = (reader.IsDBNull(reader.GetOrdinal("GrossDvdPerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossDvdPerShare")),
                                    DvdDistribution = (reader.IsDBNull(reader.GetOrdinal("DvdDistribution"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdDistribution")),
                                    DvdFxRate = (reader.IsDBNull(reader.GetOrdinal("DvdFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdFxRate")),
                                    SwapEqAmt = (reader.IsDBNull(reader.GetOrdinal("SwapEqAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapEqAmt")),
                                    UnrealizedDvd = (reader.IsDBNull(reader.GetOrdinal("UnrealizedDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedDvd")),
                                    RealizedDvd = (reader.IsDBNull(reader.GetOrdinal("RealizedDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedDvd")),
                                    SwapMTM = (reader.IsDBNull(reader.GetOrdinal("SwapMTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapMTM")),
                                    SwapMTMRptCurr = (reader.IsDBNull(reader.GetOrdinal("SwapMTMRptCurr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapMTMRptCurr")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Swap Position details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMSwapCorpActionDetailsTO> GetJPMSwapCorpActionDetails(string StartDate, string EndDate)
        {
            IList<JPMSwapCorpActionDetailsTO> fulllist = new List<JPMSwapCorpActionDetailsTO>();
            try
            {
                string sql = GetJPMSwapCorpActionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMSwapCorpActionDetailsTO listitem = new JPMSwapCorpActionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FinancingCurr = reader["FinancingCurr"] as string,
                                    ReportingCurr = reader["ReportingCurr"] as string,
                                    PortSwapRef = reader["PortSwapRef"] as string,
                                    SwapDealRef = reader["SwapDealRef"] as string,
                                    UnderlierName = reader["UnderlierName"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    CorpActionType = reader["CorpActionType"] as string,
                                    Voluntary = reader["Voluntary"] as string,
                                    TradedQty = (reader.IsDBNull(reader.GetOrdinal("TradedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradedQty")),
                                    ExDate = (reader.IsDBNull(reader.GetOrdinal("ExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    ExDateOpenQty = (reader.IsDBNull(reader.GetOrdinal("ExDateOpenQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExDateOpenQty")),
                                    DvdPayDate = (reader.IsDBNull(reader.GetOrdinal("DvdPayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdPayDate")),
                                    Ticker = reader["Ticker"] as string,
                                    BusinessDate = (reader.IsDBNull(reader.GetOrdinal("BusinessDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BusinessDate")),
                                    CorpActionPrice = (reader.IsDBNull(reader.GetOrdinal("CorpActionPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CorpActionPrice")),
                                    CorpActionRatio = (reader.IsDBNull(reader.GetOrdinal("CorpActionRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CorpActionRatio")),
                                    IssueCntry = reader["IssueCntry"] as string,
                                    Cpn = (reader.IsDBNull(reader.GetOrdinal("Cpn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cpn")),
                                    Cusip = reader["Cusip"] as string,
                                    DvdStatus = reader["DvdStatus"] as string,
                                    InstrId = reader["InstrId"] as string,
                                    LegalEntityCd = reader["LegalEntityCd"] as string,
                                    LegalEntityName = reader["LegalEntityName"] as string,
                                    UnderlyingCurrLocal = reader["UnderlyingCurrLocal"] as string,
                                    LongShortInd = reader["LongShortInd"] as string,
                                    MarketSwapId = reader["MarketSwapId"] as string,
                                    Region = reader["Region"] as string,
                                    ReportDate = (reader.IsDBNull(reader.GetOrdinal("ReportDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportDate")),
                                    ReportPeriod = (reader.IsDBNull(reader.GetOrdinal("ReportPeriod"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportPeriod")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    RICCode = reader["RICCode"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Strategy = reader["Strategy"] as string,
                                    SwapName = reader["SwapName"] as string,
                                    SwapTradeId = reader["SwapTradeId"] as string,
                                    UnderlierSyntheticType = reader["UnderlierSyntheticType"] as string,
                                    UnderlierType = reader["UnderlierType"] as string,
                                    UnderlyingCurr = reader["UnderlyingCurr"] as string,
                                    UnwindMethodology = reader["UnwindMethodology"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Corp Action details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMSwapDividendDetailsTO> GetJPMSwapDividendDetails(string StartDate, string EndDate)
        {
            IList<JPMSwapDividendDetailsTO> fulllist = new List<JPMSwapDividendDetailsTO>();
            try
            {
                string sql = GetJPMSwapDividendDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMSwapDividendDetailsTO listitem = new JPMSwapDividendDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FinancingCurr = reader["FinancingCurr"] as string,
                                    ReportingCurr = reader["ReportingCurr"] as string,
                                    SwapDealRef = reader["SwapDealRef"] as string,
                                    UnderlierName = reader["UnderlierName"] as string,
                                    IssueCntry = reader["IssueCntry"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    ExDateOpenQty = (reader.IsDBNull(reader.GetOrdinal("ExDateOpenQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExDateOpenQty")),
                                    ExDate = (reader.IsDBNull(reader.GetOrdinal("ExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    DvdPayDate = (reader.IsDBNull(reader.GetOrdinal("DvdPayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdPayDate")),
                                    GrossDvdPerShare = (reader.IsDBNull(reader.GetOrdinal("GrossDvdPerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossDvdPerShare")),
                                    CashDvd = (reader.IsDBNull(reader.GetOrdinal("CashDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashDvd")),
                                    DvdDistribution = (reader.IsDBNull(reader.GetOrdinal("DvdDistribution"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdDistribution")),
                                    Cpn = (reader.IsDBNull(reader.GetOrdinal("Cpn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cpn")),
                                    DvdCpnCurr = reader["DvdCpnCurr"] as string,
                                    DvdAmt = (reader.IsDBNull(reader.GetOrdinal("DvdAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmt")),
                                    DvdFxRate = (reader.IsDBNull(reader.GetOrdinal("DvdFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdFxRate")),
                                    SwapEqAmt = (reader.IsDBNull(reader.GetOrdinal("SwapEqAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapEqAmt")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Dividend details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMPositionDetailsTO> GetJPMPositionDetails(string StartDate, string EndDate)
        {
            IList<JPMPositionDetailsTO> fulllist = new List<JPMPositionDetailsTO>();
            try
            {
                string sql = GetJPMPositionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMPositionDetailsTO listitem = new JPMPositionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    IssueCurr = reader["IssueCurr"] as string,
                                    AssetClassLevel1 = reader["AssetClassLevel1"] as string,
                                    SecId = reader["SecId"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    LongShortInd = reader["LongShortInd"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    PriceLocal = (reader.IsDBNull(reader.GetOrdinal("PriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLocal")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    DailyPL = (reader.IsDBNull(reader.GetOrdinal("DailyPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPL")),
                                    MTDPL = (reader.IsDBNull(reader.GetOrdinal("MTDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPL")),
                                    YTDPL = (reader.IsDBNull(reader.GetOrdinal("YTDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDPL")),
                                    NetAccrual = (reader.IsDBNull(reader.GetOrdinal("NetAccrual"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAccrual")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Position details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMTaxlotDetailsTO> GetJPMTaxlotDetails(string StartDate, string EndDate)
        {
            IList<JPMTaxlotDetailsTO> fulllist = new List<JPMTaxlotDetailsTO>();
            try
            {
                string sql = GetJPMTaxLotDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMTaxlotDetailsTO listitem = new JPMTaxlotDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Curr = reader["Curr"] as string,
                                    AssetClass = reader["AssetClass"] as string,
                                    SrcSecurityId = reader["SrcSecurityId"] as string,
                                    AssetClassCd = reader["AssetClassCd"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    OpenDate = (reader.IsDBNull(reader.GetOrdinal("OpenDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OpenDate")),
                                    SecurityId = reader["SecurityId"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    TotalCost = (reader.IsDBNull(reader.GetOrdinal("TotalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCost")),
                                    DaysLongTerm = reader["DaysLongTerm"] as string,
                                    DaysHeld = reader["DaysHeld"] as string,
                                    UnrealizedPLSTLocal = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLSTLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLSTLocal")),
                                    UnrealizedPLLTLocal = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLTLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLTLocal")),
                                    UnrealizedPLLocal = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLocal")),
                                    UnrealizedPLST = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLST"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLST")),
                                    UnrealizedPLLT = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLT"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLT")),
                                    UnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Taxlot details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMSecurityPerfTO> GetJPMSecurityPerfDetails(string StartDate, string EndDate)
        {
            IList<JPMSecurityPerfTO> fulllist = new List<JPMSecurityPerfTO>();
            try
            {
                string sql = GetJPMSecurityPerfDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order By AsOfDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMSecurityPerfTO listitem = new JPMSecurityPerfTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Curr = reader["Curr"] as string,
                                    AssetClass = reader["AssetClass"] as string,
                                    SecId = reader["SecId"] as string,
                                    SrcSecId = reader["SrcSecId"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    TotalCost = (reader.IsDBNull(reader.GetOrdinal("TotalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCost")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    BasePrice = (reader.IsDBNull(reader.GetOrdinal("BasePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BasePrice")),
                                    BaseTotalCost = (reader.IsDBNull(reader.GetOrdinal("BaseTotalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseTotalCost")),
                                    BaseMV = (reader.IsDBNull(reader.GetOrdinal("BaseMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseMV")),
                                    UnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                    YTDTradingPL = (reader.IsDBNull(reader.GetOrdinal("YTDTradingPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDTradingPL")),
                                    MTDTradingPL = (reader.IsDBNull(reader.GetOrdinal("MTDTradingPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDTradingPL")),
                                    PctEquity = (reader.IsDBNull(reader.GetOrdinal("PctEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctEquity")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Security Perf details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMMarginDetailExtTO> GetJPMMarginExtDetails(string StartDate, string EndDate)
        {
            IList<JPMMarginDetailExtTO> fulllist = new List<JPMMarginDetailExtTO>();
            try
            {
                string sql = GetJPMMarginDetailExtQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by AsOfDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMMarginDetailExtTO listitem = new JPMMarginDetailExtTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Methodology = reader["Methodology"] as string,
                                    AcctType = reader["AcctType"] as string,
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    TradeDateCash = (reader.IsDBNull(reader.GetOrdinal("TradeDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCash")),
                                    FxMTM = (reader.IsDBNull(reader.GetOrdinal("FxMTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxMTM")),
                                    FOTotalEquity = (reader.IsDBNull(reader.GetOrdinal("FOTotalEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FOTotalEquity")),
                                    Equity = (reader.IsDBNull(reader.GetOrdinal("Equity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Equity")),
                                    TotalRequirement = (reader.IsDBNull(reader.GetOrdinal("TotalRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRequirement")),
                                    RequirementChng = (reader.IsDBNull(reader.GetOrdinal("RequirementChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RequirementChng")),
                                    ExcessMV = (reader.IsDBNull(reader.GetOrdinal("ExcessMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessMV")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Margin Ext details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMMarginSummaryTO> GetJPMMarginSummaryDetails(string StartDate, string EndDate)
        {
            IList<JPMMarginSummaryTO> fulllist = new List<JPMMarginSummaryTO>();
            try
            {
                string sql = GetJPMMarginSummaryQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMMarginSummaryTO listitem = new JPMMarginSummaryTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Category = reader["Category"] as string,
                                    CurrentValue = (reader.IsDBNull(reader.GetOrdinal("CurrentValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrentValue")),
                                    PriorDayValue = (reader.IsDBNull(reader.GetOrdinal("PriorDayValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriorDayValue")),
                                    ChangeInValue = (reader.IsDBNull(reader.GetOrdinal("ChangeInValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ChangeInValue")),
                                    PctChange = (reader.IsDBNull(reader.GetOrdinal("PctChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctChange")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Margin Summary details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMSecurityBorrowRateTO> GetJPMSecurityBorrowRateDetails(string StartDate, string EndDate)
        {
            IList<JPMSecurityBorrowRateTO> fulllist = new List<JPMSecurityBorrowRateTO>();
            try
            {
                string sql = GetJPMSecurityBorrowRateQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by AsOfDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMSecurityBorrowRateTO listitem = new JPMSecurityBorrowRateTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SecDesc = reader["SecDesc"] as string,
                                    SecType = reader["SecType"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    IndicativeAvailQty = (reader.IsDBNull(reader.GetOrdinal("IndicativeAvailQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndicativeAvailQty")),
                                    IndicativeRate = (reader.IsDBNull(reader.GetOrdinal("IndicativeRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndicativeRate")),
                                    IndicativeFee = (reader.IsDBNull(reader.GetOrdinal("IndicativeFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndicativeFee")),
                                    SecRegulationSHO = reader["SecRegulationSHO"] as string,
                                    Country = reader["Country"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Security BorrowRate details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMCorpActionDetailsTO> GetJPMCorpActionDetails(string StartDate, string EndDate)
        {
            IList<JPMCorpActionDetailsTO> fulllist = new List<JPMCorpActionDetailsTO>();
            try
            {
                string sql = GetJPMCorpActionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMCorpActionDetailsTO listitem = new JPMCorpActionDetailsTO
                                {
                                    AcctName = reader["AcctName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    AssetType = reader["AssetType"] as string,
                                    EventType = reader["EventType"] as string,
                                    Event = reader["Event"] as string,
                                    Status = reader["Status"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    EligibleLongQty = (reader.IsDBNull(reader.GetOrdinal("EligibleLongQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EligibleLongQty")),
                                    EligibleShortQty = (reader.IsDBNull(reader.GetOrdinal("EligibleShortQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EligibleShortQty")),
                                    ExDate = (reader.IsDBNull(reader.GetOrdinal("ExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    RecordDate = (reader.IsDBNull(reader.GetOrdinal("RecordDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RecordDate")),
                                    PayDate = (reader.IsDBNull(reader.GetOrdinal("PayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    DeadlineDate = (reader.IsDBNull(reader.GetOrdinal("DeadlineDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DeadlineDate")),
                                    Curr = reader["Curr"] as string,
                                    DvdDate = (reader.IsDBNull(reader.GetOrdinal("DvdDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Corp Action details ");
                throw;
            }
            return fulllist;
        }

        public IList<JPMFinanceSummaryTO> GetJPMFinanceSummaryDetails(string StartDate, string EndDate)
        {
            IList<JPMFinanceSummaryTO> fulllist = new List<JPMFinanceSummaryTO>();
            try
            {
                string sql = GetJPMFinanceSummaryQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMFinanceSummaryTO listitem = new JPMFinanceSummaryTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Curr = reader["Curr"] as string,
                                    AccrualDate = (reader.IsDBNull(reader.GetOrdinal("AccrualDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AccrualDate")),
                                    ShortCollateralMV = (reader.IsDBNull(reader.GetOrdinal("ShortCollateralMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortCollateralMV")),
                                    BundledFee = (reader.IsDBNull(reader.GetOrdinal("BundledFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BundledFee")),
                                    ExcessDeficitMV = (reader.IsDBNull(reader.GetOrdinal("ExcessDeficitMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessDeficitMV")),
                                    ExcessLoanFee = (reader.IsDBNull(reader.GetOrdinal("ExcessLoanFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessLoanFee")),
                                    DebitBal = (reader.IsDBNull(reader.GetOrdinal("DebitBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitBal")),
                                    DebitAmt = (reader.IsDBNull(reader.GetOrdinal("DebitAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitAmt")),
                                    DebitPct = (reader.IsDBNull(reader.GetOrdinal("DebitPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitPct")),
                                    CreditBal = (reader.IsDBNull(reader.GetOrdinal("CreditBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditBal")),
                                    CreditAmt = (reader.IsDBNull(reader.GetOrdinal("CreditAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditAmt")),
                                    CreditPct = (reader.IsDBNull(reader.GetOrdinal("CreditPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditPct")),
                                    UnreinvProceedsBal = (reader.IsDBNull(reader.GetOrdinal("UnreinvProceedsBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnreinvProceedsBal")),
                                    UnreinvProceedsInt = (reader.IsDBNull(reader.GetOrdinal("UnreinvProceedsInt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnreinvProceedsInt")),
                                    UnreinvProceedsPct = (reader.IsDBNull(reader.GetOrdinal("UnreinvProceedsPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnreinvProceedsPct")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Finance Summary Details");
                throw;
            }
            return fulllist;
        }

        public IList<JPMMarginCurrencyTO> GetJPMMarginCurrency(string StartDate, string EndDate)
        {
            IList<JPMMarginCurrencyTO> fulllist = new List<JPMMarginCurrencyTO>();
            try
            {
                string sql = GetJPMMarginCurrencyQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMMarginCurrencyTO listitem = new JPMMarginCurrencyTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Curr = reader["Curr"] as string,
                                    CurrDesc = reader["CurrDesc"] as string,
                                    TradeDateCash = (reader.IsDBNull(reader.GetOrdinal("TradeDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCash")),
                                    TradeDateCashUSD = (reader.IsDBNull(reader.GetOrdinal("TradeDateCashUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCashUSD")),
                                    NetFxContracts = (reader.IsDBNull(reader.GetOrdinal("NetFxContracts"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetFxContracts")),
                                    NetFxContractsUSD = (reader.IsDBNull(reader.GetOrdinal("NetFxContractsUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetFxContractsUSD")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    LongMVUSD = (reader.IsDBNull(reader.GetOrdinal("LongMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMVUSD")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    ShortMVUSD = (reader.IsDBNull(reader.GetOrdinal("ShortMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVUSD")),
                                    UnhedgedExpUSD = (reader.IsDBNull(reader.GetOrdinal("UnhedgedExpUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnhedgedExpUSD")),
                                    RqmtPct = (reader.IsDBNull(reader.GetOrdinal("RqmtPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RqmtPct")),
                                    RqmtUSD = (reader.IsDBNull(reader.GetOrdinal("RqmtUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RqmtUSD")),
                                    RqmtChngUSD = (reader.IsDBNull(reader.GetOrdinal("RqmtChngUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RqmtChngUSD")),
                                    RqmtChngPct = (reader.IsDBNull(reader.GetOrdinal("RqmtChngPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RqmtChngPct")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Margin Currency");
                throw;
            }
            return fulllist;
        }

        public IList<JPMMarginCurrencyDetailsTO> GetJPMMarginCurrencyDetails(string StartDate, string EndDate)
        {
            IList<JPMMarginCurrencyDetailsTO> fulllist = new List<JPMMarginCurrencyDetailsTO>();
            try
            {
                string sql = getJPMMarginCurrencyDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by FileDate desc, RowId asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMMarginCurrencyDetailsTO listitem = new JPMMarginCurrencyDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    CurrDesc = reader["CurrDesc"] as string,
                                    FXContractType = reader["FXContractType"] as string,
                                    FXContractDesc = reader["FXContractDesc"] as string,
                                    FXContractExpDate = (reader.IsDBNull(reader.GetOrdinal("FXContractExpDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FXContractExpDate")),
                                    FXContractAmt = (reader.IsDBNull(reader.GetOrdinal("FXContractAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractAmt")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    FXContractMV = (reader.IsDBNull(reader.GetOrdinal("FXContractMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMV")),
                                    FXContractMTMValue = (reader.IsDBNull(reader.GetOrdinal("FXContractMTMValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMTMValue")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Margin Currency Details");
                throw;
            }

            return fulllist;
        }
        #endregion

        public IList<EDFFundDetailsTO> GetEDFFundDetails(string StartDate, string EndDate)
        {
            IList<EDFFundDetailsTO> fulllist = new List<EDFFundDetailsTO>();
            try
            {
                string sql = GetEDFFundDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EDFFundDetailsTO listitem = new EDFFundDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Curr = reader["Curr"] as string,
                                    AcctBal = (reader.IsDBNull(reader.GetOrdinal("AcctBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AcctBal")),
                                    FutuesOpenTradeQty = (reader.IsDBNull(reader.GetOrdinal("FutuesOpenTradeQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FutuesOpenTradeQty")),
                                    LongOptionMV = (reader.IsDBNull(reader.GetOrdinal("LongOptionMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongOptionMV")),
                                    ShortOptionMV = (reader.IsDBNull(reader.GetOrdinal("ShortOptionMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOptionMV")),
                                    LongSecMV = (reader.IsDBNull(reader.GetOrdinal("LongSecMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongSecMV")),
                                    ShortSecMV = (reader.IsDBNull(reader.GetOrdinal("ShortSecMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortSecMV")),
                                    TotalEquity = (reader.IsDBNull(reader.GetOrdinal("TotalEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalEquity")),
                                    LiquidatingVal = (reader.IsDBNull(reader.GetOrdinal("LiquidatingVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LiquidatingVal")),
                                    WithdrawlableFunds = (reader.IsDBNull(reader.GetOrdinal("WithdrawlableFunds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WithdrawlableFunds")),
                                    AvgEquity = (reader.IsDBNull(reader.GetOrdinal("AvgEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgEquity")),
                                    MTDComm = (reader.IsDBNull(reader.GetOrdinal("MTDComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDComm")),
                                    FuturesInitMarginReq = (reader.IsDBNull(reader.GetOrdinal("FuturesInitMarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FuturesInitMarginReq")),
                                    FuturesMainMarginReq = (reader.IsDBNull(reader.GetOrdinal("FuturesMainMarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FuturesMainMarginReq")),
                                    ExcessMargin = (reader.IsDBNull(reader.GetOrdinal("ExcessMargin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessMargin")),
                                    TotOutstandingMarginCalls = reader["TotOutstandingMarginCalls"] as string,
                                    EquitiesInitMarginReq = (reader.IsDBNull(reader.GetOrdinal("EquitiesInitMarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquitiesInitMarginReq")),
                                    TotActReq = (reader.IsDBNull(reader.GetOrdinal("TotActReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotActReq")),
                                    LongOptionOpenTradeEquity = (reader.IsDBNull(reader.GetOrdinal("LongOptionOpenTradeEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongOptionOpenTradeEquity")),
                                    ShortOptionOpenTradeEquity = (reader.IsDBNull(reader.GetOrdinal("ShortOptionOpenTradeEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOptionOpenTradeEquity")),
                                    MTDPL = (reader.IsDBNull(reader.GetOrdinal("MTDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPL")),
                                    YTDPL = (reader.IsDBNull(reader.GetOrdinal("YTDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDPL")),
                                    LastActivityDate = (reader.IsDBNull(reader.GetOrdinal("LastActivityDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastActivityDate")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    PrevFuturesInitMarginReq = (reader.IsDBNull(reader.GetOrdinal("PrevFuturesInitMarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevFuturesInitMarginReq")),
                                    PrevFuturesOpenTradeQty = (reader.IsDBNull(reader.GetOrdinal("PrevFuturesOpenTradeQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevFuturesOpenTradeQty")),
                                    PrevTotEquity = (reader.IsDBNull(reader.GetOrdinal("PrevTotEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevTotEquity")),
                                    PrevLiquidatingVal = (reader.IsDBNull(reader.GetOrdinal("PrevLiquidatingVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevLiquidatingVal")),
                                    PrevAcctBal = (reader.IsDBNull(reader.GetOrdinal("PrevAcctBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevAcctBal")),
                                    EquitiesMainMarginReq = (reader.IsDBNull(reader.GetOrdinal("EquitiesMainMarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquitiesMainMarginReq")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EDF Fund details. ");
                throw;
            }
            return fulllist;
        }

        public IList<EDFPositionDetailsTO> GetEDFPositionDetails(string StartDate, string EndDate)
        {
            IList<EDFPositionDetailsTO> fulllist = new List<EDFPositionDetailsTO>();
            try
            {
                string sql = GetEDFPositionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EDFPositionDetailsTO listitem = new EDFPositionDetailsTO
                                {
                                    AcctName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Curr = reader["Curr"] as string,
                                    Exch = reader["Exch"] as string,
                                    FuturesCode = reader["FuturesCode"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    PutCallInd = reader["PutCallInd"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),
                                    SecTypeCode = reader["SecTypeCode"] as string,
                                    OrderId = reader["OrderId"] as string,
                                    TradeType = reader["TradeType"] as string,
                                    ContractYearMonth = reader["ContractYearMonth"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    TradePrice = (reader.IsDBNull(reader.GetOrdinal("TradePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePrice")),
                                    BuySellInd = reader["BuySellInd"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    SpreadCode = reader["SpreadCode"] as string,
                                    OpenCloseCode = reader["OpenCloseCode"] as string,
                                    SettlePrice = (reader.IsDBNull(reader.GetOrdinal("SettlePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettlePrice")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    PrintPrice = (reader.IsDBNull(reader.GetOrdinal("PrintPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrintPrice")),
                                    CommentCode = reader["CommentCode"] as string,
                                    GiveInOutCode = reader["GiveInOutCode"] as string,
                                    GiveInOutFirm = reader["GiveInOutFirm"] as string,
                                    TraceId = reader["TraceId"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    OptExpDate = (reader.IsDBNull(reader.GetOrdinal("OptExpDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OptExpDate")),
                                    LastTradeDate = (reader.IsDBNull(reader.GetOrdinal("LastTradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastTradeDate")),
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier")),
                                    DeltaFactor = (reader.IsDBNull(reader.GetOrdinal("DeltaFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DeltaFactor")),
                                    Exch3 = reader["Exch3"] as string,
                                    ExchCommCode = reader["ExchCommCode"] as string,
                                    InstrType = reader["InstrType"] as string,
                                    CashSettled = reader["CashSettled"] as string,
                                    InstrDesc = reader["InstrDesc"] as string,
                                    UnderlyingExch = reader["UnderlyingExch"] as string,
                                    UnderlyingFC = reader["UnderlyingFC"] as string,
                                    UnderlyingCYTM = reader["UnderlyingCYTM"] as string,
                                    UnderlyingClosePrice = (reader.IsDBNull(reader.GetOrdinal("UnderlyingClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingClosePrice")),
                                    ChitId = reader["ChitId"] as string,
                                    SrcRef = reader["SrcRef"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EDF Position details. ");
                throw;
            }
            return fulllist;
        }

        public IList<EDFPrelimDetailsTO> GetEDFPrelimDetails(string StartDate, string EndDate)
        {
            IList<EDFPrelimDetailsTO> fulllist = new List<EDFPrelimDetailsTO>();
            try
            {
                string sql = GetEDFPrelimDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EDFPrelimDetailsTO listitem = new EDFPrelimDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RecordId = reader["RecordId"] as string,
                                    Firm = reader["Firm"] as string,
                                    Office = reader["Office"] as string,
                                    SalesMan = reader["SalesMan"] as string,
                                    Acct = reader["Acct"] as string,
                                    Exch = reader["Exch"] as string,
                                    ExchName = reader["ExchName"] as string,
                                    FuturesCd = reader["FuturesCd"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    PutCall = reader["PutCall"] as string,
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),
                                    SecDesc = reader["SecDesc"] as string,
                                    BuySellInd = reader["BuySellInd"] as string,
                                    ContractYear = (reader.IsDBNull(reader.GetOrdinal("ContractYear"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ContractYear")),
                                    ContractMonth = (reader.IsDBNull(reader.GetOrdinal("ContractMonth"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ContractMonth")),
                                    ContractDay = (reader.IsDBNull(reader.GetOrdinal("ContractDay"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ContractDay")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Qty")),
                                    TradePrice = (reader.IsDBNull(reader.GetOrdinal("TradePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePrice")),
                                    PrintableTradePrice = (reader.IsDBNull(reader.GetOrdinal("PrintableTradePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrintableTradePrice")),
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    CashAmt = (reader.IsDBNull(reader.GetOrdinal("CashAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashAmt")),
                                    SubAcct = reader["SubAcct"] as string,
                                    OrderNum = (reader.IsDBNull(reader.GetOrdinal("OrderNum"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OrderNum")),
                                    SpreadCode = reader["SpreadCode"] as string,
                                    TradeType = reader["TradeType"] as string,
                                    CommentCd = reader["CommentCd"] as string,
                                    ExecBkr = reader["ExecBkr"] as string,
                                    OppBkr = reader["OppBkr"] as string,
                                    OppFirm = reader["OppFirm"] as string,
                                    GICd = reader["GICd"] as string,
                                    GIFirm = reader["GIFirm"] as string,
                                    CurrSymbol = reader["CurrSymbol"] as string,
                                    ProdCurr = reader["ProdCurr"] as string,
                                    ProdType = reader["ProdType"] as string,
                                    Tracer = reader["Tracer"] as string,
                                    ExecTime = reader["ExecTime"] as string,
                                    OptExpDate = (reader.IsDBNull(reader.GetOrdinal("OptExpDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OptExpDate")),
                                    LastTrdDate = (reader.IsDBNull(reader.GetOrdinal("LastTrdDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastTrdDate")),
                                    TradedExch = reader["TradedExch"] as string,
                                    SubExch = reader["SubExch"] as string,
                                    SourceRef = reader["SourceRef"] as string,
                                    ExchCommCd = reader["ExchCommCd"] as string,
                                    MultFactor = (reader.IsDBNull(reader.GetOrdinal("MultFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MultFactor")),
                                    Cusip = reader["Cusip"] as string,
                                    ChitNum = (reader.IsDBNull(reader.GetOrdinal("ChitNum"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ChitNum")),
                                    ExtAcct = reader["ExtAcct"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EDF Prelim details. ");
                throw;
            }
            return fulllist;
        }

        public IList<EDFPfdfDetailsTO> GetEDFPfdfDetails(string StartDate, string EndDate)
        {
            IList<EDFPfdfDetailsTO> fulllist = new List<EDFPfdfDetailsTO>();
            try
            {
                string sql = GetEDFPfdfDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EDFPfdfDetailsTO listitem = new EDFPfdfDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RecordId = reader["RecordId"] as string,
                                    Firm = reader["Firm"] as string,
                                    Office = reader["Office"] as string,
                                    Acct = (reader.IsDBNull(reader.GetOrdinal("Acct"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Acct")),
                                    AcctType = reader["AcctType"] as string,
                                    CurrSymbol = reader["CurrSymbol"] as string,
                                    SalesMan = reader["SalesMan"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    BuySellInd = (reader.IsDBNull(reader.GetOrdinal("BuySellInd"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("BuySellInd")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Qty")),
                                    Exch = reader["Exch"] as string,
                                    FutureCd = reader["FutureCd"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    ContractYearMonth = reader["ContractYearMonth"] as string,
                                    PromptDay = reader["PromptDay"] as string,
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),
                                    PutCall = reader["PutCall"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    TradePrice = (reader.IsDBNull(reader.GetOrdinal("TradePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePrice")),
                                    PrintablePrice = (reader.IsDBNull(reader.GetOrdinal("PrintablePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrintablePrice")),
                                    TradeType = reader["TradeType"] as string,
                                    OrderNum = (reader.IsDBNull(reader.GetOrdinal("OrderNum"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OrderNum")),
                                    SecurityTypeCode = reader["SecurityTypeCode"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    CommentCd = reader["CommentCd"] as string,
                                    SpreadCd = reader["SpreadCd"] as string,
                                    OpenCloseCd = reader["OpenCloseCd"] as string,
                                    Trace = reader["Trace"] as string,
                                    RoundHalf = reader["RoundHalf"] as string,
                                    ExecBkr = reader["ExecBkr"] as string,
                                    OppBkr = reader["OppBkr"] as string,
                                    OppFirm = reader["OppFirm"] as string,
                                    Commission = (reader.IsDBNull(reader.GetOrdinal("Commission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Commission")),
                                    Date = (reader.IsDBNull(reader.GetOrdinal("Date"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Date")),
                                    OptExpDate = (reader.IsDBNull(reader.GetOrdinal("OptExpDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OptExpDate")),
                                    LastTrdDate = (reader.IsDBNull(reader.GetOrdinal("LastTrdDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastTrdDate")),
                                    NetAmount = (reader.IsDBNull(reader.GetOrdinal("NetAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAmount")),
                                    TradeExch = reader["TradeExch"] as string,
                                    SubExch = reader["SubExch"] as string,
                                    Exch3 = reader["Exch3"] as string,
                                    ExchCommCd = reader["ExchCommCd"] as string,
                                    MultFactor = (reader.IsDBNull(reader.GetOrdinal("MultFactor"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("MultFactor")),
                                    ChitNum = reader["ChitNum"] as string,
                                    InstrType = reader["InstrType"] as string,
                                    CashSettled = reader["CashSettled"] as string,
                                    InstrDesc = reader["InstrDesc"] as string,
                                    TradeExecTime = reader["TradeExecTime"] as string,
                                    SettlementPrice = (reader.IsDBNull(reader.GetOrdinal("SettlementPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettlementPrice")),
                                    SourceRef = reader["SourceRef"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EDF Pfdf details. ");
                throw;
            }
            return fulllist;
        }

        public IList<FundSummaryTO> GetFundSummary(string fundName, DateTime asofDate)
        {
            IList<FundSummaryTO> list = new List<FundSummaryTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundSummaryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_FundName", fundName);
                        command.Parameters.AddWithValue("p_Date", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSummaryTO data = new FundSummaryTO
                                {
                                    FundName = reader["FundName"] as string,
                                    Measure = reader["Measure"] as string,
                                    FidelityVal = (reader.IsDBNull(reader.GetOrdinal("FidelityVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FidelityVal")),
                                    JPMVal = (reader.IsDBNull(reader.GetOrdinal("JPMVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("JPMVal")),
                                    JPMSubAcctVal = (reader.IsDBNull(reader.GetOrdinal("JPMSubAcctVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("JPMSubAcctVal")),
                                    JPMSwapVal = (reader.IsDBNull(reader.GetOrdinal("JPMSwapVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("JPMSwapVal")),
                                    IBVal = (reader.IsDBNull(reader.GetOrdinal("IBVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IBVal")),
                                    MarexVal = (reader.IsDBNull(reader.GetOrdinal("MarexVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarexVal")),
                                    TDVal = (reader.IsDBNull(reader.GetOrdinal("TDVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TDVal")),
                                    ScotiaVal = (reader.IsDBNull(reader.GetOrdinal("ScotiaVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ScotiaVal")),
                                    BMOVal = (reader.IsDBNull(reader.GetOrdinal("BMOVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BMOVal")),
                                    UBSVal = (reader.IsDBNull(reader.GetOrdinal("UBSVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UBSVal")),
                                    MSVal = (reader.IsDBNull(reader.GetOrdinal("MSVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSVal")),
                                    BAMLVal = (reader.IsDBNull(reader.GetOrdinal("BAMLVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BAMLVal")),
                                    TotalVal = (reader.IsDBNull(reader.GetOrdinal("TotalVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalVal")),
                                    FidelityDt = reader.IsDBNull(reader.GetOrdinal("FidelityDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FidelityDate")),
                                    JPMDt = reader.IsDBNull(reader.GetOrdinal("JPMDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("JPMDate")),
                                    JPMSubAcctDt = reader.IsDBNull(reader.GetOrdinal("JPMSubAcctDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("JPMSubAcctDate")),
                                    JPMSwapDt = reader.IsDBNull(reader.GetOrdinal("JPMSwapDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("JPMSwapDate")),
                                    IBDt = reader.IsDBNull(reader.GetOrdinal("IBDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("IBDate")),
                                    MarexDt = reader.IsDBNull(reader.GetOrdinal("MarexDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MarexDate")),
                                    TDDt = reader.IsDBNull(reader.GetOrdinal("TDDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TDDate")),
                                    ScotiaDt = reader.IsDBNull(reader.GetOrdinal("ScotiaDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ScotiaDate")),
                                    UBSDt = reader.IsDBNull(reader.GetOrdinal("UBSDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("UBSDate")),
                                    MSDt = reader.IsDBNull(reader.GetOrdinal("MSDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MSDate")),
                                    BAMLDt = reader.IsDBNull(reader.GetOrdinal("BAMLDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BAMLDate")),
                                    BMODt = reader.IsDBNull(reader.GetOrdinal("BMODate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BMODate")),
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

        public IList<JPMRealizedGLDetailsTO> GetJPMRealizedGLDetails(string StartDate, string EndDate)
        {
            IList<JPMRealizedGLDetailsTO> fulllist = new List<JPMRealizedGLDetailsTO>();
            try
            {
                string sql = GetJPMRealizedGLDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMRealizedGLDetailsTO listitem = new JPMRealizedGLDetailsTO
                                {
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FundName = reader["FundName"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecurityId = reader["SecurityId"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Status = reader["Status"] as string,
                                    OpenDate = (reader.IsDBNull(reader.GetOrdinal("OpenDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OpenDate")),
                                    AdjustedDate = (reader.IsDBNull(reader.GetOrdinal("AdjustedDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AdjustedDate")),
                                    CloseDate = (reader.IsDBNull(reader.GetOrdinal("CloseDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CloseDate")),
                                    Shares = (reader.IsDBNull(reader.GetOrdinal("Shares"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Shares")),
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    CostBasis = (reader.IsDBNull(reader.GetOrdinal("CostBasis"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostBasis")),
                                    WashSalesAdj = (reader.IsDBNull(reader.GetOrdinal("WashSalesAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WashSalesAdj")),
                                    FICostAdj = (reader.IsDBNull(reader.GetOrdinal("FICostAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FICostAdj")),
                                    UnitProceeds = (reader.IsDBNull(reader.GetOrdinal("UnitProceeds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitProceeds")),
                                    Proceeds = (reader.IsDBNull(reader.GetOrdinal("Proceeds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Proceeds")),
                                    EconomicGL = (reader.IsDBNull(reader.GetOrdinal("EconomicGL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EconomicGL")),
                                    _1099GL = (reader.IsDBNull(reader.GetOrdinal("1099GL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("1099GL")),
                                    DisallowedLoss = (reader.IsDBNull(reader.GetOrdinal("DisallowedLoss"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DisallowedLoss")),
                                    Term = reader["Term"] as string,
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing JPM Realized G/L details. ");
                throw;
            }
            return fulllist;
        }

        public IList<BrokerTO> GetASExecutingBrokers()
        {
            IList<BrokerTO> list = new List<BrokerTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetASExecutingBrokersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BrokerTO data = new BrokerTO();
                                data.label = reader["BrokerName"] as string;
                                data.value = reader["BrokerName"] as string;
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


        public IList<BMOTradeDetailsTO> GetBMOTradeDetails(string StartDate, string EndDate)
        {
            IList<BMOTradeDetailsTO> fulllist = new List<BMOTradeDetailsTO>();
            try
            {
                string sql = GetBMOTradeDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BMOTradeDetailsTO listitem = new BMOTradeDetailsTO
                                {
                                    Fund = reader["Fund"] as string,
                                    AcctId = reader["AcctId"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    TradeId = (reader.IsDBNull(reader.GetOrdinal("TradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeId")),
                                    AllocationId = (reader.IsDBNull(reader.GetOrdinal("AllocationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AllocationId")),
                                    TradeType = reader["TradeType"] as string,
                                    TradeStatus = reader["TradeStatus"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    SettleDate = (reader.IsDBNull(reader.GetOrdinal("SettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    UnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("UnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingPrice")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    FxRateType = reader["FxRateType"] as string,
                                    SwapPrice = (reader.IsDBNull(reader.GetOrdinal("SwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPrice")),
                                    SwapSpread = (reader.IsDBNull(reader.GetOrdinal("SwapSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapSpread")),
                                    BorrowCost = (reader.IsDBNull(reader.GetOrdinal("BorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BorrowCost")),
                                    DvdRate = (reader.IsDBNull(reader.GetOrdinal("DvdRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdRate")),
                                    CommType = reader["CommType"] as string,
                                    CommAmt = (reader.IsDBNull(reader.GetOrdinal("CommAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommAmt")),
                                    Side = reader["Side"] as string,
                                    CBUnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("CBUnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CBUnderlyingPrice")),
                                    CBSwapPrice = (reader.IsDBNull(reader.GetOrdinal("CBSwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CBSwapPrice")),
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    RicCode = reader["RicCode"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    Country = reader["Country"] as string,
                                    PrimaryInstrId = reader["PrimaryInstrId"] as string,
                                    InstrClass = reader["InstrClass"] as string,
                                    MaturityTradeDate = (reader.IsDBNull(reader.GetOrdinal("MaturityTradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturityTradeDate")),
                                    MaturitySettleDate = (reader.IsDBNull(reader.GetOrdinal("MaturitySettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturitySettleDate")),
                                    NetNotional = (reader.IsDBNull(reader.GetOrdinal("NetNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetNotional")),
                                    GrossNotional = (reader.IsDBNull(reader.GetOrdinal("GrossNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossNotional")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    TradeLogId = (reader.IsDBNull(reader.GetOrdinal("TradeLogId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeLogId")),
                                    UpdateDateTime = (reader.IsDBNull(reader.GetOrdinal("UpdateDateTime"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("UpdateDateTime")),
                                    InitialMarginPct = (reader.IsDBNull(reader.GetOrdinal("InitialMarginPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InitialMarginPct")),
                                    InitialMarginAmt = (reader.IsDBNull(reader.GetOrdinal("InitialMarginAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InitialMarginAmt")),
                                    SettlementOption = reader["SettlementOption"] as string,
                                    SettlmentElection = reader["SettlmentElection"] as string,
                                    TradeTime = (reader.IsDBNull(reader.GetOrdinal("TradeTime"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeTime")),
                                    OrderType = reader["OrderType"] as string,
                                    TradeVenue = reader["TradeVenue"] as string,
                                    CrossId = (reader.IsDBNull(reader.GetOrdinal("CrossId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CrossId")),
                                    SwapStartDate = (reader.IsDBNull(reader.GetOrdinal("SwapStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SwapStartDate")),
                                    CurveName = reader["CurveName"] as string,
                                    InstrType = reader["InstrType"] as string,
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BMO Trade details. ");
                throw;
            }
            return fulllist;
        }


        public IList<BMOPosExpDetailsTO> GetBMOPosExpDetails(string StartDate, string EndDate)
        {
            IList<BMOPosExpDetailsTO> fulllist = new List<BMOPosExpDetailsTO>();
            try
            {
                string sql = GetBMOPosExpDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BMOPosExpDetailsTO listitem = new BMOPosExpDetailsTO
                                {
                                    Fund = reader["Fund"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    TradeId = (reader.IsDBNull(reader.GetOrdinal("TradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeId")),
                                    BuySellInd = reader["BuySellInd"] as string,
                                    ProdType = reader["ProdType"] as string,
                                    Notional1 = (reader.IsDBNull(reader.GetOrdinal("Notional1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Notional1")),
                                    Curr1 = reader["Curr1"] as string,
                                    Notional2 = (reader.IsDBNull(reader.GetOrdinal("Notional2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Notional2")),
                                    Curr2 = reader["Curr2"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    MaturityDate = (reader.IsDBNull(reader.GetOrdinal("MaturityDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturityDate")),
                                    PVDate = (reader.IsDBNull(reader.GetOrdinal("PVDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PVDate")),
                                    IAAmt = (reader.IsDBNull(reader.GetOrdinal("IAAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAAmt")),
                                    ContractCurr = reader["ContractCurr"] as string,
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BMO PosExp details. ");
                throw;
            }
            return fulllist;
        }




        public IList<BMOMTMDetailsTO> GetBMOMTMDetails(string StartDate, string EndDate)
        {
            IList<BMOMTMDetailsTO> fulllist = new List<BMOMTMDetailsTO>();
            try
            {
                string sql = GetBMOMTMDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BMOMTMDetailsTO listitem = new BMOMTMDetailsTO
                                {
                                    Fund = reader["Fund"] as string,
                                    AcctId = reader["AcctId"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    Side = reader["Side"] as string,
                                    TradeDatedPos = (reader.IsDBNull(reader.GetOrdinal("TradeDatedPos"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeDatedPos")),
                                    ValueDatedPos = (reader.IsDBNull(reader.GetOrdinal("ValueDatedPos"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ValueDatedPos")),
                                    AvgCost = (reader.IsDBNull(reader.GetOrdinal("AvgCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgCost")),
                                    NotionalCost = (reader.IsDBNull(reader.GetOrdinal("NotionalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCost")),
                                    UnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("UnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingPrice")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    NotionalAmt = (reader.IsDBNull(reader.GetOrdinal("NotionalAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalAmt")),
                                    MTMAmt = (reader.IsDBNull(reader.GetOrdinal("MTMAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTMAmt")),
                                    AccruedComm = (reader.IsDBNull(reader.GetOrdinal("AccruedComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedComm")),
                                    AccruedTradingGainLoss = (reader.IsDBNull(reader.GetOrdinal("AccruedTradingGainLoss"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedTradingGainLoss")),
                                    AccruedDvd = (reader.IsDBNull(reader.GetOrdinal("AccruedDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedDvd")),
                                    AccruedNotionalFinancing = (reader.IsDBNull(reader.GetOrdinal("AccruedNotionalFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedNotionalFinancing")),
                                    AccruedBorrowCost = (reader.IsDBNull(reader.GetOrdinal("AccruedBorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedBorrowCost")),
                                    AccruedCashFinancing = (reader.IsDBNull(reader.GetOrdinal("AccruedCashFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedCashFinancing")),
                                    AccruedReset = (reader.IsDBNull(reader.GetOrdinal("AccruedReset"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedReset")),
                                    TotalPL = (reader.IsDBNull(reader.GetOrdinal("TotalPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalPL")),
                                    AvgSpread = (reader.IsDBNull(reader.GetOrdinal("AvgSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgSpread")),
                                    AvgBorrowCost = (reader.IsDBNull(reader.GetOrdinal("AvgBorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgBorrowCost")),
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    RicCode = reader["RicCode"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    Country = reader["Country"] as string,
                                    PrimaryInstrId = reader["PrimaryInstrId"] as string,
                                    InstrType = reader["InstrType"] as string,
                                    PV = (reader.IsDBNull(reader.GetOrdinal("PV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PV")),
                                    CleanPrice = (reader.IsDBNull(reader.GetOrdinal("CleanPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CleanPrice")),
                                    SwapTerminationDate = (reader.IsDBNull(reader.GetOrdinal("SwapTerminationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SwapTerminationDate")),
                                    CurveName = reader["CurveName"] as string,
                                    FinancingRate = (reader.IsDBNull(reader.GetOrdinal("FinancingRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinancingRate")),
                                    DayCount = reader["DayCount"] as string,
                                    OETDayType = reader["OETDayType"] as string,
                                    OETNotificationDays = (reader.IsDBNull(reader.GetOrdinal("OETNotificationDays"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OETNotificationDays")),
                                    OETRefPoint = reader["OETRefPoint"] as string,
                                    CptyOETDayType = reader["CptyOETDayType"] as string,
                                    CptyOETNotificationDays = (reader.IsDBNull(reader.GetOrdinal("CptyOETNotificationDays"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CptyOETNotificationDays")),
                                    CptyOETRefPoint = reader["CptyOETRefPoint"] as string,
                                    ClientStrategy = reader["ClientStrategy"] as string,
                                    InitialMarginPct = (reader.IsDBNull(reader.GetOrdinal("InitialMarginPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InitialMarginPct")),
                                    InitialMarginAmt = (reader.IsDBNull(reader.GetOrdinal("InitialMarginAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InitialMarginAmt")),
                                    SwapStartDate = (reader.IsDBNull(reader.GetOrdinal("SwapStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SwapStartDate")),
                                    LoanXId = reader["LoanXId"] as string,
                                    Transit = reader["Transit"] as string,
                                    IsSingleTRS = reader["IsSingleTRS"] as string,
                                    SingleTRSId = (reader.IsDBNull(reader.GetOrdinal("SingleTRSId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SingleTRSId")),
                                    CombinedId = reader["CombinedId"] as string,
                                    HedgeAcct = reader["HedgeAcct"] as string,
                                    UTI = reader["UTI"] as string,
                                    SettledNotionalCost = (reader.IsDBNull(reader.GetOrdinal("SettledNotionalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettledNotionalCost")),
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BMO MTM details. ");
                throw;
            }
            return fulllist;
        }

        public IList<BMOTradeRTDDetailsTO> GetBMOTradeRTDDetails(string StartDate, string EndDate)
        {
            IList<BMOTradeRTDDetailsTO> fulllist = new List<BMOTradeRTDDetailsTO>();
            try
            {
                string sql = GetBMOTradeRTDDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BMOTradeRTDDetailsTO listitem = new BMOTradeRTDDetailsTO
                                {
                                    Fund = reader["Fund"] as string,
                                    AcctId = reader["AcctId"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    TradeId = (reader.IsDBNull(reader.GetOrdinal("TradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeId")),
                                    AllocationId = (reader.IsDBNull(reader.GetOrdinal("AllocationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AllocationId")),
                                    TradeType = reader["TradeType"] as string,
                                    TradeStatus = reader["TradeStatus"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    SettleDate = (reader.IsDBNull(reader.GetOrdinal("SettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Qty")),
                                    UnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("UnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnderlyingPrice")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    FxRateType = reader["FxRateType"] as string,
                                    SwapPrice = (reader.IsDBNull(reader.GetOrdinal("SwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPrice")),
                                    SwapSpread = (reader.IsDBNull(reader.GetOrdinal("SwapSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapSpread")),
                                    BorrowCost = (reader.IsDBNull(reader.GetOrdinal("BorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BorrowCost")),
                                    DvdRate = (reader.IsDBNull(reader.GetOrdinal("DvdRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdRate")),
                                    CommType = reader["CommType"] as string,
                                    CommAmt = (reader.IsDBNull(reader.GetOrdinal("CommAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommAmt")),
                                    Side = reader["Side"] as string,
                                    CBUnderlyingPrice = (reader.IsDBNull(reader.GetOrdinal("CBUnderlyingPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CBUnderlyingPrice")),
                                    CBSwapPrice = (reader.IsDBNull(reader.GetOrdinal("CBSwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CBSwapPrice")),
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    RicCode = reader["RicCode"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    Country = reader["Country"] as string,
                                    PrimaryInstrId = reader["PrimaryInstrId"] as string,
                                    InstrClass = reader["InstrClass"] as string,
                                    MaturityTradeDate = (reader.IsDBNull(reader.GetOrdinal("MaturityTradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturityTradeDate")),
                                    MaturitySettleDate = (reader.IsDBNull(reader.GetOrdinal("MaturitySettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturitySettleDate")),
                                    NetNotional = (reader.IsDBNull(reader.GetOrdinal("NetNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetNotional")),
                                    GrossNotional = (reader.IsDBNull(reader.GetOrdinal("GrossNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossNotional")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    CrossId = reader["CrossId"] as string,
                                    ClienStrategy = reader["ClienStrategy"] as string,
                                    TranAssetName = reader["TranAssetName"] as string,
                                    PositionId = reader["PositionId"] as string,
                                    Asset = reader["Asset"] as string,
                                    SwapStartDate = (reader.IsDBNull(reader.GetOrdinal("SwapStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SwapStartDate")),
                                    CurveName = reader["CurveName"] as string,
                                    InstrType = reader["InstrType"] as string,
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BMO Trade RTD details. ");
                throw;
            }
            return fulllist;
        }

        public IList<BMOUnwindDetailsTO> GetBMOUnwindDetails(string StartDate, string EndDate)
        {
            IList<BMOUnwindDetailsTO> fulllist = new List<BMOUnwindDetailsTO>();
            try
            {
                string sql = GetBMOUnwindDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BMOUnwindDetailsTO listitem = new BMOUnwindDetailsTO
                                {
                                    Fund = reader["Fund"] as string,
                                    AcctId = reader["AcctId"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    InstrName = reader["InstrName"] as string,
                                    OpenTradeType = reader["OpenTradeType"] as string,
                                    OpenTradeId = (reader.IsDBNull(reader.GetOrdinal("OpenTradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OpenTradeId")),
                                    OpenAllocationId = (reader.IsDBNull(reader.GetOrdinal("OpenAllocationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OpenAllocationId")),
                                    OpenTradeDate = (reader.IsDBNull(reader.GetOrdinal("OpenTradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OpenTradeDate")),
                                    OpenSettleDate = (reader.IsDBNull(reader.GetOrdinal("OpenSettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OpenSettleDate")),
                                    OpenQty = (reader.IsDBNull(reader.GetOrdinal("OpenQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OpenQty")),
                                    OpenPrice = (reader.IsDBNull(reader.GetOrdinal("OpenPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenPrice")),
                                    CloseTradeId = (reader.IsDBNull(reader.GetOrdinal("CloseTradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CloseTradeId")),
                                    CloseAllocationId = (reader.IsDBNull(reader.GetOrdinal("CloseAllocationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CloseAllocationId")),
                                    CloseTradeDate = (reader.IsDBNull(reader.GetOrdinal("CloseTradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CloseTradeDate")),
                                    CloseSettleDate = (reader.IsDBNull(reader.GetOrdinal("CloseSettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CloseSettleDate")),
                                    UnwindQty = (reader.IsDBNull(reader.GetOrdinal("UnwindQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("UnwindQty")),
                                    ClosePrice = (reader.IsDBNull(reader.GetOrdinal("ClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosePrice")),
                                    UnwindCash = (reader.IsDBNull(reader.GetOrdinal("UnwindCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnwindCash")),
                                    Borrow = (reader.IsDBNull(reader.GetOrdinal("Borrow"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Borrow")),
                                    NTLFinancing = (reader.IsDBNull(reader.GetOrdinal("NTLFinancing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NTLFinancing")),
                                    Comm = (reader.IsDBNull(reader.GetOrdinal("Comm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Comm")),
                                    Dvd = (reader.IsDBNull(reader.GetOrdinal("Dvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dvd")),
                                    InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    RicCode = reader["RicCode"] as string,
                                    InstrCurr = reader["InstrCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    InstrCountry = reader["InstrCountry"] as string,
                                    PrimaryInstrId = reader["PrimaryInstrId"] as string,
                                    OpenPriceClean = (reader.IsDBNull(reader.GetOrdinal("OpenPriceClean"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenPriceClean")),
                                    ClosePriceClean = (reader.IsDBNull(reader.GetOrdinal("ClosePriceClean"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosePriceClean")),
                                    OpenAIPerUnit = (reader.IsDBNull(reader.GetOrdinal("OpenAIPerUnit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenAIPerUnit")),
                                    CloseAIPerUnit = (reader.IsDBNull(reader.GetOrdinal("CloseAIPerUnit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CloseAIPerUnit")),
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BMO Unwind details. ");
                throw;
            }
            return fulllist;
        }

        public IList<UBSSwapTradeDetailsTO> GetUBSSwapTradeDetails(string StartDate, string EndDate)
        {
            IList<UBSSwapTradeDetailsTO> fulllist = new List<UBSSwapTradeDetailsTO>();
            try
            {
                string sql = GetUBSSwapTradeDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UBSSwapTradeDetailsTO listitem = new UBSSwapTradeDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    SettleCCY = reader["SettleCCY"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    SettleDate = (reader.IsDBNull(reader.GetOrdinal("SettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    TransType = reader["TransType"] as string,
                                    Cancel = reader["Cancel"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    UBSRef = (reader.IsDBNull(reader.GetOrdinal("UBSRef"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("UBSRef")),
                                    ExecBroker = reader["ExecBroker"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    BookedPrice = (reader.IsDBNull(reader.GetOrdinal("BookedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BookedPrice")),
                                    GrossPrice = (reader.IsDBNull(reader.GetOrdinal("GrossPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossPrice")),
                                    GrossBrokerCommission = (reader.IsDBNull(reader.GetOrdinal("GrossBrokerCommission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossBrokerCommission")),
                                    NetAmount = (reader.IsDBNull(reader.GetOrdinal("NetAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAmount")),
                                    ExecFeeAmount = (reader.IsDBNull(reader.GetOrdinal("ExecFeeAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExecFeeAmount")),
                                    ExecFeeRate = (reader.IsDBNull(reader.GetOrdinal("ExecFeeRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExecFeeRate")),
                                    ExecFeeType = reader["ExecFeeType"] as string,
                                    ResearchFeeAmt = (reader.IsDBNull(reader.GetOrdinal("ResearchFeeAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResearchFeeAmt")),
                                    ResearchFeeRate = (reader.IsDBNull(reader.GetOrdinal("ResearchFeeRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResearchFeeRate")),
                                    ResearchFeeType = reader["ResearchFeeType"] as string,
                                    ThirdPartyFeeAmt = (reader.IsDBNull(reader.GetOrdinal("ThirdPartyFeeAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ThirdPartyFeeAmt")),
                                    ThirdPartyFeeRate = (reader.IsDBNull(reader.GetOrdinal("ThirdPartyFeeRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ThirdPartyFeeRate")),
                                    ThirdPartyFeeType = reader["ThirdPartyFeeType"] as string,
                                    EquitySwapIsin = reader["EquitySwapIsin"] as string,
                                    VenueIdCode = reader["VenueIdCode"] as string,
                                    ExecBrokerName = reader["ExecBrokerName"] as string,
                                    UBSCustodian = reader["UBSCustodian"] as string,
                                    SwapTradeTime = (reader.IsDBNull(reader.GetOrdinal("SwapTradeTime"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SwapTradeTime")),
                                    SwapInterestRateSource = reader["SwapInterestRateSource"] as string,
                                    InterestSpreadPercent = (reader.IsDBNull(reader.GetOrdinal("InterestSpreadPercent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InterestSpreadPercent")),
                                    BorrowRate = (reader.IsDBNull(reader.GetOrdinal("BorrowRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BorrowRate")),
                                    StrategyCd = reader["StrategyCd"] as string,
                                    RIC = reader["RIC"] as string,
                                    SwapMaturityDt = (reader.IsDBNull(reader.GetOrdinal("SwapMaturityDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SwapMaturityDt")),
                                    NetAmtExclResearchAndTP = (reader.IsDBNull(reader.GetOrdinal("NetAmtExclResearchAndTP"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAmtExclResearchAndTP")),
                                    CFI = reader["CFI"] as string,
                                    PBLEI = reader["PBLEI"] as string,
                                    Isin = reader["Isin"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing UBS Swap Trade details. ");
                throw;
            }
            return fulllist;
        }

        public IList<UBSOpenTransactionDetailsTO> GetUBSOpenTransactionDetails(string StartDate, string EndDate)
        {
            IList<UBSOpenTransactionDetailsTO> fulllist = new List<UBSOpenTransactionDetailsTO>();
            try
            {
                string sql = GetUBSOpenTransactionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UBSOpenTransactionDetailsTO listitem = new UBSOpenTransactionDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    AcctGLI = (reader.IsDBNull(reader.GetOrdinal("AcctGLI"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AcctGLI")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    CurrencyId = reader["CurrencyId"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Isin = reader["Isin"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    TradeId = (reader.IsDBNull(reader.GetOrdinal("TradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeId")),
                                    TradeDt = (reader.IsDBNull(reader.GetOrdinal("TradeDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDt")),
                                    SettlementDt = (reader.IsDBNull(reader.GetOrdinal("SettlementDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettlementDt")),
                                    OriginalQty = (reader.IsDBNull(reader.GetOrdinal("OriginalQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OriginalQty")),
                                    OpenQty = (reader.IsDBNull(reader.GetOrdinal("OpenQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OpenQty")),
                                    CostPrice = (reader.IsDBNull(reader.GetOrdinal("CostPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostPrice")),
                                    MarketPrice = (reader.IsDBNull(reader.GetOrdinal("MarketPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPrice")),
                                    CostValue = (reader.IsDBNull(reader.GetOrdinal("CostValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostValue")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue")),
                                    ProfitLoss = (reader.IsDBNull(reader.GetOrdinal("ProfitLoss"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProfitLoss")),
                                    InterestSpread = (reader.IsDBNull(reader.GetOrdinal("InterestSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InterestSpread")),
                                    DivPayRate = (reader.IsDBNull(reader.GetOrdinal("DivPayRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DivPayRate")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing UBS Open Transaction details. ");
                throw;
            }
            return fulllist;
        }

        public IList<UBSOpenPositionDetailsTO> GetUBSOpenPositionDetails(string StartDate, string EndDate)
        {
            IList<UBSOpenPositionDetailsTO> fulllist = new List<UBSOpenPositionDetailsTO>();
            try
            {
                string sql = GetUBSOpenPositionDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UBSOpenPositionDetailsTO listitem = new UBSOpenPositionDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    AccountGLI = (reader.IsDBNull(reader.GetOrdinal("AccountGLI"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountGLI")),
                                    SwapID = (reader.IsDBNull(reader.GetOrdinal("SwapID"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapID")),
                                    SwapCCY = reader["SwapCCY"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    TDQty = (reader.IsDBNull(reader.GetOrdinal("TDQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TDQty")),
                                    SDQty = (reader.IsDBNull(reader.GetOrdinal("SDQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SDQty")),
                                    SwapCostPrice = (reader.IsDBNull(reader.GetOrdinal("SwapCostPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapCostPrice")),
                                    SwapPrice = (reader.IsDBNull(reader.GetOrdinal("SwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPrice")),
                                    NotionalCost = (reader.IsDBNull(reader.GetOrdinal("NotionalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCost")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue")),
                                    SwapPnL = (reader.IsDBNull(reader.GetOrdinal("SwapPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPnL")),
                                    BasePnL = (reader.IsDBNull(reader.GetOrdinal("BasePnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BasePnL")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing UBS Open Positions details. ");
                throw;
            }
            return fulllist;
        }

        public IList<UBSValuationDetailsTO> GetUBSValuationDetails(string StartDate, string EndDate)
        {
            IList<UBSValuationDetailsTO> fulllist = new List<UBSValuationDetailsTO>();
            try
            {
                string sql = GetUBSValuationDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UBSValuationDetailsTO listitem = new UBSValuationDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    SwapCCY = reader["SwapCCY"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    TDQty = (reader.IsDBNull(reader.GetOrdinal("TDQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TDQty")),
                                    SDQty = (reader.IsDBNull(reader.GetOrdinal("SDQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SDQty")),
                                    SwapCostPrice = (reader.IsDBNull(reader.GetOrdinal("SwapCostPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapCostPrice")),
                                    SwapPrice = (reader.IsDBNull(reader.GetOrdinal("SwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPrice")),
                                    SwapCostNotional = (reader.IsDBNull(reader.GetOrdinal("SwapCostNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapCostNotional")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue")),
                                    UnrealizedPnL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPnL")),
                                    RealizedPnL = (reader.IsDBNull(reader.GetOrdinal("RealizedPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedPnL")),
                                    ResetPnL = (reader.IsDBNull(reader.GetOrdinal("ResetPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ResetPnL")),
                                    DividendCash = (reader.IsDBNull(reader.GetOrdinal("DividendCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendCash")),
                                    OtherCash = (reader.IsDBNull(reader.GetOrdinal("OtherCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OtherCash")),
                                    Financing = (reader.IsDBNull(reader.GetOrdinal("Financing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Financing")),
                                    MarginInterest = (reader.IsDBNull(reader.GetOrdinal("MarginInterest"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginInterest")),
                                    CashInterest = (reader.IsDBNull(reader.GetOrdinal("CashInterest"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashInterest")),
                                    BorrowCost = (reader.IsDBNull(reader.GetOrdinal("BorrowCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BorrowCost")),
                                    BSCharge = (reader.IsDBNull(reader.GetOrdinal("BSCharge"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BSCharge")),
                                    PTHCost = (reader.IsDBNull(reader.GetOrdinal("PTHCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PTHCost")),
                                    DividendAccruals = (reader.IsDBNull(reader.GetOrdinal("DividendAccruals"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendAccruals")),
                                    SwapPnL = (reader.IsDBNull(reader.GetOrdinal("SwapPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPnL")),
                                    BasePnL = (reader.IsDBNull(reader.GetOrdinal("BasePnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BasePnL")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing UBS Valuation details. ");
                throw;
            }
            return fulllist;
        }

        public IList<UBSValuationSummaryDetailsTO> GetUBSValuationSummaryDetails(string StartDate, string EndDate)
        {
            IList<UBSValuationSummaryDetailsTO> fulllist = new List<UBSValuationSummaryDetailsTO>();
            try
            {
                string sql = GetUBSValuationSummaryDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UBSValuationSummaryDetailsTO listitem = new UBSValuationSummaryDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    AccountGLI = (reader.IsDBNull(reader.GetOrdinal("AccountGLI"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountGLI")),
                                    SwapId = (reader.IsDBNull(reader.GetOrdinal("SwapId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapId")),
                                    SwapCCY = reader["SwapCCY"] as string,
                                    SwapCostNotional = (reader.IsDBNull(reader.GetOrdinal("SwapCostNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapCostNotional")),
                                    SwapMarketValue = (reader.IsDBNull(reader.GetOrdinal("SwapMarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapMarketValue")),
                                    SwapCash = (reader.IsDBNull(reader.GetOrdinal("SwapCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapCash")),
                                    SwapInterestAccruals = (reader.IsDBNull(reader.GetOrdinal("SwapInterestAccruals"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapInterestAccruals")),
                                    SwapDividendAccruals = (reader.IsDBNull(reader.GetOrdinal("SwapDividendAccruals"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapDividendAccruals")),
                                    SwapNetPnL = (reader.IsDBNull(reader.GetOrdinal("SwapNetPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapNetPnL")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    BaseNetPnL = (reader.IsDBNull(reader.GetOrdinal("BaseNetPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseNetPnL")),

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing UBS Valuation Summary details. ");
                throw;
            }
            return fulllist;
        }

        public IList<UBSMarginDetailsTO> GetUBSMarginDetails(string StartDate, string EndDate)
        {
            IList<UBSMarginDetailsTO> fulllist = new List<UBSMarginDetailsTO>();
            try
            {
                string sql = GetUBSMarginDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UBSMarginDetailsTO listitem = new UBSMarginDetailsTO
                                {
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RollupCcy = reader["RollupCcy"] as string,
                                    PriceCcy = reader["PriceCcy"] as string,
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    LMV = (reader.IsDBNull(reader.GetOrdinal("LMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LMV")),
                                    SMV = (reader.IsDBNull(reader.GetOrdinal("SMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SMV")),
                                    NetMV = (reader.IsDBNull(reader.GetOrdinal("NetMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetMV")),
                                    PBCash = (reader.IsDBNull(reader.GetOrdinal("PBCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PBCash")),
                                    NetOrigSwapNotional = (reader.IsDBNull(reader.GetOrdinal("NetOrigSwapNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetOrigSwapNotional")),
                                    SwapCash = (reader.IsDBNull(reader.GetOrdinal("SwapCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapCash")),
                                    NetCash = (reader.IsDBNull(reader.GetOrdinal("NetCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetCash")),
                                    Equity = (reader.IsDBNull(reader.GetOrdinal("Equity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Equity")),
                                    PositionMargin = (reader.IsDBNull(reader.GetOrdinal("PositionMargin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PositionMargin")),
                                    MarginAdj1 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj1")),
                                    MarginAdj2 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj2")),
                                    MarginAdj3 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj3"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj3")),
                                    MarginAdj4 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj4"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj4")),
                                    MarginAdj5 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj5"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj5")),
                                    TotalMargin = (reader.IsDBNull(reader.GetOrdinal("TotalMargin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalMargin")),
                                    ExcessDeficit = (reader.IsDBNull(reader.GetOrdinal("ExcessDeficit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessDeficit")),
                                    OTCMTM = (reader.IsDBNull(reader.GetOrdinal("OTCMTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OTCMTM")),
                                    RepoMV = (reader.IsDBNull(reader.GetOrdinal("RepoMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepoMV")),
                                    RepoCash = (reader.IsDBNull(reader.GetOrdinal("RepoCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepoCash")),
                                    ETDMV = (reader.IsDBNull(reader.GetOrdinal("ETDMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ETDMV")),
                                    MarginAdj6 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj6"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj6")),
                                    MarginAdj7 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj7"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj7")),
                                    MarginAdj8 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj8"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj8")),
                                    MarginAdj9 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj9"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj9")),
                                    MarginAdj10 = (reader.IsDBNull(reader.GetOrdinal("MarginAdj10"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginAdj10")),
                                    EtdCash = (reader.IsDBNull(reader.GetOrdinal("EtdCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EtdCash")),
                                    EtdCollateral = (reader.IsDBNull(reader.GetOrdinal("EtdCollateral"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EtdCollateral")),
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing UBS Valuation Summary details. ");
                throw;
            }
            return fulllist;
        }

        public IList<UBSCrossMarginDetailsTO> GetUBSCrossMarginDetails(string StartDate, string EndDate)
        {
            IList<UBSCrossMarginDetailsTO> fulllist = new List<UBSCrossMarginDetailsTO>();
            try
            {
                string sql = GetUBSCrossMarginDetailsQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "' order by FileDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UBSCrossMarginDetailsTO listitem = new UBSCrossMarginDetailsTO
                                {
                                    AcctId = reader["AcctId"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    FundName = reader["FundName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RollCcy = reader["RollCcy"] as string,
                                    MarginType = reader["MarginType"] as string,
                                    Product = reader["Product"] as string,
                                    RptGroup = reader["RptGroup"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecType = reader["SecType"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Strategy = reader["Strategy"] as string,
                                    RatingCatScenario = reader["RatingCatScenario"] as string,
                                    CnvRatio = (reader.IsDBNull(reader.GetOrdinal("CnvRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CnvRatio")),
                                    ContractMultiplier = (reader.IsDBNull(reader.GetOrdinal("ContractMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ContractMultiplier")),
                                    Duration = (reader.IsDBNull(reader.GetOrdinal("Duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Duration")),
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    PosDV01Roll = (reader.IsDBNull(reader.GetOrdinal("PosDV01Roll"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PosDV01Roll")),
                                    Delta = (reader.IsDBNull(reader.GetOrdinal("Delta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Delta")),
                                    Ccy = reader["Ccy"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin")),
                                    MarginReq = (reader.IsDBNull(reader.GetOrdinal("MarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginReq")),
                                    RIC = reader["RIC"] as string

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing UBS Cross Margin details. ");
                throw;
            }
            return fulllist;
        }
        public IList<MSPositionDetailsTO> GetMSPositionDetails(string StartDate, string EndDate)
        {
            IList<MSPositionDetailsTO> fulllist = new List<MSPositionDetailsTO>();
            try
            {
                string sql = GetMSPositionDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MSPositionDetailsTO listitem = new MSPositionDetailsTO
                                {
                                    FileName = reader["FileName"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    FundName = reader["FundName"] as string,
                                    //MainAcctId = reader["MainAcctId"] as string,
                                    //MainAcctName = reader["MainAcctName"] as string,
                                    //SubAcctId = reader["SubAcctId"] as string,
                                    //SubAcctName = reader["SubAcctName"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Quick = reader["Quick"] as string,
                                    RIC = reader["RIC"] as string,
                                    MBSCusip = reader["MBSCusip"] as string,
                                    BBGTicker = reader["BBGTicker"] as string,
                                    MSIdentifier = reader["MSIdentifier"] as string,
                                    IssuerCd = reader["IssuerCd"] as string,
                                    UnderlyingSecDesc = reader["UnderlyingSecDesc"] as string,
                                    UnderlyingCusip = reader["UnderlyingCusip"] as string,
                                    UnderlyingSymbol = reader["UnderlyingSymbol"] as string,
                                    UnderlyingSedol = reader["UnderlyingSedol"] as string,
                                    UnderlyingISIN = reader["UnderlyingISIN"] as string,
                                    UnderlyingQuick = reader["UnderlyingQuick"] as string,
                                    UnderlyingRIC = reader["UnderlyingRIC"] as string,
                                    UnderlyingBBGTicker = reader["UnderlyingBBGTicker"] as string,
                                    MSTradeRefId = reader["MSTradeRefId"] as string,
                                    ClientTradeRefId = reader["ClientTradeRefId"] as string,
                                    CurrentQty = (reader.IsDBNull(reader.GetOrdinal("CurrentQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrentQty")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    LongShortCd = reader["LongShortCd"] as string,
                                    PriceIssue = (reader.IsDBNull(reader.GetOrdinal("PriceIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceIssue")),
                                    PriceUSD = (reader.IsDBNull(reader.GetOrdinal("PriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceUSD")),
                                    MVIssue = (reader.IsDBNull(reader.GetOrdinal("MVIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVIssue")),
                                    MVUSD = (reader.IsDBNull(reader.GetOrdinal("MVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVUSD")),
                                    GrossMVIssue = (reader.IsDBNull(reader.GetOrdinal("GrossMVIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMVIssue")),
                                    GrossMVUSD = (reader.IsDBNull(reader.GetOrdinal("GrossMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMVUSD")),
                                    AccruedIntIssue = (reader.IsDBNull(reader.GetOrdinal("AccruedIntIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedIntIssue")),
                                    AccruedIntUSD = (reader.IsDBNull(reader.GetOrdinal("AccruedIntUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedIntUSD")),
                                    PendingPaymentIssue = (reader.IsDBNull(reader.GetOrdinal("PendingPaymentIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PendingPaymentIssue")),
                                    PendingPaymentUSD = (reader.IsDBNull(reader.GetOrdinal("PendingPaymentUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PendingPaymentUSD")),
                                    NotionalCostIssue = (reader.IsDBNull(reader.GetOrdinal("NotionalCostIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCostIssue")),
                                    NotionalCostUSD = (reader.IsDBNull(reader.GetOrdinal("NotionalCostUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCostUSD")),
                                    OrigPremiumIssue = (reader.IsDBNull(reader.GetOrdinal("OrigPremiumIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrigPremiumIssue")),
                                    OrigPremiumUSD = (reader.IsDBNull(reader.GetOrdinal("OrigPremiumUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrigPremiumUSD")),
                                    IssueCurr = reader["IssueCurr"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    FXRateIssueUSD = (reader.IsDBNull(reader.GetOrdinal("FXRateIssueUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRateIssueUSD")),
                                    FXRateIndIssueUSD = reader["FXRateIndIssueUSD"] as string,
                                    FXRateSettleUSD = (reader.IsDBNull(reader.GetOrdinal("FXRateSettleUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRateSettleUSD")),
                                    FXRateIndSettleUSD = reader["FXRateIndSettleUSD"] as string,
                                    ProdType = reader["ProdType"] as string,
                                    PosTypeDesc = reader["PosTypeDesc"] as string,
                                    PosType = reader["PosType"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    StartDate = (reader.IsDBNull(reader.GetOrdinal("StartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartDate")),
                                    MaturityDate = (reader.IsDBNull(reader.GetOrdinal("MaturityDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaturityDate")),
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),
                                    ConvFactor = (reader.IsDBNull(reader.GetOrdinal("ConvFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ConvFactor")),
                                    NumOfTicks = (reader.IsDBNull(reader.GetOrdinal("NumOfTicks"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumOfTicks")),
                                    TickValue = (reader.IsDBNull(reader.GetOrdinal("TickValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TickValue")),
                                    CustodianCd = reader["CustodianCd"] as string,
                                    NotionalFlag = reader["NotionalFlag"] as string,
                                    DayCountMethod = reader["DayCountMethod"] as string,
                                    AcctType = reader["AcctType"] as string,
                                    CpnRate = (reader.IsDBNull(reader.GetOrdinal("CpnRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CpnRate")),
                                    SettleDateBalIssue = (reader.IsDBNull(reader.GetOrdinal("SettleDateBalIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateBalIssue")),
                                    SettleDateBalBase = (reader.IsDBNull(reader.GetOrdinal("SettleDateBalBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateBalBase")),
                                    FXRateIssueBase = (reader.IsDBNull(reader.GetOrdinal("FXRateIssueBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRateIssueBase")),
                                    FXRateIndIssueBase = reader["FXRateIndIssueBase"] as string,
                                    FXRateSettleBase = (reader.IsDBNull(reader.GetOrdinal("FXRateSettleBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRateSettleBase")),
                                    FXRateIndSettleBase = reader["FXRateIndSettleBase"] as string,
                                    BaseCurr = reader["BaseCurr"] as string,
                                    PriceBase = (reader.IsDBNull(reader.GetOrdinal("PriceBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceBase")),
                                    MVBase = (reader.IsDBNull(reader.GetOrdinal("MVBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVBase")),
                                    GrossMVBase = (reader.IsDBNull(reader.GetOrdinal("GrossMVBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMVBase")),
                                    AccruedIntBase = (reader.IsDBNull(reader.GetOrdinal("AccruedIntBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedIntBase")),
                                    PendingPaymentBase = (reader.IsDBNull(reader.GetOrdinal("PendingPaymentBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PendingPaymentBase")),
                                    NotionalCostBase = (reader.IsDBNull(reader.GetOrdinal("NotionalCostBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCostBase")),
                                    OrigPremiumBase = (reader.IsDBNull(reader.GetOrdinal("OrigPremiumBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrigPremiumBase")),
                                    ExchCd = reader["ExchCd"] as string,
                                    AssetClass = reader["AssetClass"] as string,
                                    Direction = reader["Direction"] as string,
                                    IndependentAmt = (reader.IsDBNull(reader.GetOrdinal("IndependentAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndependentAmt")),
                                    Rate = (reader.IsDBNull(reader.GetOrdinal("Rate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Rate")),
                                    BloombergCd = reader["BloombergCd"] as string,
                                    UnderlyingProdType = reader["UnderlyingProdType"] as string,
                                    ExpirationDate = (reader.IsDBNull(reader.GetOrdinal("ExpirationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpirationDate")),
                                    NotionalCostSettle = (reader.IsDBNull(reader.GetOrdinal("NotionalCostSettle"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NotionalCostSettle")),
                                    SettlementDateQty = (reader.IsDBNull(reader.GetOrdinal("SettlementDateQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettlementDateQty")),
                                    SettleDateGrossMVIssue = (reader.IsDBNull(reader.GetOrdinal("SettleDateGrossMVIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateGrossMVIssue")),
                                    SettleDateGrossMVUSD = (reader.IsDBNull(reader.GetOrdinal("SettleDateGrossMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateGrossMVUSD")),
                                    SettleDateGrossMVBase = (reader.IsDBNull(reader.GetOrdinal("SettleDateGrossMVBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateGrossMVBase")),
                                    SwapFuturesContractSize = (reader.IsDBNull(reader.GetOrdinal("SwapFuturesContractSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapFuturesContractSize")),
                                    SwapFuturesNumContracts = (reader.IsDBNull(reader.GetOrdinal("SwapFuturesNumContracts"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapFuturesNumContracts")),
                                    PayToHoldInd = reader["PayToHoldInd"] as string,
                                    CommodityOptionsPutCallInd = reader["CommodityOptionsPutCallInd"] as string,
                                    TradingNameId = reader["TradingNameId"] as string,
                                    TradingName = reader["TradingName"] as string,
                                    USI = (reader.IsDBNull(reader.GetOrdinal("USI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("USI")),
                                    UTI = (reader.IsDBNull(reader.GetOrdinal("UTI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UTI")),
                                    RestrictedQtySwaps = (reader.IsDBNull(reader.GetOrdinal("RestrictedQtySwaps"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RestrictedQtySwaps")),
                                    AccruedDvdIssue = (reader.IsDBNull(reader.GetOrdinal("AccruedDvdIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedDvdIssue")),
                                    AccruedDvdUSD = (reader.IsDBNull(reader.GetOrdinal("AccruedDvdUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedDvdUSD")),
                                    AccruedDvdBase = (reader.IsDBNull(reader.GetOrdinal("AccruedDvdBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedDvdBase")),
                                    AccruedStock = (reader.IsDBNull(reader.GetOrdinal("AccruedStock"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedStock")),
                                    SettleCurr = reader["SettleCurr"] as string,
                                    ReportName = reader["ReportName"] as string,
                                    GenerationAcctNum = reader["GenerationAcctNum"] as string,
                                    RunDate = (reader.IsDBNull(reader.GetOrdinal("RunDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RunDate")),
                                    BloombergId = reader["BloombergId"] as string,
                                    SNSId = reader["SNSId"] as string,
                                    PendingAssetTransferQty = (reader.IsDBNull(reader.GetOrdinal("PendingAssetTransferQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PendingAssetTransferQty")),
                                    PendingAssetTransferAmtIssue = (reader.IsDBNull(reader.GetOrdinal("PendingAssetTransferAmtIssue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PendingAssetTransferAmtIssue")),
                                    PendingAssetTransferAmtUSD = (reader.IsDBNull(reader.GetOrdinal("PendingAssetTransferAmtUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PendingAssetTransferAmtUSD")),
                                    PendingAssetTransferAmtBase = (reader.IsDBNull(reader.GetOrdinal("PendingAssetTransferAmtBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PendingAssetTransferAmtBase")),
                                    BloombergGlobalId = reader["BloombergGlobalId"] as string,
                                    REDCd = reader["REDCd"] as string,
                                    CompositeRIC = reader["CompositeRIC"] as string,
                                    FXOptionTypeCd = reader["FXOptionTypeCd"] as string,
                                    REDIdentifier = reader["REDIdentifier"] as string,
                                    BloombergGlobalComposite = reader["BloombergGlobalComposite"] as string,
                                    AISettleDateSettle = (reader.IsDBNull(reader.GetOrdinal("AISettleDateSettle"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AISettleDateSettle")),
                                    AISettleDateBase = (reader.IsDBNull(reader.GetOrdinal("AISettleDateBase"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AISettleDateBase")),
                                    AISettleDateUSD = (reader.IsDBNull(reader.GetOrdinal("AISettleDateUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AISettleDateUSD")),
                                    FXProdType = reader["FXProdType"] as string,
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Morgam Stanley details. ");
                throw;
            }
            return fulllist;
        }

        public IList<MSFundDetailsTO> GetMSFundDetails(string StartDate, string EndDate)
        {
            IList<MSFundDetailsTO> fulllist = new List<MSFundDetailsTO>();
            try
            {
                string sql = GetMSFundDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MSFundDetailsTO listitem = new MSFundDetailsTO
                                {
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    FundName = reader["FundName"] as string,
                                    //AcctId = reader["AcctId"] as string,
                                    //AcctName = reader["AcctName"] as string,
                                    EquityCurrent = (reader.IsDBNull(reader.GetOrdinal("EquityCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityCurrent")),
                                    EquityPrevious = (reader.IsDBNull(reader.GetOrdinal("EquityPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityPrevious")),
                                    EquityDODChng = (reader.IsDBNull(reader.GetOrdinal("EquityDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityDODChng")),
                                    EquityPctChng = (reader.IsDBNull(reader.GetOrdinal("EquityPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityPctChng")),
                                    HouseReqCurrent = (reader.IsDBNull(reader.GetOrdinal("HouseReqCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseReqCurrent")),
                                    HouseReqPrevious = (reader.IsDBNull(reader.GetOrdinal("HouseReqPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseReqPrevious")),
                                    HouseReqDODChng = (reader.IsDBNull(reader.GetOrdinal("HouseReqDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseReqDODChng")),
                                    HouseReqPctChng = (reader.IsDBNull(reader.GetOrdinal("HouseReqPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseReqPctChng")),
                                    HouseExcessCurrent = (reader.IsDBNull(reader.GetOrdinal("HouseExcessCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseExcessCurrent")),
                                    HouseExcessPrevious = (reader.IsDBNull(reader.GetOrdinal("HouseExcessPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseExcessPrevious")),
                                    HouseExcessDODChng = (reader.IsDBNull(reader.GetOrdinal("HouseExcessDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseExcessDODChng")),
                                    HouseExcessPctChng = (reader.IsDBNull(reader.GetOrdinal("HouseExcessPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseExcessPctChng")),
                                    RegExcessDeficitCurrent = (reader.IsDBNull(reader.GetOrdinal("RegExcessDeficitCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RegExcessDeficitCurrent")),
                                    RegExcessDeficitPrevious = (reader.IsDBNull(reader.GetOrdinal("RegExcessDeficitPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RegExcessDeficitPrevious")),
                                    RegExcessDeficitDODChng = (reader.IsDBNull(reader.GetOrdinal("RegExcessDeficitDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RegExcessDeficitDODChng")),
                                    RegExcessDeficitPctChng = (reader.IsDBNull(reader.GetOrdinal("RegExcessDeficitPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RegExcessDeficitPctChng")),
                                    PotentialCashAvailCurrent = (reader.IsDBNull(reader.GetOrdinal("PotentialCashAvailCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PotentialCashAvailCurrent")),
                                    PotentialCashAvailPrevious = (reader.IsDBNull(reader.GetOrdinal("PotentialCashAvailPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PotentialCashAvailPrevious")),
                                    PotentialCashAvailDOD = (reader.IsDBNull(reader.GetOrdinal("PotentialCashAvailDOD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PotentialCashAvailDOD")),
                                    PotentialCashAvailPctChng = (reader.IsDBNull(reader.GetOrdinal("PotentialCashAvailPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PotentialCashAvailPctChng")),
                                    ProductTypeDesc = reader["ProductTypeDesc"] as string,
                                    LongExp = (reader.IsDBNull(reader.GetOrdinal("LongExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongExp")),
                                    LongExpDODChng = (reader.IsDBNull(reader.GetOrdinal("LongExpDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongExpDODChng")),
                                    ShortExp = (reader.IsDBNull(reader.GetOrdinal("ShortExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortExp")),
                                    ShortExpDODChng = (reader.IsDBNull(reader.GetOrdinal("ShortExpDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortExpDODChng")),
                                    NetEquity = (reader.IsDBNull(reader.GetOrdinal("NetEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEquity")),
                                    NetEquityDODChng = (reader.IsDBNull(reader.GetOrdinal("NetEquityDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEquityDODChng")),
                                    Requirement = (reader.IsDBNull(reader.GetOrdinal("Requirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Requirement")),
                                    ReqDODChng = (reader.IsDBNull(reader.GetOrdinal("ReqDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReqDODChng")),
                                    PortReqAdjustment = (reader.IsDBNull(reader.GetOrdinal("PortReqAdjustment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortReqAdjustment")),
                                    PortReqAdjustmentDODChng = (reader.IsDBNull(reader.GetOrdinal("PortReqAdjustmentDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortReqAdjustmentDODChng")),
                                    FXReq = (reader.IsDBNull(reader.GetOrdinal("FXReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXReq")),
                                    FXReqDODChng = (reader.IsDBNull(reader.GetOrdinal("FXReqDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXReqDODChng")),
                                    FXForwardPL = (reader.IsDBNull(reader.GetOrdinal("FXForwardPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXForwardPL")),
                                    FXForwardPLDODChng = (reader.IsDBNull(reader.GetOrdinal("FXForwardPLDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXForwardPLDODChng")),
                                    FXForwardUnrlPL = (reader.IsDBNull(reader.GetOrdinal("FXForwardUnrlPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXForwardUnrlPL")),
                                    Cash = (reader.IsDBNull(reader.GetOrdinal("Cash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cash")),
                                    CashDODChng = (reader.IsDBNull(reader.GetOrdinal("CashDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashDODChng")),
                                    MoneyMarketBal = (reader.IsDBNull(reader.GetOrdinal("MoneyMarketBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MoneyMarketBal")),
                                    MoneyMarketBalDODChng = (reader.IsDBNull(reader.GetOrdinal("MoneyMarketBalDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MoneyMarketBalDODChng")),
                                    AccruedIntReq = (reader.IsDBNull(reader.GetOrdinal("AccruedIntReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedIntReq")),
                                    AccruedIntReqDOD = (reader.IsDBNull(reader.GetOrdinal("AccruedIntReqDOD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedIntReqDOD")),
                                    AccruedIntRequirement = (reader.IsDBNull(reader.GetOrdinal("AccruedIntRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedIntRequirement")),
                                    AccruedIntRequirementDOD = (reader.IsDBNull(reader.GetOrdinal("AccruedIntRequirementDOD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccruedIntRequirementDOD")),
                                    MarginBalCurrent = (reader.IsDBNull(reader.GetOrdinal("MarginBalCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginBalCurrent")),
                                    MarginBalPrevious = (reader.IsDBNull(reader.GetOrdinal("MarginBalPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginBalPrevious")),
                                    MarginBalDODChng = (reader.IsDBNull(reader.GetOrdinal("MarginBalDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginBalDODChng")),
                                    MarginBalPctChng = (reader.IsDBNull(reader.GetOrdinal("MarginBalPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginBalPctChng")),
                                    EquityRatioCurrent = (reader.IsDBNull(reader.GetOrdinal("EquityRatioCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityRatioCurrent")),
                                    EquityRatioPrevious = (reader.IsDBNull(reader.GetOrdinal("EquityRatioPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityRatioPrevious")),
                                    EquityRatioDODChng = (reader.IsDBNull(reader.GetOrdinal("EquityRatioDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityRatioDODChng")),
                                    EquityRatioPctChng = (reader.IsDBNull(reader.GetOrdinal("EquityRatioPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityRatioPctChng")),
                                    LevCurrent = (reader.IsDBNull(reader.GetOrdinal("LevCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevCurrent")),
                                    LevPrevious = (reader.IsDBNull(reader.GetOrdinal("LevPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevPrevious")),
                                    LevDODChng = (reader.IsDBNull(reader.GetOrdinal("LevDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevDODChng")),
                                    LevPCtChng = (reader.IsDBNull(reader.GetOrdinal("LevPCtChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevPCtChng")),
                                    AvgReqCurrent = (reader.IsDBNull(reader.GetOrdinal("AvgReqCurrent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgReqCurrent")),
                                    AvgReqPrevious = (reader.IsDBNull(reader.GetOrdinal("AvgReqPrevious"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgReqPrevious")),
                                    AvgReqDODChng = (reader.IsDBNull(reader.GetOrdinal("AvgReqDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgReqDODChng")),
                                    AvgReqPctChng = (reader.IsDBNull(reader.GetOrdinal("AvgReqPctChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgReqPctChng")),
                                    GrossExp = (reader.IsDBNull(reader.GetOrdinal("GrossExp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossExp")),
                                    ExpDODChng = (reader.IsDBNull(reader.GetOrdinal("ExpDODChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpDODChng")),
                                    ReqPct = (reader.IsDBNull(reader.GetOrdinal("ReqPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReqPct")),
                                    EquityAdjustment = (reader.IsDBNull(reader.GetOrdinal("EquityAdjustment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquityAdjustment")),
                                    FreeCredit = (reader.IsDBNull(reader.GetOrdinal("FreeCredit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FreeCredit")),
                                    NetEquityCategory = reader["NetEquityCategory"] as string,
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Morgam Stanley Fund Details");
                throw;
            }
            return fulllist;
        }


        public IList<MSTaxLotDetailsTO> GetMSTaxLotDetails(string StartDate, string EndDate)
        {
            IList<MSTaxLotDetailsTO> fulllist = new List<MSTaxLotDetailsTO>();
            try
            {
                string sql = GetMSTaxLotDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MSTaxLotDetailsTO listitem = new MSTaxLotDetailsTO
                                {
                                    FileName = reader["FileName"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    FundName = reader["FundName"] as string,
                                    SwapAcctName = reader["SwapAcctName"] as string,
                                    AcctId = reader["AcctId"] as string,
                                    SwapNum = reader["SwapNum"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    SettleDate = (reader.IsDBNull(reader.GetOrdinal("SettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    Stock = (reader.IsDBNull(reader.GetOrdinal("Stock"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stock")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    OpenQty = (reader.IsDBNull(reader.GetOrdinal("OpenQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenQty")),
                                    NetPrice = (reader.IsDBNull(reader.GetOrdinal("NetPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetPrice")),
                                    TotalCost = (reader.IsDBNull(reader.GetOrdinal("TotalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCost")),
                                    CurId = reader["CurId"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    Spread = (reader.IsDBNull(reader.GetOrdinal("Spread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Spread")),
                                    SpreadEffDate = (reader.IsDBNull(reader.GetOrdinal("SpreadEffDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SpreadEffDate")),
                                    DvdAccrual = (reader.IsDBNull(reader.GetOrdinal("DvdAccrual"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAccrual")),
                                    SecDesc = reader["SecDesc"] as string,
                                    DvdEntitlementPct = (reader.IsDBNull(reader.GetOrdinal("DvdEntitlementPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdEntitlementPct")),
                                    Cusip = reader["Cusip"] as string,
                                    IssueType = reader["IssueType"] as string,
                                    ListingMarkPrice = (reader.IsDBNull(reader.GetOrdinal("ListingMarkPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ListingMarkPrice")),
                                    MarkFxPrice = (reader.IsDBNull(reader.GetOrdinal("MarkFxPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarkFxPrice")),
                                    MarkPrice = (reader.IsDBNull(reader.GetOrdinal("MarkPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarkPrice")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue")),
                                    NetUnrealizedPerformance = (reader.IsDBNull(reader.GetOrdinal("NetUnrealizedPerformance"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetUnrealizedPerformance")),
                                    GrossPrice = (reader.IsDBNull(reader.GetOrdinal("GrossPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossPrice")),
                                    ExecutionPriceFx = (reader.IsDBNull(reader.GetOrdinal("ExecutionPriceFx"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExecutionPriceFx")),
                                    TradeId = reader["TradeId"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    SwapAcctNum = reader["SwapAcctNum"] as string,
                                    TerminationDate = (reader.IsDBNull(reader.GetOrdinal("TerminationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TerminationDate")),
                                    BasketCurr = reader["BasketCurr"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    BasketId = reader["BasketId"] as string,
                                    SecurityId = reader["SecurityId"] as string,
                                    ExchangeId = reader["ExchangeId"] as string,
                                    BaseCurr = reader["BaseCurr"] as string,
                                    RICCode = reader["RICCode"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Quick = reader["Quick"] as string,
                                    Valoren = reader["Valoren"] as string,
                                    CountryIssue = reader["CountryIssue"] as string,
                                    ISODomicile = reader["ISODomicile"] as string,
                                    Divisor = (reader.IsDBNull(reader.GetOrdinal("Divisor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Divisor")),
                                    ContractSize = (reader.IsDBNull(reader.GetOrdinal("ContractSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ContractSize")),
                                    BasketOpenDate = (reader.IsDBNull(reader.GetOrdinal("BasketOpenDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BasketOpenDate")),
                                    LotSize = (reader.IsDBNull(reader.GetOrdinal("LotSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LotSize")),
                                    TaxlotGroupId = (reader.IsDBNull(reader.GetOrdinal("TaxlotGroupId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TaxlotGroupId")),
                                    BaseRate = (reader.IsDBNull(reader.GetOrdinal("BaseRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseRate")),
                                    PayToHold = reader["PayToHold"] as string,
                                    USI = (reader.IsDBNull(reader.GetOrdinal("USI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("USI")),
                                    UTI = (reader.IsDBNull(reader.GetOrdinal("UTI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UTI")),
                                    HUTIGeneratingPartyRole = reader["HUTIGeneratingPartyRole"] as string,
                                    HUTIPrefix = reader["HUTIPrefix"] as string,
                                    HUTI = (reader.IsDBNull(reader.GetOrdinal("HUTI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HUTI")),
                                    BBGId = reader["BBGId"] as string,
                                    IntRate = (reader.IsDBNull(reader.GetOrdinal("IntRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntRate")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Morgam Stanley Tax Details");
                throw;
            }
            return fulllist;
        }

        public IList<MSTradeHistDetailsTO> GetMSTradeHistDetails(string StartDate, string EndDate)
        {
            IList<MSTradeHistDetailsTO> fulllist = new List<MSTradeHistDetailsTO>();
            try
            {
                string sql = GetMSTradeHistDetailsQuery;
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MSTradeHistDetailsTO listitem = new MSTradeHistDetailsTO
                                {
                                    FileName = reader["FileName"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    SwapAcctId = reader["SwapAcctId"] as string,
                                    AcctNum = reader["AcctNum"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    SwapCurr = reader["SwapCurr"] as string,
                                    SwapNum = reader["SwapNum"] as string,
                                    BasketDesc = reader["BasketDesc"] as string,
                                    BasketId = reader["BasketId"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    SettleDate = (reader.IsDBNull(reader.GetOrdinal("SettleDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    RICCode = reader["RICCode"] as string,
                                    IssueCurr = reader["IssueCurr"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    BuySell = reader["BuySell"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    ExecutionPrice = (reader.IsDBNull(reader.GetOrdinal("ExecutionPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExecutionPrice")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    Commission = (reader.IsDBNull(reader.GetOrdinal("Commission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Commission")),
                                    Embedded = (reader.IsDBNull(reader.GetOrdinal("Embedded"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Embedded")),
                                    NetPrice = (reader.IsDBNull(reader.GetOrdinal("NetPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetPrice")),
                                    Spread = (reader.IsDBNull(reader.GetOrdinal("Spread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Spread")),
                                    DvdEntitlementPct = (reader.IsDBNull(reader.GetOrdinal("DvdEntitlementPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdEntitlementPct")),
                                    Cost = (reader.IsDBNull(reader.GetOrdinal("Cost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost")),
                                    MdSymbol = reader["MdSymbol"] as string,
                                    SecurityId = reader["SecurityId"] as string,
                                    ExchangeId = reader["ExchangeId"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Quick = reader["Quick"] as string,
                                    Valoren = reader["Valoren"] as string,
                                    ListingCurr = reader["ListingCurr"] as string,
                                    CountryOfIssue = reader["CountryOfIssue"] as string,
                                    IssueType = reader["IssueType"] as string,
                                    ISODomicile = reader["ISODomicile"] as string,
                                    Divisor = (reader.IsDBNull(reader.GetOrdinal("Divisor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Divisor")),
                                    ContractSize = (reader.IsDBNull(reader.GetOrdinal("ContractSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ContractSize")),
                                    TradeRef = reader["TradeRef"] as string,
                                    Sequence = reader["Sequence"] as string,
                                    SwapAccountName = reader["SwapAccountName"] as string,
                                    RestrictedPosition = reader["RestrictedPosition"] as string,
                                    NumContracts = (reader.IsDBNull(reader.GetOrdinal("NumContracts"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumContracts")),
                                    SwapDesc = reader["SwapDesc"] as string,
                                    CommSwapCurr = (reader.IsDBNull(reader.GetOrdinal("CommSwapCurr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommSwapCurr")),
                                    AltQtyBS = (reader.IsDBNull(reader.GetOrdinal("AltQtyBS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AltQtyBS")),
                                    SwapMaturityDate = (reader.IsDBNull(reader.GetOrdinal("SwapMaturityDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SwapMaturityDate")),
                                    Strategy = reader["Strategy"] as string,
                                    AdhocFee = (reader.IsDBNull(reader.GetOrdinal("AdhocFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdhocFee")),
                                    ContainerSwapNum = reader["ContainerSwapNum"] as string,
                                    AcquisitionDate = (reader.IsDBNull(reader.GetOrdinal("AcquisitionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AcquisitionDate")),
                                    OpenClose = reader["OpenClose"] as string,
                                    LongShort = reader["LongShort"] as string,
                                    ExecutingBroker = reader["ExecutingBroker"] as string,
                                    PayToHold = reader["PayToHold"] as string,
                                    BloombergID = reader["BloombergID"] as string,
                                    Strategy2 = reader["Strategy2"] as string,

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Morgam Stanley Trade History Details");
                throw;
            }
            return fulllist;
        }

        public IList<ALMFxFwdPosDetailsTO> GetALMFxFwdPosDetails()
        {
            IList<ALMFxFwdPosDetailsTO> detailslist = new List<ALMFxFwdPosDetailsTO>();
            string sql = GetALMFxFwdPosQuery + " order by GroupId desc, Fund, Broker, Ccy1";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ALMFxFwdPosDetailsTO listitem = new ALMFxFwdPosDetailsTO
                                {
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    Fund = reader["Fund"] as string,
                                    Broker = reader["Broker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecId = reader["SecId"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Ccy1 = reader["Ccy1"] as string,
                                    Ccy2 = reader["Ccy2"] as string,
                                    ExpiryDate = (reader.IsDBNull(reader.GetOrdinal("ExpiryDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpiryDate")),
                                    RollCcy = reader["RollCcy"] as string,
                                    Ccy = reader["Ccy"] as string,
                                    Delta = (reader.IsDBNull(reader.GetOrdinal("Delta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Delta")),
                                    PriceLocal = (reader.IsDBNull(reader.GetOrdinal("PriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLocal")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    LongShortInd = reader["LongShortInd"] as string,
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin")),
                                    MarginReq = (reader.IsDBNull(reader.GetOrdinal("MarginReq"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginReq")),
                                    DailyPnL = (reader.IsDBNull(reader.GetOrdinal("DailyPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPnL")),
                                    MTDPnL = (reader.IsDBNull(reader.GetOrdinal("MTDPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPnL")),
                                    YTDPnL = (reader.IsDBNull(reader.GetOrdinal("YTDPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDPnL")),
                                    NetAccrual = (reader.IsDBNull(reader.GetOrdinal("NetAccrual"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAccrual")),
                                    GrpId = (reader.IsDBNull(reader.GetOrdinal("GroupId"))) ? (Int16?)null : reader.GetInt16(reader.GetOrdinal("GroupId")),
                                };
                                detailslist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing ALM Fx Fwd Position details ");
                throw;
            }
            return detailslist;
        }


        public IList<BAMLPerformanceDetailsTO> GetBAMLPerformanceDetails(string startDate, string endDate)
        {
           
            IList<BAMLPerformanceDetailsTO> list = new List<BAMLPerformanceDetailsTO>();
            try
            {
                string sql = GetBAMLPerformanceDetailsQuery + " where AsOfDate >= '" + startDate + "' and AsOfDate <= '" + endDate + "'";
                sql += " order by AsOfDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BAMLPerformanceDetailsTO data = new BAMLPerformanceDetailsTO
                                {
                                    AcctId = reader["AcctId"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    PeriodStartDate = (reader.IsDBNull(reader.GetOrdinal("PeriodStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PeriodStartDate")),
                                    PeriodEndDate = (reader.IsDBNull(reader.GetOrdinal("PeriodEndDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PeriodEndDate")),
                                    FinalValuationDate = (reader.IsDBNull(reader.GetOrdinal("FinalValuationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinalValuationDate")),
                                    FinalPaymentDate = (reader.IsDBNull(reader.GetOrdinal("FinalPaymentDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinalPaymentDate")),
                                    SwapCcy = reader["SwapCcy"] as string,
                                    SwapDesc = reader["SwapDesc"] as string,
                                    UnderlyingSecDesc = reader["UnderlyingSecDesc"] as string,
                                    UnderlyingId = reader["UnderlyingId"] as string,
                                    UnderlyingISIN = reader["UnderlyingISIN"] as string,
                                    UnderlyingCusip = reader["UnderlyingCusip"] as string,
                                    UnderlyingTicker = reader["UnderlyingTicker"] as string,
                                    UnderlyingQuickCode = reader["UnderlyingQuickCode"] as string,
                                    UnderlyingSedol = reader["UnderlyingSedol"] as string,
                                    UnderlyingInternalId = reader["UnderlyingInternalId"] as string,
                                    PrimaryRICCode = reader["PrimaryRICCode"] as string,
                                    UnderlyingCcy = reader["UnderlyingCcy"] as string,
                                    StartingQty = (reader.IsDBNull(reader.GetOrdinal("StartingQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StartingQty")),
                                    StartingCostBasis = (reader.IsDBNull(reader.GetOrdinal("StartingCostBasis"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StartingCostBasis")),
                                    StartingVal = (reader.IsDBNull(reader.GetOrdinal("StartingVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StartingVal")),
                                    RebalanceQty = (reader.IsDBNull(reader.GetOrdinal("RebalanceQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebalanceQty")),
                                    RebalanceCostBasis = (reader.IsDBNull(reader.GetOrdinal("RebalanceCostBasis"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebalanceCostBasis")),
                                    RebalanceVal = (reader.IsDBNull(reader.GetOrdinal("RebalanceVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebalanceVal")),
                                    ClosingQty = (reader.IsDBNull(reader.GetOrdinal("ClosingQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosingQty")),
                                    ClosingCostBasis = (reader.IsDBNull(reader.GetOrdinal("ClosingCostBasis"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosingCostBasis")),
                                    ClosingVal = (reader.IsDBNull(reader.GetOrdinal("ClosingVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosingVal")),
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier")),
                                    LocalPrice = (reader.IsDBNull(reader.GetOrdinal("LocalPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalPrice")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    SwapPrice = (reader.IsDBNull(reader.GetOrdinal("SwapPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPrice")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    MTMRtn = (reader.IsDBNull(reader.GetOrdinal("MTMRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTMRtn")),
                                    PerfRtn = (reader.IsDBNull(reader.GetOrdinal("PerfRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PerfRtn")),
                                    Dvd = (reader.IsDBNull(reader.GetOrdinal("Dvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dvd")),
                                    IntAccrual = (reader.IsDBNull(reader.GetOrdinal("IntAccrual"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntAccrual")),
                                    StockLoanFeeAccrual = (reader.IsDBNull(reader.GetOrdinal("StockLoanFeeAccrual"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockLoanFeeAccrual")),
                                    BondNetAI = (reader.IsDBNull(reader.GetOrdinal("BondNetAI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BondNetAI")),
                                    TotalReserve = (reader.IsDBNull(reader.GetOrdinal("TotalReserve"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalReserve")),
                                    ReserveEqtyRtn = (reader.IsDBNull(reader.GetOrdinal("ReserveEqtyRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReserveEqtyRtn")),
                                    ReserveDvdRtn = (reader.IsDBNull(reader.GetOrdinal("ReserveDvdRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReserveDvdRtn")),
                                    ReserveIntRtn = (reader.IsDBNull(reader.GetOrdinal("ReserveIntRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReserveIntRtn")),
                                    ReserveStockFeeRtn = (reader.IsDBNull(reader.GetOrdinal("ReserveStockFeeRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReserveStockFeeRtn")),
                                    ReserveBondIntRtn = (reader.IsDBNull(reader.GetOrdinal("ReserveBondIntRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReserveBondIntRtn")),
                                    TotalRtnSwapCcy = (reader.IsDBNull(reader.GetOrdinal("TotalRtnSwapCcy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtnSwapCcy")),
                                    ReportingCcy = (reader.IsDBNull(reader.GetOrdinal("ReportingCcy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportingCcy")),
                                    FxRateSwapToRptCcy = (reader.IsDBNull(reader.GetOrdinal("FxRateSwapToRptCcy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRateSwapToRptCcy")),
                                    TotalRtnRptCcy = (reader.IsDBNull(reader.GetOrdinal("TotalRtnRptCcy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtnRptCcy")),
                                    LegalEntity = reader["LegalEntity"] as string,
                                    UnderlyingBbTicker = reader["UnderlyingBbTicker"] as string,

                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BAML Performance details query");
                throw;
            }
            return list;
        }



        public IList<BAMLSwapActivityDetailsTO> GetBAMLSwapActivityDetails(string startDate, string endDate)
        {

            IList<BAMLSwapActivityDetailsTO> list = new List<BAMLSwapActivityDetailsTO>();
            try
            {
                string sql = GetBAMLSwapActivityDetailsQuery + " where FileDate >= '" + startDate + "' and FileDate <= '" + endDate + "'";               
                sql += " order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BAMLSwapActivityDetailsTO data = new BAMLSwapActivityDetailsTO
                                {
                                    ClientId = reader["ClientId"] as string,
                                    ClientName = reader["ClientName"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    BeneficialOwnerId = (reader.IsDBNull(reader.GetOrdinal("BeneficialOwnerId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("BeneficialOwnerId")),
                                    BeneficialOwnerName = reader["BeneficialOwnerName"] as string,
                                    AcctId = reader["AcctId"] as string,
                                    AcctName = reader["AcctName"] as string,
                                    AccountAliasId = reader["AccountAliasId"] as string,
                                    AccountAliasName = reader["AccountAliasName"] as string,
                                    InvestmentManagerName = reader["InvestmentManagerName"] as string,
                                    InvestmentManagerId = reader["InvestmentManagerId"] as string,
                                    TraderName = reader["TraderName"] as string,
                                    DeskName = reader["DeskName"] as string,
                                    StrategyName = reader["StrategyName"] as string,
                                    CustomClassificationName = reader["CustomClassificationName"] as string,
                                    PeriodDate = (reader.IsDBNull(reader.GetOrdinal("PeriodDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PeriodDate")),
                                    SwapCurrency = reader["SwapCurrency"] as string,
                                    MlReferenceNumber = reader["MlReferenceNumber"] as string,
                                    SwapDescription = reader["SwapDescription"] as string,
                                    UnderlyingDescription = reader["UnderlyingDescription"] as string,
                                    UnderlyingId = reader["UnderlyingId"] as string,
                                    UnderlyingISIN = reader["UnderlyingISIN"] as string,
                                    UnderlyingCusip = reader["UnderlyingCusip"] as string,
                                    UnderlyingTicker = reader["UnderlyingTicker"] as string,
                                    UnderlyingQuickCode = reader["UnderlyingQuickCode"] as string,
                                    UnderlyingSedol = reader["UnderlyingSedol"] as string,
                                    UnderlyingInternalId = reader["UnderlyingInternalId"] as string,
                                    PrimaryRicCode = reader["PrimaryRicCode"] as string,
                                    UnderlyingCcy = reader["UnderlyingCcy"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    SettlementDate = (reader.IsDBNull(reader.GetOrdinal("SettlementDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettlementDate")),
                                    TransactionId = (reader.IsDBNull(reader.GetOrdinal("TransactionId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TransactionId")),
                                    Status = reader["Status"] as string,
                                    TransactionType = reader["TransactionType"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Qty")),
                                    GrossPrice = (reader.IsDBNull(reader.GetOrdinal("GrossPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossPrice")),
                                    Commission = (reader.IsDBNull(reader.GetOrdinal("Commission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Commission")),
                                    NetPrice = (reader.IsDBNull(reader.GetOrdinal("NetPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetPrice")),
                                    PurchasedSoldInterest = (reader.IsDBNull(reader.GetOrdinal("PurchasedSoldInterest"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PurchasedSoldInterest")),
                                    Notional = (reader.IsDBNull(reader.GetOrdinal("Notional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Notional")),
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Multiplier")),
                                    BaseRate = (reader.IsDBNull(reader.GetOrdinal("BaseRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseRate")),
                                    StockLoanFeeRate = (reader.IsDBNull(reader.GetOrdinal("StockLoanFeeRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StockLoanFeeRate")),
                                    Spread = (reader.IsDBNull(reader.GetOrdinal("Spread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Spread")),
                                    DividendPercentage = (reader.IsDBNull(reader.GetOrdinal("DividendPercentage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendPercentage")),
                                    FinalValuationDate = (reader.IsDBNull(reader.GetOrdinal("FinalValuationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinalValuationDate")),
                                    FinalPaymentDate = (reader.IsDBNull(reader.GetOrdinal("FinalPaymentDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FinalPaymentDate")),
                                    NextResetDate = (reader.IsDBNull(reader.GetOrdinal("NextResetDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NextResetDate")),
                                    LastResetDate = (reader.IsDBNull(reader.GetOrdinal("LastResetDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastResetDate")),
                                    NextCashResetDate = (reader.IsDBNull(reader.GetOrdinal("NextCashResetDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NextCashResetDate")),
                                    DesignatedMaturity = reader["DesignatedMaturity"] as string,
                                    IndependentPercent = (reader.IsDBNull(reader.GetOrdinal("IndependentPercent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndependentPercent")),
                                    ClientStrategy = reader["ClientStrategy"] as string,
                                    PayToHoldIndicator = reader["PayToHoldIndicator"] as string,
                                    GrossNotional = (reader.IsDBNull(reader.GetOrdinal("GrossNotional"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossNotional")),
                                    AccessFee = (reader.IsDBNull(reader.GetOrdinal("AccessFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccessFee")),
                                    OtherAmt = (reader.IsDBNull(reader.GetOrdinal("OtherAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OtherAmt")),
                                    AccessAdjustment = (reader.IsDBNull(reader.GetOrdinal("AccessAdjustment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccessAdjustment")),
                                    OtherCharges = (reader.IsDBNull(reader.GetOrdinal("OtherCharges"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OtherCharges")),
                                    ClientTransId = reader["ClientTransId"] as string,
                                    ExternalBrokerComm = (reader.IsDBNull(reader.GetOrdinal("ExternalBrokerComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExternalBrokerComm")),
                                    LegalEntity = reader["LegalEntity"] as string,
                                    UnderlyingBloombergTicker = reader["UnderlyingBloombergTicker"] as string,
                                    UTI = reader["UTI"] as string,
                                    EventTypeCode = reader["EventTypeCode"] as string,
                                    EventTypeCodeDescription = reader["EventTypeCodeDescription"] as string,
                                    ExecCommission = (reader.IsDBNull(reader.GetOrdinal("ExecCommission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExecCommission")),
                                    RschCommission = (reader.IsDBNull(reader.GetOrdinal("RschCommission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RschCommission")),
                                    ExtBrokerExecCommission = (reader.IsDBNull(reader.GetOrdinal("ExtBrokerExecCommission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExtBrokerExecCommission")),
                                    ExtBrokerRschCommission = (reader.IsDBNull(reader.GetOrdinal("ExtBrokerRschCommission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExtBrokerRschCommission")),
                                    Usi = reader["Usi"] as string,
                                    PortfolioId = reader["PortfolioId"] as string,


                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing BAML Swap Activity details query");
                throw;
            }
            return list;
        }



        private const string GetSecurityMarginHistoryQuery = "spGetSecurityMarginHistory";
        private const string GetJPMSecurityBorrowRatesQuery = "select * from almitasc_ACTradingBBGData.JPMSecurityBorrowRate where Ticker is not null and length(Ticker) > 0";
        public string GetBrokerCommissionRateQuery = "select * from Trading.BrokerCommission";
        public string GetExecutedBrokersQuery = "select distinct BrokerName from Trading.BrokerMap";
        public string GetASExecutingBrokersQuery = "select distinct ucase(ExecutingBroker) as BrokerName from almitasc_ACTradingBBGData.TradeAllocation order by 1";

        //SC report queries
        public string GetSCMarginSummaryQuery = "select (case when s.AcctId = 'AOFLP' then 'OPP' when s.AcctId = 'ATFLP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.SCMarginSummary s";
        public string GetSCMarginDetailQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.SCMarginDet s";
        public string GetSCTradeDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.SCTradeDet s";
        public string GetSCMTMDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.SCMTMDet s";
        public string GetSCSecurityResetQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.SCSecurityReset s";
        public string GetSCSecurityResetDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.SCSecurityResetDet s";

        //Fidelity reports queries
        public string GetFDFundDetailsQuery = "select (case when s.AcctId = '752017932' then 'OPP' when s.AcctId = '752019549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDFundDet s";
        public string GetFDFundCashBalanceQuery = "select (case when s.AcctId = '752017932' then 'OPP' when s.AcctId = '752019549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDFundCashBal s";
        public string GetFDCorpActionDetailsQuery = "select (case when s.AcctId = '752017932' then 'OPP' when s.AcctId = '752019549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDCorpActionDet s";
        public string GetFDIssuerMarginDetailsQuery = "select (case when s.AcctId = '17932' then 'OPP' when s.AcctId = '19549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDIssuerMarginDet s";
        public string GetFDMarginAttributionDetailsQuery = "select (case when s.AcctId = '17932' then 'OPP' when s.AcctId = '19549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDMarginAttributionDet s";
        public string GetFDMarginReqDetailsQuery = "select (case when s.AcctId = '17932' then 'OPP' when s.AcctId = '19549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDMarginReqDet s";
        public string GetFDSecurityMarginDetailsQuery = "select (case when s.AcctId = '17932' then 'OPP' when s.AcctId = '19549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDSecurityMarginDet s";
        public string GetFDTaxLotDetailsQuery = "select (case when s.AcctName = 'Almitas Opportunity Fund Lp' then 'OPP' when s.AcctName = 'Almitas Tactical Fund Lp' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDTaxLot s";
        public string GetFDPositionTypeDetailsQuery = "select (case when s.AcctName = 'Almitas Opportunity Fund Lp' then 'OPP' when s.AcctName = 'Almitas Tactical Fund Lp' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDPositionTypeDet s";
        public string GetFDValuationDetailsQuery = "select (case when s.AcctId = '17932' then 'OPP' when s.AcctId = '19549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDValuationDet s";
        public string GetFDIntEarningsQuery = "select (case when s.AcctName = 'Almitas Opportunity Fund Lp' then 'OPP' when s.AcctName = 'Almitas Tactical Fund Lp' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDIntEarningDet s";
        public string GetFDPositionDetailsQuery = "select (case when s.AcctName = 'Almitas Opportunity Fund Lp' then 'OPP' when s.AcctName = 'Almitas Tactical Fund Lp' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDPositionDet s";
        public string GetFDTransactionDetailsQuery = "select (case when s.AcctId = '752017932' then 'OPP' when s.AcctId = '752019549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDTransactionDet s";
        public string GetFDSecurityRebateRateQuery = "select (case when s.AcctId = '17932' then 'OPP' when s.AcctId = '19549' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.FDSecurityRebateRate s";
        public string GetFDRealizedGLDetailsQuery = "select (case when s.AcctName = 'Almitas Opportunity Fund Lp' then 'OPP' when s.AcctName = 'Almitas Tactical Fund Lp' then 'TAC' else s.AcctName end) as FundName, s.*  from Primebrokerfiles.FDGainLossDet s";

        //TD report queries
        public string GetTDCollateralDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.TDCollateralDet s";
        public string GetTDTaxLotDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.TDTaxLotDet s";

        //IB Report queries
        public string GetIBPositionDetailsQuery = "select (case when s.AcctId = 'U1272365' then 'OPP' when s.AcctId = 'U***2365' then 'OPP' when s.AcctId = 'U2718620' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.IBPositionDet s";
        public string GetIBTaxLotDetailsQuery = "select (case when s.AcctId = 'U1272365' then 'OPP' when s.AcctId = 'U***2365' then 'OPP' when s.AcctId = 'U2718620' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.IBTaxLotDet s";

        //BDC Report Queries
        public string GetBDCGlobalResearchQuery = "select * from almitasc_ACTradingBBGData.globalresearch_cefatable4bdc";
        public string GetBDCDataComparisionQuery = "select b.* from almitasc_ACTradingBBGLink.BDCFundHist b";

        //JPM Report Queries
        public string GetJPMSwapPositionDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMSwapPositionDet s";
        public string GetJPMSwapCorpActionDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMSwapCorpActionDet s";
        public string GetJPMSwapDividendDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMSwapDividendDet s";
        public string GetJPMPositionDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMPositionDet s";
        public string GetJPMTaxLotDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMTaxLotDet s";
        public string GetJPMSecurityPerfDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMSecurityPerf s";
        public string GetJPMMarginDetailExtQuery = "select (case when s.AcctId = 'Almitas Opportunity Fund LP (ALRNQ)' then 'OPP' when s.AcctId = 'Almitas Tactical Fund LP (ALUQY)' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMMarginDetailExt s";
        public string GetJPMMarginSummaryQuery = "select (case when s.AcctId = 'Almitas Opportunity Fund LP (ALRNQ)' then 'OPP' when s.AcctId = 'Almitas Tactical Fund LP (ALUQY)' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMMarginSummary s";
        public string GetJPMSecurityBorrowRateQuery = "select * from Primebrokerfiles.JPMSecurityBorrowRate";
        public string GetJPMCorpActionDetailsQuery = "select * from Primebrokerfiles.JPMCorpActionDet";
        public string GetJPMFinanceSummaryQuery = "select (case when s.AcctName = 'Almitas Opportunity Fund LP' then 'OPP' when s.AcctName = 'Almitas Tactical Fund LP' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMFinanceSummary s";
        public string GetJPMMarginCurrencyQuery = "select (case when s.AcctId = 'Almitas Opportunity Fund LP (ALRNQ)' then 'OPP' when s.AcctId = 'Almitas Tactical Fund LP (ALUQY)' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMMarginCurrency s";
        public string getJPMMarginCurrencyDetailsQuery = "select (case when s.AcctId = 'Almitas Opportunity Fund LP (ALRNQ)' then 'OPP' when s.AcctId = 'Almitas Tactical Fund LP (ALUQY)' then 'TAC' else s.AcctId end) as FundName, s.* from Primebrokerfiles.JPMMarginCurrencyDet s";
        public string GetJPMRealizedGLDetailsQuery = "select (case when s.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when s.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else s.AcctName end) as FundName, s.* from Primebrokerfiles.JPMGainLossDet s";


        //EDF queries
        public string GetEDFFundDetailsQuery = "select (case when a.AcctId = 18875 then 'OPP' when a.AcctId = 28875 then 'TAC' else a.AcctId end) as FundName, a.* from Primebrokerfiles.EDFFundDet a";
        public string GetEDFPositionDetailsQuery = "select (case when a.AcctId = 18875 then 'OPP' when a.AcctId = 28875 then 'TAC' else a.AcctId end) as FundName, a.* from Primebrokerfiles.EDFPosDet a";
        public string GetEDFPrelimDetailsQuery = "select (case when a.Acct = 18875 then 'OPP' when a.Acct = 28875 then 'TAC' else a.Acct end) as FundName, a.* from Primebrokerfiles.EDFPrelimDet a";
        public string GetEDFPfdfDetailsQuery = "select (case when a.Acct = 18875 then 'OPP' when a.Acct = 28875 then 'TAC' else a.Acct end) as FundName, a.* from Primebrokerfiles.EDFPfdfDet a";

        //Fund Summary
        private const string GetFundSummaryQuery = "Primebrokerfiles.spGetFundSummary";

        //BMO Report Queries
        public string GetBMOTradeDetailsQuery = "select (case when a.AcctId = 'ALMOF' then 'OPP' when a.AcctId = 'ALMTF' then 'TAC' end) as Fund, a.* from Primebrokerfiles.BMOTradeDet a";
        public string GetBMOMTMDetailsQuery = "select (case when a.AcctId = 'ALMOF' then 'OPP' when a.AcctId = 'ALMTF' then 'TAC' end) as Fund, a.* from Primebrokerfiles.BMOMTMDet a";
        public string GetBMOTradeRTDDetailsQuery = "select (case when a.AcctId = 'ALMOF' then 'OPP' when a.AcctId = 'ALMTF' then 'TAC' end) as Fund, a.* from Primebrokerfiles.BMOTradeRTDDet a";
        public string GetBMOUnwindDetailsQuery = "select (case when a.AcctId = 'ALMOF' then 'OPP' when a.AcctId = 'ALMTF' then 'TAC' end) as Fund, a.* from Primebrokerfiles.BMOUnwindDet a";
        public string GetBMOPosExpDetailsQuery = "select (case when a.AcctId = 'Almitas Opportunity Fund' then 'OPP' when a.AcctId = 'Almitas Tactical Fund' then 'TAC' end) as Fund, a.* from Primebrokerfiles.BMOPosExp a";

        //UBS Report Queries
        public string GetUBSSwapTradeDetailsQuery = "select (case when a.AcctId = 510101295 then 'OPP' when a.AcctId = 410101294 then 'TAC' else a.AcctId end) as FundName, a.* from Primebrokerfiles.UBSSwapTradeDet a";
        public string GetUBSOpenTransactionDetailsQuery = "select (case when a.RefAcctId = 510101295 then 'OPP' when a.RefAcctId = 410101294 then 'TAC' else a.RefAcctId end) as FundName, a.* from Primebrokerfiles.UBSOpenTransactionDet a";
        public string GetUBSOpenPositionDetailsQuery = "select (case when a.RefAcctId = 510101295 then 'OPP' when a.RefAcctId = 410101294 then 'TAC' else a.RefAcctId end) as FundName, a.* from Primebrokerfiles.UBSNetOpenPositionDet a";
        public string GetUBSValuationDetailsQuery = "select (case when a.RefAcctId = 510101295 then 'OPP' when a.RefAcctId = 410101294 then 'TAC' else a.RefAcctId end) as FundName, a.* from Primebrokerfiles.UBSValuationDet a";
        public string GetUBSValuationSummaryDetailsQuery = "select (case when a.RefAcctId = 510101295 then 'OPP' when a.RefAcctId = 410101294 then 'TAC' else a.RefAcctId end) as FundName, a.* from Primebrokerfiles.UBSValuationSummaryDet a";
        public string GetUBSMarginDetailsQuery = "select (case when a.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when a.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else a.AcctName end) as FundName, a.* from Primebrokerfiles.UBSMarginDet a";
        public string GetUBSCrossMarginDetailsQuery = "select (case when a.AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when a.AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else a.AcctName end) as FundName, a.* from Primebrokerfiles.UBSCrossMarginDet a";


        //Morgan Stanley Queries
        public string GetMSPositionDetailsQuery = "select (case when MainAcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when MainAcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else MainAcctName end) as FundName, a.* from Primebrokerfiles.MSPositionDet a";
        public string GetMSFundDetailsQuery = "select (case when AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else AcctName end) as FundName, a.* from Primebrokerfiles.MSFundDet a";
        public string GetMSTaxLotDetailsQuery = "select (case when SwapAcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when SwapAcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else AcctName end) as FundName, a.* from Primebrokerfiles.MSTaxLotDet a";
        public string GetMSTradeHistDetailsQuery = "select (case when AcctName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' when AcctName = 'ALMITAS TACTICAL FUND LP' then 'TAC' else AcctName end) as FundName, a.* from Primebrokerfiles.MSTradeHistDet a";

        //ALM Fx Fwd Postion Queries
        public string GetALMFxFwdPosQuery = "select * from Primebrokerfiles.ALMFXFwdPosDet";

        //BAML Queries
        private string GetBAMLPerformanceDetailsQuery = "select * from Primebrokerfiles.BAMLPerfDet";
        private string GetBAMLSwapActivityDetailsQuery = "select * from Primebrokerfiles.BAMLSwapActivityDet";
    }
}