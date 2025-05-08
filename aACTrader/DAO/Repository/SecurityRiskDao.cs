using aACTrader.DAO.Interface;
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
    public class SecurityRiskDao : ISecurityRiskDao
    {
        private readonly ILogger<SecurityRiskDao> _logger;
        private const string DATEFORMAT = "yyyy-MM-dd";

        public SecurityRiskDao(ILogger<SecurityRiskDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing SecurityRiskDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, SecurityRiskFactor> GetSecurityRiskFactors()
        {
            IDictionary<string, SecurityRiskFactor> dict = new Dictionary<string, SecurityRiskFactor>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityRiskFactorsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityRiskFactor data = new SecurityRiskFactor
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Duration = (reader.IsDBNull(reader.GetOrdinal("Duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Duration")),
                                    RiskBeta = (reader.IsDBNull(reader.GetOrdinal("RiskBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RiskBeta")),
                                    DurationBeta = (reader.IsDBNull(reader.GetOrdinal("DurationBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DurationBeta")),
                                    DiscountBeta = (reader.IsDBNull(reader.GetOrdinal("DiscountBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountBeta"))
                                };
                                if (!dict.ContainsKey(data.Ticker))
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

        public IDictionary<string, SecurityRiskFactor> GetSecurityRiskFactorsWithDates()
        {
            IDictionary<string, SecurityRiskFactor> dict = new Dictionary<string, SecurityRiskFactor>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityRiskFactorsWithDatesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityRiskFactor data = new SecurityRiskFactor
                                {
                                    Ticker = reader["Ticker"] as string,
                                    RiskBeta = (reader.IsDBNull(reader.GetOrdinal("RiskBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RiskBeta")),
                                    RiskBetaStartDate = reader.IsDBNull(reader.GetOrdinal("RiskBetaStartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RiskBetaStartDate")),
                                    RiskBetaEndDate = reader.IsDBNull(reader.GetOrdinal("RiskBetaEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RiskBetaEndDate")),
                                    RiskBetaFactorType = reader["RiskBetaFactorType"] as string,
                                    UserId = reader["UserId"] as string,
                                    LastModifyDate = reader.IsDBNull(reader.GetOrdinal("ModifyDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ModifyDate")),
                                    CreateDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                };
                                if (!dict.ContainsKey(data.Ticker))
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveSecurityRiskFactors(IList<SecurityRiskFactor> list)
        {
            StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgSecRiskFactorHist " +
                    "(FIGI, Ticker, EffectiveDate, Duration, RiskBeta, DurationBeta, DiscountBeta) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (list != null && list.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgSecRiskFactorHist");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgSecRiskFactorHist";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgSecRiskFactorHist");

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (SecurityRiskFactor data in list)
                            {
                                if (!string.IsNullOrEmpty(data.FIGI))
                                    sb.Append(string.Concat("'", data.FIGI, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.EffectiveDateAsString))
                                    sb.Append(string.Concat("'", data.EffectiveDateAsString, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Duration.HasValue)
                                    sb.Append(data.Duration).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.RiskBeta.HasValue)
                                    sb.Append(data.RiskBeta).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.DurationBeta.HasValue)
                                    sb.Append(data.DurationBeta).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.DiscountBeta.HasValue)
                                    sb.Append(data.DiscountBeta);
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            using (MySqlCommand command = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                command.CommandType = CommandType.Text;
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.SecRiskFactorHist");
                            string sql = "almitasc_ACTradingBBGData.spPopulateSecurityRiskFactors";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundTicker"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<SecurityReturn> GetSecurityReturns(string fundTicker, string ticker)
        {
            IList<SecurityReturn> securityPriceList = new List<SecurityReturn>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityReturnsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_FundTicker", fundTicker);
                        command.Parameters.AddWithValue("p_Ticker", ticker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityReturn data = new SecurityReturn
                                {
                                    FundTicker = reader["FundTicker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    PortDate = (reader.IsDBNull(reader.GetOrdinal("PortDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortDate")),
                                    NavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    SecurityName = reader["SecurityName"] as string,
                                    FundCurrency = reader["FundCurrency"] as string,
                                    SecurityCurrency = reader["SecurityCurrency"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    SecurityRtnLocal = (reader.IsDBNull(reader.GetOrdinal("SecurityRtnLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityRtnLocal")),
                                    SecurityRtn = (reader.IsDBNull(reader.GetOrdinal("SecurityRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityRtn")),
                                    FXRtn = (reader.IsDBNull(reader.GetOrdinal("FXRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRtn")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    SecurityFXRate = (reader.IsDBNull(reader.GetOrdinal("SecurityFXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityFXRate")),
                                    SecurityFXRtn = (reader.IsDBNull(reader.GetOrdinal("SecurityFXRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityFXRtn")),
                                    FundFXRate = (reader.IsDBNull(reader.GetOrdinal("FundFXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundFXRate")),
                                    FundFXRtn = (reader.IsDBNull(reader.GetOrdinal("FundFXRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundFXRtn")),
                                    ReportedPosition = (reader.IsDBNull(reader.GetOrdinal("ReportedPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPosition")),
                                    ReportedPrice = (reader.IsDBNull(reader.GetOrdinal("ReportedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPrice")),
                                    ReportedWeight = (reader.IsDBNull(reader.GetOrdinal("ReportedWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedWeight"))
                                };
                                data.PortDateAsString = DateUtils.ConvertDate(data.PortDate, "yyyy-MM-dd");
                                data.NavDateAsString = DateUtils.ConvertDate(data.NavDate, "yyyy-MM-dd");
                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                securityPriceList.Add(data);
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
            return securityPriceList;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<FundSecurityTotalReturn> GetSecurityTotalReturns()
        {
            IList<FundSecurityTotalReturn> list = new List<FundSecurityTotalReturn>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityTotalReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSecurityTotalReturn data = new FundSecurityTotalReturn
                                {
                                    FundTicker = reader["FundTicker"] as string,
                                    Ticker = reader["Ticker"] as string,

                                    EqCumRtnLocal = (reader.IsDBNull(reader.GetOrdinal("EqCumRtnNavDateLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCumRtnNavDateLocal")),
                                    FxCumRtn = (reader.IsDBNull(reader.GetOrdinal("FXRateCumRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRateCumRtn")),
                                    EqCumRtnBase = (reader.IsDBNull(reader.GetOrdinal("EqCumRtnNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCumRtnNavDate"))
                                };
                                string id = data.FundTicker + "|" + data.Ticker;
                                data.Id = id;
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
        public IDictionary<string, IList<HistSecurityReturn>> GetHistSecurityReturns()
        {
            IDictionary<string, IList<HistSecurityReturn>> dict = new Dictionary<string, IList<HistSecurityReturn>>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetHistSecurityReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string ticker = reader["Ticker"] as string;

                                HistSecurityReturn data = new HistSecurityReturn
                                {
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    SecurityRtn = (reader.IsDBNull(reader.GetOrdinal("TotalReturn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalReturn"))
                                };

                                IList<HistSecurityReturn> list;
                                if (dict.TryGetValue(ticker, out list))
                                {
                                    list.Add(data);
                                }
                                else
                                {
                                    list = new List<HistSecurityReturn>
                                    {
                                        data
                                    };
                                    dict.Add(ticker, list);
                                }
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, IDictionary<DateTime, HistFXRate>> GetHistFXRates()
        {
            IDictionary<string, IDictionary<DateTime, HistFXRate>> dict = new Dictionary<string, IDictionary<DateTime, HistFXRate>>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetHistFXRatesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string currency = reader["Currency"] as string;

                                HistFXRate data = new HistFXRate
                                {
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("Rate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Rate"))
                                };

                                IDictionary<DateTime, HistFXRate> fxRateDict;
                                if (dict.TryGetValue(currency, out fxRateDict))
                                {
                                    fxRateDict.Add(data.EffectiveDate.GetValueOrDefault(), data);
                                }
                                else
                                {
                                    fxRateDict = new Dictionary<DateTime, HistFXRate>
                                    {
                                        { data.EffectiveDate.GetValueOrDefault(), data }
                                    };
                                    dict.Add(currency, fxRateDict);
                                }
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, SecurityMasterExt> GetSecurityMasterExtDetails()
        {
            IDictionary<string, SecurityMasterExt> dict = new Dictionary<string, SecurityMasterExt>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityExtQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMasterExt data = new SecurityMasterExt
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundGroup = reader["FundGroup"] as string,
                                    RiskBeta = (reader.IsDBNull(reader.GetOrdinal("RiskBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RiskBeta")),
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier")),
                                    Duration = (reader.IsDBNull(reader.GetOrdinal("Duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Duration")),
                                    SortOrder = (reader.IsDBNull(reader.GetOrdinal("SortId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SortId")),
                                    Security13F = reader["Security13F"] as string,
                                    Security1940Act = reader["Security1940Act"] as string,
                                    Level1 = reader["Level1"] as string,
                                    Level2 = reader["Level2"] as string,
                                    Level3 = reader["Level3"] as string,
                                    GeoLevel1 = reader["GeoLevel1"] as string,
                                    GeoLevel2 = reader["GeoLevel2"] as string,
                                    GeoLevel3 = reader["GeoLevel3"] as string,
                                    ExchCode = reader["ExchCode"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    Country = reader["Country"] as string,
                                    CustomLevel1 = reader["CustomLevel1"] as string,
                                    CustomLevel2 = reader["CustomLevel2"] as string,
                                    CustomLevel3 = reader["CustomLevel3"] as string,
                                    CustomLevel4 = reader["CustomLevel4"] as string,
                                    ShOut = (reader.IsDBNull(reader.GetOrdinal("ShOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShOut")),
                                    //FX Exposures
                                    FXExpUSD = (reader.IsDBNull(reader.GetOrdinal("FXExpUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpUSD")),
                                    FXExpCAD = (reader.IsDBNull(reader.GetOrdinal("FXExpCAD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpCAD")),
                                    FXExpGBP = (reader.IsDBNull(reader.GetOrdinal("FXExpGBP"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpGBP")),
                                    FXExpEUR = (reader.IsDBNull(reader.GetOrdinal("FXExpEUR"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpEUR")),
                                    FXExpJPY = (reader.IsDBNull(reader.GetOrdinal("FXExpJPY"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpJPY")),
                                    FXExpRON = (reader.IsDBNull(reader.GetOrdinal("FXExpRON"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpRON")),
                                    FXExpILS = (reader.IsDBNull(reader.GetOrdinal("FXExpILS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpILS")),
                                    FXExpAUD = (reader.IsDBNull(reader.GetOrdinal("FXExpAUD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpAUD")),
                                    FXExpINR = (reader.IsDBNull(reader.GetOrdinal("FXExpINR"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpINR")),
                                    FXExpHKD = (reader.IsDBNull(reader.GetOrdinal("FXExpHKD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpHKD")),
                                    FXExpCHF = (reader.IsDBNull(reader.GetOrdinal("FXExpCHF"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpCHF")),
                                    FXExpMXN = (reader.IsDBNull(reader.GetOrdinal("FXExpMXN"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpMXN")),
                                    FXExpSGD = (reader.IsDBNull(reader.GetOrdinal("FXExpSGD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpSGD")),
                                    FXExpCNY = (reader.IsDBNull(reader.GetOrdinal("FXExpCNY"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpCNY")),
                                    FXExpBRL = (reader.IsDBNull(reader.GetOrdinal("FXExpBRL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpBRL")),
                                    FXExpOthers = (reader.IsDBNull(reader.GetOrdinal("FXExpOthers"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXExpOthers")),
                                };

                                if (!dict.ContainsKey(data.Ticker))
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveSecurityMasterExtDetails(IList<SecurityMasterExt> list)
        {
            StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgSecurityMstExt " +
                    "(Ticker, FundGroup, RiskBeta, Multiplier, Duration, Level1, Level2, Level3, ExchCode, CustomLevel1, CustomLevel2, CustomLevel3, CustomLevel4" +
                    ", GeoLevel1, GeoLevel2, GeoLevel3, ShOut,FXExpUSD, FXExpCAD, FXExpGBP, FXExpEUR, FXExpJPY, FXExpRON, FXExpILS, FXExpAUD, FXExpINR, " +
                    "FXExpHKD, FXExpCHF, FXExpMXN, FXExpSGD, FXExpCNY, FXExpBRL, FXExpOthers ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (list != null && list.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgSecurityMstExt");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgSecurityMstExt";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgSecurityMstExt");

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (SecurityMasterExt data in list)
                            {
                                if (!string.IsNullOrEmpty(data.Ticker))
                                {
                                    //Ticker
                                    if (!string.IsNullOrEmpty(data.Ticker))
                                        sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FundGroup
                                    if (!string.IsNullOrEmpty(data.FundGroup))
                                        sb.Append(string.Concat("'", data.FundGroup, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RiskBeta
                                    if (data.RiskBeta.HasValue)
                                        sb.Append(data.RiskBeta).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Multiplier
                                    if (data.Multiplier.HasValue)
                                        sb.Append(data.Multiplier).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Duration
                                    if (data.Duration.HasValue)
                                        sb.Append(data.Duration).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Level1
                                    if (!string.IsNullOrEmpty(data.Level1))
                                        sb.Append(string.Concat("'", data.Level1, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Level2
                                    if (!string.IsNullOrEmpty(data.Level2))
                                        sb.Append(string.Concat("'", data.Level2, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //Level3
                                    if (!string.IsNullOrEmpty(data.Level3))
                                        sb.Append(string.Concat("'", data.Level3, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ExchCode
                                    if (!string.IsNullOrEmpty(data.ExchCode))
                                        sb.Append(string.Concat("'", data.ExchCode, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //CustomLevel1
                                    if (!string.IsNullOrEmpty(data.CustomLevel1))
                                        sb.Append(string.Concat("'", data.CustomLevel1, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //CustomLevel2
                                    if (!string.IsNullOrEmpty(data.CustomLevel2))
                                        sb.Append(string.Concat("'", data.CustomLevel2, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //CustomLevel3
                                    if (!string.IsNullOrEmpty(data.CustomLevel3))
                                        sb.Append(string.Concat("'", data.CustomLevel3, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //CustomLevel4
                                    if (!string.IsNullOrEmpty(data.CustomLevel4))
                                        sb.Append(string.Concat("'", data.CustomLevel4, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //GeoLevel1
                                    if (!string.IsNullOrEmpty(data.GeoLevel1))
                                        sb.Append(string.Concat("'", data.GeoLevel1, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //GeoLevel2
                                    if (!string.IsNullOrEmpty(data.GeoLevel2))
                                        sb.Append(string.Concat("'", data.GeoLevel2, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //GeoLevel3
                                    if (!string.IsNullOrEmpty(data.GeoLevel3))
                                        sb.Append(string.Concat("'", data.GeoLevel3, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //ShOut
                                    if (data.ShOut.HasValue)
                                        sb.Append(data.ShOut);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FxExpUSD
                                    if (data.FXExpUSD.HasValue)
                                        sb.Append(data.FXExpUSD).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpCAD
                                    if (data.FXExpCAD.HasValue)
                                        sb.Append(data.FXExpCAD).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpGBP
                                    if (data.FXExpGBP.HasValue)
                                        sb.Append(data.FXExpGBP).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpEUR
                                    if (data.FXExpEUR.HasValue)
                                        sb.Append(data.FXExpEUR).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpJPY
                                    if (data.FXExpJPY.HasValue)
                                        sb.Append(data.FXExpJPY).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpRON
                                    if (data.FXExpRON.HasValue)
                                        sb.Append(data.FXExpRON).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpILS
                                    if (data.FXExpILS.HasValue)
                                        sb.Append(data.FXExpILS).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpAUD
                                    if (data.FXExpAUD.HasValue)
                                        sb.Append(data.FXExpAUD).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpINR
                                    if (data.FXExpINR.HasValue)
                                        sb.Append(data.FXExpINR).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpHKD
                                    if (data.FXExpHKD.HasValue)
                                        sb.Append(data.FXExpHKD).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpCHF
                                    if (data.FXExpCHF.HasValue)
                                        sb.Append(data.FXExpCHF).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpMXN
                                    if (data.FXExpMXN.HasValue)
                                        sb.Append(data.FXExpMXN).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpSGD
                                    if (data.FXExpSGD.HasValue)
                                        sb.Append(data.FXExpSGD).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpCNY
                                    if (data.FXExpCNY.HasValue)
                                        sb.Append(data.FXExpCNY).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpBRL
                                    if (data.FXExpBRL.HasValue)
                                        sb.Append(data.FXExpBRL).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //FXExpOthers
                                    if (data.FXExpOthers.HasValue)
                                        sb.Append(data.FXExpOthers);
                                    else
                                        sb.Append("null");

                                    string row = sb.ToString();
                                    Rows.Add(string.Concat("(", row, ")"));
                                    sb.Clear();
                                }
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            using (MySqlCommand command = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                command.CommandType = CommandType.Text;
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.SecurityMstExt");
                            string sql = "almitasc_ACTradingBBGData.spPopulateSecurityMstExtDetails";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public void SaveSecurityRiskFactorsWithDates(IList<SecurityRiskFactor> list)
        {
            StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgSecRiskFactorMst " +
                    "(Ticker, RiskBeta, RiskBetaStartDate, RiskBetaEndDate, RiskBetaFactorType, UserId) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (list != null && list.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgSecRiskFactorMst");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgSecRiskFactorMst";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgSecRiskFactorMst");

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();


                            foreach (SecurityRiskFactor data in list)
                            {
                                if (!"D".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                                {

                                    //Ticker
                                    if (!string.IsNullOrEmpty(data.Ticker))
                                        sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RiskBeta
                                    if (data.RiskBeta.HasValue)
                                        sb.Append(data.RiskBeta).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RiskBetaStartDate
                                    if (data.RiskBetaStartDate.HasValue)
                                        sb.Append(string.Concat("'", DateUtils.ConvertDate(data.RiskBetaStartDate, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RiskBetaEndDate
                                    if (data.RiskBetaEndDate.HasValue)
                                        sb.Append(string.Concat("'", DateUtils.ConvertDate(data.RiskBetaEndDate, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //RiskBetaFactorType
                                    if (!string.IsNullOrEmpty(data.RiskBetaFactorType))
                                        sb.Append(string.Concat("'", data.RiskBetaFactorType, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    //UserId
                                    if (!string.IsNullOrEmpty(data.UserId))
                                        sb.Append(string.Concat("'", data.UserId, "'"));
                                    else
                                        sb.Append("null");

                                    string row = sb.ToString();
                                    Rows.Add(string.Concat("(", row, ")"));
                                    sb.Clear();
                                }
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            using (MySqlCommand command = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                command.CommandType = CommandType.Text;
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.StgSecRiskFactorMst");
                            string sql = "almitasc_ACTradingBBGData.spPopulateSecurityRiskFactorsWithDates";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        private const string DELIMITER = ",";

        private const string GetSecurityRiskFactorsQuery = "select * from almitasc_ACTradingBBGData.SecRiskFactorHist where EffectiveDate = (select max(EffectiveDate) from almitasc_ACTradingBBGData.SecRiskFactorHist)";
        private const string GetSecurityReturnsQuery = "spGetSecTotalReturns";
        private const string GetSecurityTotalReturnsQuery = "call almitasc_ACTradingBBGData.spGetFundSecurityTotalReturns";
        private const string GetHistSecurityReturnsQuery = "select Ticker, EffectiveDate, TotalReturn from almitasc_ACTradingBBGData.PortSecurityRtn";
        private const string GetHistFXRatesQuery = "select fx as Currency, date as EffectiveDate, rate as Rate from almitasc_ACTradingBBGData.globalmarket_fxhistory where date >= '2010-01-01'";
        //private const string GetSecurityExtQuery = "select s.Id, s.Ticker, ifnull(s.FundGroup, 'Other') as FundGroup, ifnull(g.SortId, 10000) as SortId, s.RiskBeta, s.Multiplier, s.Duration, s.Security13F, s.Security1940Act"
        //                                    + ", s.Level1, s.Level2, s.Level3, s.ExchCode, s.SecurityType, s.Country"
        //                                    + ", s.GeoLevel1, s.GeoLevel2, s.GeoLevel3"
        //                                    + ", s.CustomLevel1, s.CustomLevel2, s.CustomLevel3, s.CustomLevel4, s.ShOut from almitasc_ACTradingBBGData.SecurityMstExt s"
        //                                    + " left join almitasc_ACTradingBBGData.SecurityGroup g on (s.FundGroup = g.GroupName)"
        //                                    + " where s.Ticker is not null";
        private const string GetSecurityExtQuery = "call almitasc_ACTradingBBGData.spGetSecurityMstExtDetails";
        private const string GetSecurityRiskFactorsWithDatesQuery = "select * from almitasc_ACTradingBBGData.SecRiskFactorMst order by Ticker";
    }
}
