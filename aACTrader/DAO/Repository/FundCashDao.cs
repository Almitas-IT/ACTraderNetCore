using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Fund;
using aCommons.Utils;
using aCommons.Web;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class FundCashDao : IFundCashDao
    {
        private readonly ILogger<FundCashDao> _logger;
        private const string DELIMITER = ",";

        public FundCashDao(ILogger<FundCashDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing FundCashDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundCashDetail> GetFidelityFundCashDetails(string broker)
        {
            IList<FundCashDetail> list = new List<FundCashDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundCashDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCashDetail data = new FundCashDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Currency = reader["Currency"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    TradeDateCash = (reader.IsDBNull(reader.GetOrdinal("TradeDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCash")),
                                    TradeDateCashUSD = (reader.IsDBNull(reader.GetOrdinal("TradeDateCashUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCashUSD")),
                                    SettleDateCash = (reader.IsDBNull(reader.GetOrdinal("SettleDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateCash")),
                                    SettleDateUSD = (reader.IsDBNull(reader.GetOrdinal("SettleDateCashUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SettleDateCashUSD"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Currency + "|" + data.AssetType;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundDetail> GetFidelityFundDetails(string broker)
        {
            IList<FundDetail> list = new List<FundDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDetail data = new FundDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin")),
                                    Cash = (reader.IsDBNull(reader.GetOrdinal("Cash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cash")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundDetail> GetIBFundDetails(string broker)
        {
            IList<FundDetail> list = new List<FundDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDetail data = new FundDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin")),
                                    Cash = (reader.IsDBNull(reader.GetOrdinal("Cash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cash")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundDetail> GetJefferiesFundDetails(string broker)
        {
            IList<FundDetail> list = new List<FundDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDetail data = new FundDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Cash = (reader.IsDBNull(reader.GetOrdinal("Cash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cash")),
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("TotalMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalMV")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    EquityValue = (reader.IsDBNull(reader.GetOrdinal("Equity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Equity"))
                                };

                                data.MarketValue = (reader.IsDBNull(reader.GetOrdinal("TotalMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalMV"));
                                data.Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav"));
                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundCashDetail> GetJPMFundCashDetails(string broker)
        {
            IList<FundCashDetail> list = new List<FundCashDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundCashDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCashDetail data = new FundCashDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Currency = reader["Currency"] as string,
                                    TradeDateCash = (reader.IsDBNull(reader.GetOrdinal("TradeDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCash")),
                                    TradeDateCashUSD = (reader.IsDBNull(reader.GetOrdinal("TradeDateCashUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCashUSD")),
                                    NetFxContracts = (reader.IsDBNull(reader.GetOrdinal("NetFxContracts"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetFxContracts")),
                                    NetFxContractsUSD = (reader.IsDBNull(reader.GetOrdinal("NetFxContractsUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetFxContractsUSD")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    LongMVUSD = (reader.IsDBNull(reader.GetOrdinal("LongMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMVUSD")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    ShortMVUSD = (reader.IsDBNull(reader.GetOrdinal("ShortMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVUSD"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Currency;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundDetail> GetJPMFundDetails(string broker)
        {
            IList<FundDetail> list = new List<FundDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDetail data = new FundDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    FxMTM = (reader.IsDBNull(reader.GetOrdinal("FxMTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxMTM")),
                                    Cash = (reader.IsDBNull(reader.GetOrdinal("Cash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cash")),
                                    EquityValue = (reader.IsDBNull(reader.GetOrdinal("Equity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Equity"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundCashDetail> GetJefferiesFundCashDetails(string broker)
        {
            IList<FundCashDetail> list = new List<FundCashDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundCashDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCashDetail data = new FundCashDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Currency = reader["Currency"] as string,
                                    AssetType = reader["PositionType"] as string,
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue")),
                                    MarketValueUSD = (reader.IsDBNull(reader.GetOrdinal("MarketValueUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValueUSD"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Currency + "|" + data.AssetType;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundCashDetail> GetIBFundCashDetails(string broker)
        {
            IList<FundCashDetail> list = new List<FundCashDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundCashDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCashDetail data = new FundCashDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Currency = reader["Currency"] as string,
                                    TradeDateCash = (reader.IsDBNull(reader.GetOrdinal("TradeDateCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCash")),
                                    TradeDateCashUSD = (reader.IsDBNull(reader.GetOrdinal("TradeDateCashUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeDateCashUSD"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Currency;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<SecurityMargin> GetJPMSecurityMarginRates(string broker)
        {
            IList<SecurityMargin> list = new List<SecurityMargin>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecurityMarginRatesQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMargin data = new SecurityMargin
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Ticker = reader["SecTicker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue")),
                                    MarginRequirement = (reader.IsDBNull(reader.GetOrdinal("MarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirement")),
                                    MarginPct = (reader.IsDBNull(reader.GetOrdinal("MarginPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginPct"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Ticker;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundMarginAttributionDetail> GetFidelityMarginAttributionDetails(string broker)
        {
            IList<FundMarginAttributionDetail> list = new List<FundMarginAttributionDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetMarginAttributionDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundMarginAttributionDetail data = new FundMarginAttributionDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    GrossMktExposure = (reader.IsDBNull(reader.GetOrdinal("GrossMktExposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossMktExposure")),
                                    IdiosyncraticRisk = (reader.IsDBNull(reader.GetOrdinal("IdiosyncraticRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IdiosyncraticRisk")),
                                    SystematicMove = (reader.IsDBNull(reader.GetOrdinal("SystematicMove"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SystematicMove")),
                                    IndustryGroupRisk = (reader.IsDBNull(reader.GetOrdinal("IndustryGroupRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndustryGroupRisk")),
                                    IntraIndustryRisk = (reader.IsDBNull(reader.GetOrdinal("IntraIndustryRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntraIndustryRisk")),
                                    FirmMinimumRequirement = (reader.IsDBNull(reader.GetOrdinal("FirmMinimumRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FirmMinimumRequirement")),
                                    HouseRequirement = (reader.IsDBNull(reader.GetOrdinal("HouseRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HouseRequirement")),
                                    LineItemRequirement = (reader.IsDBNull(reader.GetOrdinal("LineItemRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LineItemRequirement")),
                                    NonPortMarginRequirement = (reader.IsDBNull(reader.GetOrdinal("NonPortMarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonPortMarginRequirement")),
                                    NetEquityRequirement = (reader.IsDBNull(reader.GetOrdinal("NetEquityRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEquityRequirement"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IDictionary<string, SecurityMargin> GetFidelitySecurityMarginRates(string broker)
        {
            IDictionary<string, SecurityMargin> dict = new Dictionary<string, SecurityMargin>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecurityMarginRatesQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMargin data = new SecurityMargin
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Ticker = reader["Ticker"] as string,
                                    IssuerDesc = reader["IssuerDesc"] as string,
                                    IndustryGroup = reader["IndustryGroup"] as string,
                                    IssuerSpecificRisk = (reader.IsDBNull(reader.GetOrdinal("IssuerSpecificRisk"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IssuerSpecificRisk")),
                                    SystematicMove = (reader.IsDBNull(reader.GetOrdinal("SystematicMove"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SystematicMove")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("ExpAdjGMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpAdjGMV")),
                                    MarginPct = (reader.IsDBNull(reader.GetOrdinal("MarginRequirementPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirementPct")),
                                    MarginRequirement = (reader.IsDBNull(reader.GetOrdinal("MarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirement"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Ticker;
                                if (!dict.ContainsKey(data.Id))
                                    dict.Add(data.Id, data);
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundFinanceDetail> GetJPMFundFinanceDetails(string broker)
        {
            IList<FundFinanceDetail> list = new List<FundFinanceDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundFinanceDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundFinanceDetail data = new FundFinanceDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Currency = reader["Currency"] as string,
                                    ShortCollateralMV = (reader.IsDBNull(reader.GetOrdinal("ShortCollateralMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortCollateralMV")),
                                    BundledFee = (reader.IsDBNull(reader.GetOrdinal("BundledFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BundledFee")),
                                    ExcessMV = (reader.IsDBNull(reader.GetOrdinal("ExcessMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessMV")),
                                    ExcessLoanFee = (reader.IsDBNull(reader.GetOrdinal("ExcessLoanFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExcessLoanFee")),
                                    DebitBalance = (reader.IsDBNull(reader.GetOrdinal("DebitBalance"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitBalance")),
                                    DebitAmount = (reader.IsDBNull(reader.GetOrdinal("DebitAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitAmount")),
                                    DebitPct = (reader.IsDBNull(reader.GetOrdinal("DebitPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitPct")),
                                    CreditBalance = (reader.IsDBNull(reader.GetOrdinal("CreditBalance"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditBalance")),
                                    CreditAmount = (reader.IsDBNull(reader.GetOrdinal("CreditAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditAmount")),
                                    CreditPct = (reader.IsDBNull(reader.GetOrdinal("CreditPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditPct")),
                                    UnreinvProceedsBal = (reader.IsDBNull(reader.GetOrdinal("UnreinvProceedsBal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnreinvProceedsBal")),
                                    UnreinvProceedsInt = (reader.IsDBNull(reader.GetOrdinal("UnreinvProceedsInt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnreinvProceedsInt")),
                                    UnreinvProceedsPct = (reader.IsDBNull(reader.GetOrdinal("UnreinvProceedsPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnreinvProceedsPct"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Currency;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundInterestEarningDetail> GetFidelityFundInterestEarningsDetails(string broker)
        {
            IList<FundInterestEarningDetail> list = new List<FundInterestEarningDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundInterestEarningDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundInterestEarningDetail data = new FundInterestEarningDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Currency = reader["Currency"] as string,
                                    IndexName = reader["IndexName"] as string,
                                    CashBalanceLocal = (reader.IsDBNull(reader.GetOrdinal("CashBalanceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashBalanceLocal")),
                                    ShortMVLocal = (reader.IsDBNull(reader.GetOrdinal("ShortMVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVLocal")),
                                    NetBalanceLocal = (reader.IsDBNull(reader.GetOrdinal("NetBalanceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetBalanceLocal")),
                                    IndexRate = (reader.IsDBNull(reader.GetOrdinal("IndexRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndexRate")),
                                    CreditRate = (reader.IsDBNull(reader.GetOrdinal("CreditRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CreditRate")),
                                    DebitRate = (reader.IsDBNull(reader.GetOrdinal("DebitRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DebitRate")),
                                    IntEarningsLocal = (reader.IsDBNull(reader.GetOrdinal("IntEarningsLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntEarningsLocal")),
                                    IntExpenseLocal = (reader.IsDBNull(reader.GetOrdinal("IntExpenseLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntExpenseLocal")),
                                    NetEarningsLocal = (reader.IsDBNull(reader.GetOrdinal("NetEarningsLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetEarningsLocal")),
                                    BaseNetEarnings = (reader.IsDBNull(reader.GetOrdinal("BaseNetEarnings"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseNetEarnings"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Currency;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundCurrencyDetail> GetJPMCurrencyDetails(string broker)
        {
            IList<FundCurrencyDetail> list = new List<FundCurrencyDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCurrencyDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCurrencyDetail data = new FundCurrencyDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Currency = reader["Currency"] as string,
                                    ContractType = reader["FXContractType"] as string,
                                    ContractDesc = reader["FXContractDesc"] as string,
                                    ContractAmount = (reader.IsDBNull(reader.GetOrdinal("FXContractAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractAmt")),
                                    ContractMV = (reader.IsDBNull(reader.GetOrdinal("FXContractMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMV")),
                                    ContractMTMValue = (reader.IsDBNull(reader.GetOrdinal("FXContractMTMValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMTMValue"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Currency + "|" + data.ContractType;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundSwapDetail> GetJPMFundSwapDetails(string broker)
        {
            IList<FundSwapDetail> list = new List<FundSwapDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundSwapDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSwapDetail data = new FundSwapDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    SwapMTM = (reader.IsDBNull(reader.GetOrdinal("SwapMTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapMTM"))
                                };

                                data.Id = data.FundName + "|" + data.Ticker;
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
        public IList<FundSwapDetail> GetJPMFundSwapDetails()
        {
            IList<FundSwapDetail> list = new List<FundSwapDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundSwapDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", "JPM_BY_FUND");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSwapDetail data = new FundSwapDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    SwapMTM = (reader.IsDBNull(reader.GetOrdinal("SwapMTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapMTM"))
                                };

                                data.Id = data.FundName;
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
        public IList<FundSwapDetail> GetJPMFundSwapDetailsByType()
        {
            IList<FundSwapDetail> list = new List<FundSwapDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundSwapDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", "JPM_BY_FUND_BY_SIDE");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSwapDetail data = new FundSwapDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    Side = reader["LongShortInd"] as string,
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV"))
                                };

                                data.Id = data.FundName + "|" + data.Side;
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
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<FundPositionDetail> GetIBPositionDetails(string broker)
        {
            IList<FundPositionDetail> list = new List<FundPositionDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundPositionDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundPositionDetail data = new FundPositionDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    AssetClass = reader["AssetClass"] as string,
                                    PrimaryCurrency = reader["PrimaryCurrency"] as string,
                                    Side = reader["Side"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    UnderlyingSymbol = reader["UnderlyingSymbol"] as string,
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName + "|" + data.Symbol + "|" + data.AssetClass + data.Side;
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
        public IDictionary<string, SecurityRebateRate> GetSecurityRebateRates()
        {
            IDictionary<string, SecurityRebateRate> dict = new Dictionary<string, SecurityRebateRate>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecurityShortBorrowRateQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string source = reader["Source"] as string;
                                string sourceTicker = reader["SourceTicker"] as string;

                                SecurityRebateRate securityShortBorrowRate;
                                if (dict.TryGetValue(sourceTicker, out securityShortBorrowRate))
                                {
                                    if (source.Equals("JPM", StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        securityShortBorrowRate.JPMEffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"));
                                        securityShortBorrowRate.JPMLocatedQty = (reader.IsDBNull(reader.GetOrdinal("LocatedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocatedQty"));
                                        securityShortBorrowRate.JPMRebateRate = (reader.IsDBNull(reader.GetOrdinal("RebateRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebateRate"));
                                        securityShortBorrowRate.JPMEffectiveDateAsString = DateUtils.ConvertDate(securityShortBorrowRate.JPMEffectiveDate, "yyyy-MM-dd");
                                    }
                                    else if (source.Equals("Fidelity", StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        securityShortBorrowRate.FidelityEffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"));
                                        securityShortBorrowRate.FidelityLocatedQty = (reader.IsDBNull(reader.GetOrdinal("LocatedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocatedQty"));
                                        securityShortBorrowRate.FidelityRebateRate = (reader.IsDBNull(reader.GetOrdinal("RebateRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebateRate"));
                                        securityShortBorrowRate.FidelityEffectiveDateAsString = DateUtils.ConvertDate(securityShortBorrowRate.FidelityEffectiveDate, "yyyy-MM-dd");
                                    }
                                }
                                else
                                {
                                    securityShortBorrowRate = new SecurityRebateRate();
                                    securityShortBorrowRate.SourceTicker = reader["SourceTicker"] as string;
                                    securityShortBorrowRate.Ticker = reader["Ticker"] as string;
                                    securityShortBorrowRate.Cusip = reader["Cusip"] as string;
                                    securityShortBorrowRate.Sedol = reader["Sedol"] as string;
                                    securityShortBorrowRate.SecurityDesc = reader["SecurityDesc"] as string;

                                    if (source.Equals("JPM", StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        securityShortBorrowRate.JPMEffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"));
                                        securityShortBorrowRate.JPMLocatedQty = (reader.IsDBNull(reader.GetOrdinal("LocatedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocatedQty"));
                                        securityShortBorrowRate.JPMRebateRate = (reader.IsDBNull(reader.GetOrdinal("RebateRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebateRate"));
                                        securityShortBorrowRate.JPMEffectiveDateAsString = DateUtils.ConvertDate(securityShortBorrowRate.JPMEffectiveDate, "yyyy-MM-dd");
                                    }
                                    else if (source.Equals("Fidelity", StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        securityShortBorrowRate.FidelityEffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"));
                                        securityShortBorrowRate.FidelityLocatedQty = (reader.IsDBNull(reader.GetOrdinal("LocatedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocatedQty"));
                                        securityShortBorrowRate.FidelityRebateRate = (reader.IsDBNull(reader.GetOrdinal("RebateRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebateRate"));
                                        securityShortBorrowRate.FidelityEffectiveDateAsString = DateUtils.ConvertDate(securityShortBorrowRate.FidelityEffectiveDate, "yyyy-MM-dd");
                                    }

                                    dict.Add(sourceTicker, securityShortBorrowRate);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Security Short Borrow rates");
                throw;
            }

            return dict;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<SecurityActualRebateRate> GetSecurityActualRebateRates()
        {
            IList<SecurityActualRebateRate> list = new List<SecurityActualRebateRate>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecurityRebateRateQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityActualRebateRate data = new SecurityActualRebateRate
                                {
                                    Source = reader["Source"] as string,
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    AccrualDate = reader.IsDBNull(reader.GetOrdinal("AccrualDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AccrualDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    SourceTicker = reader["SourceTicker"] as string,
                                    SecurityDesc = reader["SecurityDesc"] as string,
                                    Currency = reader["Currency"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    LocalClosingMark = (reader.IsDBNull(reader.GetOrdinal("LocalClosingMark"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalClosingMark")),
                                    LocalMarkValue = (reader.IsDBNull(reader.GetOrdinal("LocalMarkValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LocalMarkValue")),
                                    IndexRate = (reader.IsDBNull(reader.GetOrdinal("IndexRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IndexRate")),
                                    RebateRate = (reader.IsDBNull(reader.GetOrdinal("RebateRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RebateRate")),
                                    BaseRebate = (reader.IsDBNull(reader.GetOrdinal("BaseRebate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseRebate"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.AccrualDateAsString = DateUtils.ConvertDate(data.AccrualDate, "yyyy-MM-dd");
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
        public IDictionary<string, SecurityMarginDetail> GetJPMSecurityMarginDetails()
        {
            IDictionary<string, SecurityMarginDetail> dict = new Dictionary<string, SecurityMarginDetail>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetJPMSecurityMarginDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string secTicker = reader["SecTicker"] as string;

                                if (!string.IsNullOrEmpty(secTicker))
                                {
                                    SecurityMarginDetail data = new SecurityMarginDetail
                                    {
                                        FundName = reader["FundName"] as string,
                                        EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                        Ticker = reader["SecTicker"] as string,
                                        Sedol = reader["Sedol"] as string,
                                        MarginRequirement = (reader.IsDBNull(reader.GetOrdinal("MarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirement")),
                                        BaselineAmt = (reader.IsDBNull(reader.GetOrdinal("BaselineEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaselineEquity")),
                                        LiquidityAmt = (reader.IsDBNull(reader.GetOrdinal("AddOnLiquidity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnLiquidity")),
                                        OtherAmt = (reader.IsDBNull(reader.GetOrdinal("Other"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Other")),
                                        MarginRate = (reader.IsDBNull(reader.GetOrdinal("MarginRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRate")),
                                        BaselineRate = (reader.IsDBNull(reader.GetOrdinal("BaselineEquityRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaselineEquityRate")),
                                        LiquidityRate = (reader.IsDBNull(reader.GetOrdinal("AddOnLiquidityRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddOnLiquidityRate")),
                                        OtherRate = (reader.IsDBNull(reader.GetOrdinal("OtherRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OtherRate")),
                                    };

                                    string id = data.FundName + "|" + data.Ticker;
                                    if (!dict.ContainsKey(id))
                                    {
                                        data.Id = id;
                                        dict.Add(id, data);
                                    }
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

        public IList<FundSummaryExpTO> GetFundSummaryExposures()
        {
            IList<FundSummaryExpTO> list = new List<FundSummaryExpTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundSummaryExposuresQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Level", "Summary");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSummaryExpTO data = new FundSummaryExpTO
                                {
                                    Category = reader["Category"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    FundOppValue = (reader.IsDBNull(reader.GetOrdinal("FundOppValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundOppValue")),
                                    FundTacValue = (reader.IsDBNull(reader.GetOrdinal("FundTacValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundTacValue")),
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

        public IList<FundDetailExpTO> GetFundDetailExposures()
        {
            IList<FundDetailExpTO> list = new List<FundDetailExpTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDetailExposuresQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Level", "Detail");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDetailExpTO data = new FundDetailExpTO
                                {
                                    Category = reader["Category"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    //Opportunity Fund
                                    FundOppFidelityValue = (reader.IsDBNull(reader.GetOrdinal("FundOppFidelityValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundOppFidelityValue")),
                                    FundOppJPMValue = (reader.IsDBNull(reader.GetOrdinal("FundOppJPMValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundOppJPMValue")),
                                    FundOppIBValue = (reader.IsDBNull(reader.GetOrdinal("FundOppIBValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundOppIBValue")),
                                    //Tactical Fund
                                    FundTacFidelityValue = (reader.IsDBNull(reader.GetOrdinal("FundTacFidelityValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundTacFidelityValue")),
                                    FundTacJPMValue = (reader.IsDBNull(reader.GetOrdinal("FundTacJPMValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundTacJPMValue")),
                                    FundTacIBValue = (reader.IsDBNull(reader.GetOrdinal("FundTacIBValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundTacIBValue")),
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

        public void SaveFundSummaryExposures(IList<FundSummaryExp> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGLink.FundSummaryExp"
                    + " (FundName, EffectiveDate, Category, CategoryValue) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGLink.FundSummaryExp for EffectiveDate = '" + list[0].EffectiveDateAsString + "'");
                        string sqlDelete = "delete from almitasc_ACTradingBBGLink.FundSummaryExp where EffectiveDate = '" + list[0].EffectiveDateAsString + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (FundSummaryExp data in list)
                        {
                            // FundName
                            if (!string.IsNullOrEmpty(data.FundName))
                                sb.Append(string.Concat("'", data.FundName, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // EffectiveDate
                            if (!string.IsNullOrEmpty(data.EffectiveDateAsString))
                                sb.Append(string.Concat("'", data.EffectiveDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Category
                            if (!string.IsNullOrEmpty(data.Category))
                                sb.Append(string.Concat("'", data.Category, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // CategoryValue
                            if (data.CategoryValue.HasValue)
                                sb.Append(data.CategoryValue);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
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
                _logger.LogError(ex, "Error saving Fund Summary details into database");
                throw;
            }
        }

        public void SaveFundDetailExposures(IList<FundDetailExp> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGLink.FundDetailExp"
                    + " (FundName, EffectiveDate, Broker, Category, CategoryValue) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGLink.FundDetailExp for EffectiveDate = '" + list[0].EffectiveDateAsString + "'");
                        string sqlDelete = "delete from almitasc_ACTradingBBGLink.FundDetailExp where EffectiveDate = '" + list[0].EffectiveDateAsString + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (FundDetailExp data in list)
                        {
                            // FundName
                            if (!string.IsNullOrEmpty(data.FundName))
                                sb.Append(string.Concat("'", data.FundName, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // EffectiveDate
                            if (!string.IsNullOrEmpty(data.EffectiveDateAsString))
                                sb.Append(string.Concat("'", data.EffectiveDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Broker
                            if (!string.IsNullOrEmpty(data.Broker))
                                sb.Append(string.Concat("'", data.Broker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Category
                            if (!string.IsNullOrEmpty(data.Category))
                                sb.Append(string.Concat("'", data.Category, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // CategoryValue
                            if (data.CategoryValue.HasValue)
                                sb.Append(data.CategoryValue);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
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
                _logger.LogError(ex, "Error saving Fund details into database");
                throw;
            }
        }

        public IList<FundDetail> GetEDFFundDetails(string broker)
        {
            IList<FundDetail> list = new List<FundDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDetail data = new FundDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin")),
                                    Cash = (reader.IsDBNull(reader.GetOrdinal("Cash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cash")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.Id = data.FundName;
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

        public IDictionary<string, JPMSecurityMarginDetail> GetLatestJPMSecurityMarginDetails()
        {
            IDictionary<string, JPMSecurityMarginDetail> dict = new Dictionary<string, JPMSecurityMarginDetail>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetLatestJPMSecurityMarginDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string secTicker = reader["SecTicker"] as string;

                                JPMSecurityMarginDetail data = new JPMSecurityMarginDetail
                                {
                                    FundName = reader["FundName"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    IssuerDesc = reader["IssuerDesc"] as string,
                                    Industry = reader["Industry"] as string,
                                    Country = reader["Country"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    SecType = reader["SecTypeDesc"] as string,
                                    Currency = reader["Currency"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    DeltaAdjMV = (reader.IsDBNull(reader.GetOrdinal("DeltaAdjMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DeltaAdjMV")),
                                    MarginRule = reader["MarginRule"] as string,
                                    MarginRequirement = (reader.IsDBNull(reader.GetOrdinal("MarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarginRequirement")),
                                    TotalMarginRequirement = (reader.IsDBNull(reader.GetOrdinal("TotalMarginRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalMarginRequirement")),
                                    MinRequirementDesc = reader["MinRequirementDesc"] as string,
                                    MinRequirement = (reader.IsDBNull(reader.GetOrdinal("MinRequirement"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MinRequirement")),
                                    AppliedReq = reader["AppliedReq"] as string,
                                    AppliedReqDesc = reader["AppliedReqDesc"] as string,
                                    Measure1 = reader["Measure1"] as string,
                                    Measure1Desc = reader["Measure1Desc"] as string,
                                    Measure2 = reader["Measure2"] as string,
                                    Measure2Desc = reader["Measure2Desc"] as string,
                                    Measure3 = reader["Measure3"] as string,
                                    Measure3Desc = reader["Measure3Desc"] as string,
                                    LongRiskRate = (reader.IsDBNull(reader.GetOrdinal("LongRiskRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongRiskRate")),
                                    ShortRiskRate = (reader.IsDBNull(reader.GetOrdinal("ShortRiskRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortRiskRate")),
                                };

                                string rule = data.MarginRule;
                                if (string.IsNullOrEmpty(rule))
                                    rule = data.SecDesc;

                                data.ALMTicker = secTicker;
                                //string id = data.FundName + "|" + data.ALMTicker + "|" + data.SecType + "|" + rule;
                                string id = data.FundName + "|" + data.ALMTicker + "|" + rule;
                                if (!dict.ContainsKey(id))
                                {
                                    data.Id = id;
                                    dict.Add(id, data);
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

        public IDictionary<string, FundCurrencyExposureTO> GetFundCurrencyExposures(string broker)
        {
            IDictionary<string, FundCurrencyExposureTO> dict = new Dictionary<string, FundCurrencyExposureTO>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCurrencyExposuresDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string fundName = reader["FundName"] as string;
                                string currency = reader["Currency"] as string;
                                string fxContractDesc = reader["FXContractDesc"] as string;
                                DateTime? fxContractExpDt = (reader.IsDBNull(reader.GetOrdinal("FXContractExpDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FXContractExpDate"));

                                string id = currency + "|" + fxContractDesc + "|" + fxContractExpDt.GetValueOrDefault().ToString("yyyyMMdd");
                                if (!dict.TryGetValue(id, out FundCurrencyExposureTO data))
                                {
                                    data = new FundCurrencyExposureTO
                                    {
                                        Curr = currency,
                                        FxContractDesc = fxContractDesc,
                                        FxContractExpDt = fxContractExpDt,
                                        EffectiveDt = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                        FxContractType = reader["FXContractType"] as string,
                                        CurrDesc = reader["CurrencyDesc"] as string,
                                    };
                                    dict.Add(id, data);
                                }

                                if (fundName.Equals("Opp", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    data.FxContractAmtOpp = (reader.IsDBNull(reader.GetOrdinal("FXContractAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractAmt"));
                                    data.FxContractMVOpp = (reader.IsDBNull(reader.GetOrdinal("FXContractMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMV"));
                                    data.FxContractMTMValueOpp = (reader.IsDBNull(reader.GetOrdinal("FXContractMTMValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMTMValue"));
                                }
                                else if (fundName.Equals("Tac", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    data.FxContractAmtTac = (reader.IsDBNull(reader.GetOrdinal("FXContractAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractAmt"));
                                    data.FxContractMVTac = (reader.IsDBNull(reader.GetOrdinal("FXContractMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMV"));
                                    data.FxContractMTMValueTac = (reader.IsDBNull(reader.GetOrdinal("FXContractMTMValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXContractMTMValue"));
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

        private const string GetFundDetailsQuery = "spGetFundDetails";
        private const string GetFundCashDetailsQuery = "spGetFundCashDetails";
        private const string GetFundFinanceDetailsQuery = "spGetFundFinanceDetails";
        private const string GetFundInterestEarningDetailsQuery = "spGetFundInterestEarningDetails";
        private const string GetSecurityBorrowRatesQuery = "spGetSecurityBorrowRates";
        private const string GetSecurityMarginRatesQuery = "spGetSecurityMarginRates";
        private const string GetMarginAttributionDetailsQuery = "spGetFundMarginAttributionDetail";
        private const string GetCurrencyDetailsQuery = "spGetFundCurrencyDetails";
        private const string GetCurrencyExposuresDetailsQuery = "spGetFundCurrencyExpDetails";
        private const string GetFundSwapDetailsQuery = "spGetFundSwapDetails";
        private const string GetFundPositionDetailsQuery = "spGetFundBrokerPositionDetails";
        private const string GetSecurityShortBorrowRateQuery = "call almitasc_ACTradingBBGData.spGetSecurityShortBorrowRates";
        private const string GetSecurityRebateRateQuery = "call almitasc_ACTradingBBGData.spGetSecurityRebateRates";
        private const string GetJPMSecurityMarginDetailsQuery = "spGetSecurityMarginDetails";
        private const string GetLatestJPMSecurityMarginDetailsQuery = "spGetJPMMarginDetails";
        private const string GetFundSummaryExposuresQuery = "spGetFundExposures";
        private const string GetFundDetailExposuresQuery = "spGetFundExposures";
    }
}