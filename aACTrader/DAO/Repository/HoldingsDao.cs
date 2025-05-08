using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Fund;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class HoldingsDao : IHoldingsDao
    {
        private readonly ILogger<HoldingsDao> _logger;
        private const string DELIMITER = ",";

        public HoldingsDao(ILogger<HoldingsDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing HoldingsDao...");
        }

        /// <summary>
        ///  
        /// </summary>
        /// <param name="activistName"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<CompanyHoldings> GetActivistHoldings(string activistName, DateTime startDate, DateTime endDate)
        {
            IList<CompanyHoldings> list = new List<CompanyHoldings>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetActivistHoldingsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_CompanyName", activistName);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CompanyHoldings data = new CompanyHoldings
                                {
                                    DisplayTicker = reader["DisplayTicker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    IssuerName = reader["IssuerName"] as string,
                                    SecurityClass = reader["SecurityClass"] as string,
                                    ReportQuarter = reader["ReportQuarter"] as string,
                                    ReportDate = reader.IsDBNull(reader.GetOrdinal("ReportDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportDate")),
                                    SharesHeld = (reader.IsDBNull(reader.GetOrdinal("SharesHeld"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesHeld")),
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("SharesOutstanding"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesOutstanding"))
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
        /// <param name="holder"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<ActivistHolding> GetActivistHoldings(string holder, string ticker)
        {
            IList<ActivistHolding> list = new List<ActivistHolding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetActivistHoldersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Holder", string.IsNullOrEmpty(holder) ? null : holder);
                        command.Parameters.AddWithValue("p_Ticker", string.IsNullOrEmpty(ticker) ? null : ticker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ActivistHolding data = new ActivistHolding
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Holder = reader["Holder"] as string,
                                    HolderPortfolio = reader["HolderPortfolio"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    PositionChange = (reader.IsDBNull(reader.GetOrdinal("PositionChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PositionChange")),
                                    FilingDate = reader.IsDBNull(reader.GetOrdinal("FilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FilingDate")),
                                    FilingSource = reader["FilingSource"] as string,
                                    InsiderStatus = reader["InsiderStatus"] as string,
                                    PctOutstanding = (reader.IsDBNull(reader.GetOrdinal("PctOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctOutstanding")),
                                    PctPortfolio = (reader.IsDBNull(reader.GetOrdinal("PctPortfolio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctPortfolio")),
                                    HolderInstitutionType = reader["HolderInstitutionType"] as string,
                                    Country = reader["Country"] as string,
                                    ActivistScore = (reader.IsDBNull(reader.GetOrdinal("ActivistScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScore")),
                                    ActivistShortName = reader["ActivistShortName"] as string,
                                    HolderType = reader["HolderType"] as string,
                                    Comments = reader["Comments"] as string
                                };
                                data.FilingDateAsString = DateUtils.ConvertDate(data.FilingDate, "yyyy-MM-dd");
                                data.LastNavDateAsString = DateUtils.ConvertDate(data.LastNavDate, "yyyy-MM-dd");
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

        public IList<ActivistSummaryTO> GetActivistSummary()
        {
            IList<ActivistSummaryTO> list = new List<ActivistSummaryTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetActivistSummaryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ActivistSummaryTO data = new ActivistSummaryTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Country = reader["Country"] as string,
                                    MinFilingDate = (reader.IsDBNull(reader.GetOrdinal("MinFilingDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinFilingDate")),
                                    MaxFilingDate = (reader.IsDBNull(reader.GetOrdinal("MaxFilingDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxFilingDate")),
                                    ALMPos = (reader.IsDBNull(reader.GetOrdinal("ALMPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMPos")),
                                    ALMPosHeld = (reader.IsDBNull(reader.GetOrdinal("ALMPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMPosHeld")),
                                    SabaPos = (reader.IsDBNull(reader.GetOrdinal("SabaPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SabaPos")),
                                    SabaPosHeld = (reader.IsDBNull(reader.GetOrdinal("SabaPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SabaPosHeld")),
                                    KarpusPos = (reader.IsDBNull(reader.GetOrdinal("KarpusPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("KarpusPos")),
                                    KarpusPosHeld = (reader.IsDBNull(reader.GetOrdinal("KarpusPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("KarpusPosHeld")),
                                    BulldogPos = (reader.IsDBNull(reader.GetOrdinal("BulldogPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BulldogPos")),
                                    BulldogPosHeld = (reader.IsDBNull(reader.GetOrdinal("BulldogPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BulldogPosHeld")),
                                    CP1607Pos = (reader.IsDBNull(reader.GetOrdinal("CP1607Pos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CP1607Pos")),
                                    CP1607PosHeld = (reader.IsDBNull(reader.GetOrdinal("CP1607PosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CP1607PosHeld")),
                                    SITPos = (reader.IsDBNull(reader.GetOrdinal("SITPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SITPos")),
                                    SITPosHeld = (reader.IsDBNull(reader.GetOrdinal("SITPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SITPosHeld")),
                                    RVPPos = (reader.IsDBNull(reader.GetOrdinal("RVPPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RVPPos")),
                                    RVPPosHeld = (reader.IsDBNull(reader.GetOrdinal("RVPPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RVPPosHeld")),
                                    WeissPos = (reader.IsDBNull(reader.GetOrdinal("WeissPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WeissPos")),
                                    WeissPosHeld = (reader.IsDBNull(reader.GetOrdinal("WeissPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WeissPosHeld")),
                                    COLPos = (reader.IsDBNull(reader.GetOrdinal("COLPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("COLPos")),
                                    COLPosHeld = (reader.IsDBNull(reader.GetOrdinal("COLPosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("COLPosHeld")),
                                    WolverinePos = (reader.IsDBNull(reader.GetOrdinal("WolverinePos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WolverinePos")),
                                    WolverinePosHeld = (reader.IsDBNull(reader.GetOrdinal("WolverinePosHeld"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WolverinePosHeld")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Activist Summary query");
                throw;
            }

            return list;
        }

        public IList<ActivistMasterTO> GetActivistMaster()
        {
            IList<ActivistMasterTO> list = new List<ActivistMasterTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetActivistMasterQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.Text;

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ActivistMasterTO data = new ActivistMasterTO
                                {
                                    Holder = reader["Holder"] as string,
                                    ShortName = reader["ShortName"] as string,
                                    InstitutionType = reader["InstitutionType"] as string,
                                    HolderGroup = reader["HolderGroup"] as string,
                                    Score = (reader.IsDBNull(reader.GetOrdinal("Score"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Score")),
                                    Comments = reader["Comments"] as string,
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),

                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Activist Master query");
                throw;
            }
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="holder"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<ActivistHolding> GetActivistHoldingsHistory(string holder, string ticker)
        {
            IList<ActivistHolding> list = new List<ActivistHolding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetActivistHoldersHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Holder", holder);
                        command.Parameters.AddWithValue("p_Ticker", ticker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ActivistHolding data = new ActivistHolding
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Holder = reader["Holder"] as string,
                                    HolderPortfolio = reader["HolderPortfolio"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    PositionChange = (reader.IsDBNull(reader.GetOrdinal("PositionChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PositionChange")),
                                    FilingDate = reader.IsDBNull(reader.GetOrdinal("FilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FilingDate")),
                                    FilingSource = reader["FilingSource"] as string,
                                    InsiderStatus = reader["InsiderStatus"] as string,
                                    PctOutstanding = (reader.IsDBNull(reader.GetOrdinal("PctOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctOutstanding")),
                                    PctPortfolio = (reader.IsDBNull(reader.GetOrdinal("PctPortfolio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctPortfolio")),
                                    HolderInstitutionType = reader["HolderInstitutionType"] as string,
                                    Country = reader["Country"] as string,
                                    ActivistScore = (reader.IsDBNull(reader.GetOrdinal("ActivistScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScore")),
                                    ActivistShortName = reader["ActivistShortName"] as string,
                                    HolderType = reader["HolderType"] as string,
                                    Comments = reader["Comments"] as string,
                                    AsofDate = reader.IsDBNull(reader.GetOrdinal("AsofDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsofDate"))
                                };
                                data.FilingDateAsString = DateUtils.ConvertDate(data.FilingDate, "yyyy-MM-dd");
                                data.AsofDateAsString = DateUtils.ConvertDate(data.AsofDate, "yyyy-MM-dd");
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
        public IDictionary<string, ActivistHolding> GetActivistHoldings()
        {
            IDictionary<string, ActivistHolding> map = new Dictionary<string, ActivistHolding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetActivistHoldersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Holder", null);
                        command.Parameters.AddWithValue("p_Ticker", null);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ActivistHolding data = new ActivistHolding
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Holder = reader["Holder"] as string,
                                    HolderPortfolio = reader["HolderPortfolio"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    PositionChange = (reader.IsDBNull(reader.GetOrdinal("PositionChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PositionChange")),
                                    FilingDate = reader.IsDBNull(reader.GetOrdinal("FilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FilingDate")),
                                    FilingSource = reader["FilingSource"] as string,
                                    InsiderStatus = reader["InsiderStatus"] as string,
                                    PctOutstanding = (reader.IsDBNull(reader.GetOrdinal("PctOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctOutstanding")),
                                    PctPortfolio = (reader.IsDBNull(reader.GetOrdinal("PctPortfolio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctPortfolio")),
                                    HolderInstitutionType = reader["HolderInstitutionType"] as string,
                                    Country = reader["Country"] as string,
                                    ActivistScore = (reader.IsDBNull(reader.GetOrdinal("ActivistScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScore")),
                                    ActivistShortName = reader["ActivistShortName"] as string,
                                    HolderType = reader["HolderType"] as string,
                                    Comments = reader["Comments"] as string
                                };

                                data.FilingDateAsString = DateUtils.ConvertDate(data.FilingDate, "yyyy-MM-dd");
                                data.LastNavDateAsString = DateUtils.ConvertDate(data.LastNavDate, "yyyy-MM-dd");

                                string key = data.Ticker + "|" + data.Holder;
                                data.HolderKey = key;

                                if (!map.ContainsKey(key))
                                    map.Add(key, data);
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

            return map;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="country"></param>
        /// <returns></returns>
        public IList<ActivistScore> GetActivistScores(string country)
        {
            IList<ActivistScore> list = new List<ActivistScore>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetActivistScoresQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Country", country);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ActivistScore data = new ActivistScore
                                {
                                    Holder = reader["Holder"] as string,
                                    InstitutionType = reader["InstitutionType"] as string,
                                    Score = (reader.IsDBNull(reader.GetOrdinal("Score"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Score")),
                                    ShortName = reader["ShortName"] as string,
                                    Comments = reader["Comments"] as string,
                                    Country = reader["Country"] as string,
                                    FundsOwned = (reader.IsDBNull(reader.GetOrdinal("FundsOwned"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FundsOwned")),
                                    MktValueUSD = (reader.IsDBNull(reader.GetOrdinal("MktValueUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueUSD")),
                                    MktValueLocal = (reader.IsDBNull(reader.GetOrdinal("MktValueLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueLocal")),
                                    MktValueUSDWtdPD = (reader.IsDBNull(reader.GetOrdinal("MktValueUSDWtdPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueUSDWtdPD")),
                                    SharesOwned = (reader.IsDBNull(reader.GetOrdinal("SharesOwned"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOwned")),
                                    MaxPctOwn = (reader.IsDBNull(reader.GetOrdinal("MaxPctOwn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MaxPctOwn")),
                                    FS13D = (reader.IsDBNull(reader.GetOrdinal("FS13D"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FS13D")),
                                    FS13F = (reader.IsDBNull(reader.GetOrdinal("FS13F"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FS13F")),
                                    FS13G = (reader.IsDBNull(reader.GetOrdinal("FS13G"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FS13G")),
                                    FSForm3 = (reader.IsDBNull(reader.GetOrdinal("FSForm3"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSForm3")),
                                    FSForm4 = (reader.IsDBNull(reader.GetOrdinal("FSForm4"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSForm4")),
                                    FSForm5 = (reader.IsDBNull(reader.GetOrdinal("FSForm5"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSForm5")),
                                    FSMFAgg = (reader.IsDBNull(reader.GetOrdinal("FSMFAgg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSMFAgg")),
                                    FSMulti = (reader.IsDBNull(reader.GetOrdinal("FSMulti"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSMulti")),
                                    FSProxy = (reader.IsDBNull(reader.GetOrdinal("FSProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSProxy")),
                                    FSSchD = (reader.IsDBNull(reader.GetOrdinal("FSSchD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSSchD")),
                                    FSSEDI = (reader.IsDBNull(reader.GetOrdinal("FSSEDI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSSEDI")),
                                    FSULTAgg = (reader.IsDBNull(reader.GetOrdinal("FSULTAgg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FSULTAgg")),
                                    MaxFilingDate = reader.IsDBNull(reader.GetOrdinal("MaxFilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxFilingDate")),
                                    MinFilingDate = reader.IsDBNull(reader.GetOrdinal("MinFilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinFilingDate"))
                                };

                                data.MaxFilingDateAsString = DateUtils.ConvertDate(data.MaxFilingDate, "yyyy-MM-dd");
                                data.MinFilingDateAsString = DateUtils.ConvertDate(data.MinFilingDate, "yyyy-MM-dd");

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
        /// <param name="asofDate"></param>
        /// <returns></returns>
        public IList<AlmHolding> GetAlmHoldings(DateTime asofDate)
        {
            IList<AlmHolding> list = new List<AlmHolding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetAlmHoldingsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Date", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                AlmHolding data = new AlmHolding
                                {
                                    Portfolio = reader["Portfolio"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    BBGTicker = reader["BBGTicker"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    SecurityName = reader["SecurityName"] as string,
                                    Security13FFlag = reader["Security13FFlag"] as string,
                                    Currency = reader["Currency"] as string,
                                    AssetType = reader["SecurityType"] as string,
                                    PaymentRank = reader["PaymentRank"] as string,
                                    GeoLevel1 = reader["GeoLevel1"] as string,
                                    GeoLevel2 = reader["GeoLevel2"] as string,
                                    GeoLevel3 = reader["GeoLevel3"] as string,
                                    AssetClassLevel1 = reader["AssetLevel1"] as string,
                                    AssetClassLevel2 = reader["AssetLevel2"] as string,
                                    AssetClassLevel3 = reader["AssetLevel3"] as string,

                                    AsofDate = reader.IsDBNull(reader.GetOrdinal("AsofDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsofDate")),
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    QuoteUnit = (reader.IsDBNull(reader.GetOrdinal("PriceUnit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceUnit")),
                                    PriceMultiplier = (reader.IsDBNull(reader.GetOrdinal("PriceMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMultiplier")),
                                    Factor = (reader.IsDBNull(reader.GetOrdinal("Factor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Factor")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    MktValueLocal = (reader.IsDBNull(reader.GetOrdinal("MktValueLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueLocal")),
                                    MktValueUSD = (reader.IsDBNull(reader.GetOrdinal("MktValueUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueUSD")),
                                    Duration = (reader.IsDBNull(reader.GetOrdinal("Duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Duration")),
                                    DurationExposure = (reader.IsDBNull(reader.GetOrdinal("DurationExposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DurationExposure")),
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("SharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOutstanding"))
                                };
                                data.AsofDateString = DateUtils.ConvertToDate(data.AsofDate);
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
        public IList<AlmHolding> GetAlmHoldingsSharesRecall()
        {
            IList<AlmHolding> list = new List<AlmHolding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetAlmHoldingsSharesRecallQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                AlmHolding data = new AlmHolding
                                {
                                    Portfolio = reader["Portfolio"] as string,
                                    Broker = reader["Broker"] as string,
                                    BrokerSystemId = reader["BrokerSystemId"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    SecurityName = reader["SecurityName"] as string,
                                    TaxStatus = reader["TaxStatus"] as string,
                                    FundCategory = reader["FundCategory"] as string,
                                    DividendFrequency = reader["DvdFrequency"] as string,
                                    Currency = reader["Currency"] as string,
                                    InOpportunityFund = reader["FundOpt"] as string,
                                    InTacticalFund = reader["FundTac"] as string,
                                    AccountType = reader["AccountType"] as string,

                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    MktValueLocal = (reader.IsDBNull(reader.GetOrdinal("MktValueLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueLocal")),
                                    MktValueUSD = (reader.IsDBNull(reader.GetOrdinal("MktValueUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueUSD")),
                                    QualifyingDividends = (reader.IsDBNull(reader.GetOrdinal("QualifyingDividends"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QualifyingDividends")),
                                    DividendYield = (reader.IsDBNull(reader.GetOrdinal("DvdYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdYield")),
                                    OvrDividendYield = (reader.IsDBNull(reader.GetOrdinal("OvrDvdYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OvrDvdYield")),
                                    DividendExDate = reader.IsDBNull(reader.GetOrdinal("DvdExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdExDate")),
                                    OvrDividendExDate = reader.IsDBNull(reader.GetOrdinal("OvrDvdExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OvrDvdExDate")),
                                    RecallDays = reader.IsDBNull(reader.GetOrdinal("RecallDays")) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RecallDays")),

                                    AsofDate = reader.IsDBNull(reader.GetOrdinal("AsofDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsofDate")),
                                };

                                data.DividendExDateString = DateUtils.ConvertToDate(data.DividendExDate);
                                data.OvrDividendExDateString = DateUtils.ConvertToDate(data.OvrDividendExDate);

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
        /// <param name="reportType"></param>
        /// <returns></returns>
        public IList<FundHolder> GetFundHolders(string reportType)
        {
            IList<FundHolder> list = new List<FundHolder>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundHoldersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_ReportType", reportType);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHolder data = new FundHolder
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundHolderName = reader["FundHolder"] as string,
                                    FilingSource = reader["FilingSource"] as string,

                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("ClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosePrice")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MktValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValue")),
                                    PctOwned = (reader.IsDBNull(reader.GetOrdinal("PctOwned"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctOwned")),
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("SharesOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOut")),
                                    FilingDate = reader.IsDBNull(reader.GetOrdinal("FilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FilingDate")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD"))
                                };

                                data.FilingDateAsString = DateUtils.ConvertDate(data.FilingDate, "yyyy-MM-dd");
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
        /// <param name="reportType"></param>
        /// <returns></returns>
        public IList<FundHolder> GetFundHoldersSummary(string reportType)
        {
            IList<FundHolder> list = new List<FundHolder>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundHoldersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_ReportType", reportType);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHolder data = new FundHolder
                                {
                                    FundHolderName = reader["Holder"] as string,
                                    InstitutionType = reader["InstitutionType"] as string,
                                    NumFundsOwned = (reader.IsDBNull(reader.GetOrdinal("TotalNumberFundsOwned"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalNumberFundsOwned")),
                                    TotalMarketValue = (reader.IsDBNull(reader.GetOrdinal("TotalMktValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalMktValue")),
                                    TotalSharesOwned = (reader.IsDBNull(reader.GetOrdinal("TotalSharesOwned"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalSharesOwned")),
                                    MaxPctOwned = (reader.IsDBNull(reader.GetOrdinal("MaxPctOwned"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MaxPctOwned")),
                                    NumFilings13D = (reader.IsDBNull(reader.GetOrdinal("13D"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("13D")),
                                    NumFilings13F = (reader.IsDBNull(reader.GetOrdinal("13F"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("13F")),
                                    NumFilings13G = (reader.IsDBNull(reader.GetOrdinal("13G"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("13G")),
                                    NumFilingsForm3 = (reader.IsDBNull(reader.GetOrdinal("Form3"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Form3")),
                                    NumFilingsForm4 = (reader.IsDBNull(reader.GetOrdinal("Form4"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Form4")),
                                    NumFilingsForm5 = (reader.IsDBNull(reader.GetOrdinal("Form5"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Form5")),
                                    NumFilingsMFAGG = (reader.IsDBNull(reader.GetOrdinal("MF-AGG"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("MF-AGG")),
                                    NumFilingsMulti = (reader.IsDBNull(reader.GetOrdinal("Multi"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Multi")),
                                    NumFilingsProxy = (reader.IsDBNull(reader.GetOrdinal("Proxy"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Proxy")),
                                    NumFilingsSchD = (reader.IsDBNull(reader.GetOrdinal("Sch-D"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Sch-D")),
                                    NumFilingsSEDI = (reader.IsDBNull(reader.GetOrdinal("SEDI"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SEDI")),
                                    NumFilingsULTAGG = (reader.IsDBNull(reader.GetOrdinal("ULT-AGG"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ULT-AGG")),
                                    WeightedPD = (reader.IsDBNull(reader.GetOrdinal("WeightedPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WeightedPD")),
                                    Score = (reader.IsDBNull(reader.GetOrdinal("Score"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Score")),
                                    MinFilingDate = reader.IsDBNull(reader.GetOrdinal("MinFilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinFilingDate")),
                                    MaxFilingDate = reader.IsDBNull(reader.GetOrdinal("MaxFilingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxFilingDate"))
                                };

                                data.MinFilingDateAsString = DateUtils.ConvertDate(data.MinFilingDate, "yyyy-MM-dd");
                                data.MaxFilingDateAsString = DateUtils.ConvertDate(data.MaxFilingDate, "yyyy-MM-dd");

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
        /// <param name="activistScores"></param>
        public void SaveActivistScores(IList<ActivistScore> activistScores)
        {
            StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.stg_globalcef_fundholdermaster " +
                "(Holder, ShortName, InstitutionType, Score, Comments) values ");
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (activistScores != null && activistScores.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.stg_globalcef_fundholdermaster");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.stg_globalcef_fundholdermaster";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.stg_globalcef_fundholdermaster");

                            IList<string> rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (ActivistScore data in activistScores)
                            {
                                if (!string.IsNullOrEmpty(data.Holder))
                                    sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.Holder), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.ShortName))
                                    sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.ShortName), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.InstitutionType))
                                    sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.InstitutionType), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Score.HasValue)
                                    sb.Append(data.Score).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Comments))
                                    sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.Comments), "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", rows));
                            sCommand.Append(";");

                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Calculating fund level scores...");
                            using (MySqlCommand command = new MySqlCommand(SaveActivistScoresQuery, connection, trans))
                            {
                                command.CommandType = CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
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
        /// <param name="holdings"></param>
        public void SaveAlmHoldings(IList<AlmHolding> holdings)
        {
            StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.PMHoldings " +
                    "(Portfolio, Ticker, EffectiveDate, Position) values ");
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (holdings != null && holdings.Count > 0)
                    {
                        string effectiveDateString = holdings[0].AsofDateString;
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.PMHoldings for EffectiveDate = '" + effectiveDateString + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.PMHoldings where EffectiveDate = '" + effectiveDateString + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.PMHoldings");

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (AlmHolding data in holdings)
                            {
                                //data.AsofDate = DateUtils.ConvertToDate(data.AsofDateString, "yyyy-MM-dd");
                                //Rows.Add(string.Format("('{0}','{1}','{2}',{3})",
                                //MySqlHelper.EscapeString(data.Portfolio),
                                //data.Ticker,
                                //effectiveDateString,
                                //data.Position
                                //));

                                if (!string.IsNullOrEmpty(data.Portfolio))
                                    sb.Append(string.Concat("'", data.Portfolio, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(effectiveDateString))
                                    sb.Append(string.Concat("'", effectiveDateString, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Position.HasValue)
                                    sb.Append(data.Position);
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
        /// <param name="holdings"></param>
        public void SaveSharesRecallOverrides(IList<AlmHolding> holdings)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        using (MySqlCommand command = new MySqlCommand(SaveSharesRecallOverridesQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.Parameters.Add(new MySqlParameter("p_Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_TaxStatus", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_QualifyingDividends", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_DvdYield", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExDvdDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_NextDvdDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_RecallDays", MySqlDbType.Int32));

                            foreach (AlmHolding data in holdings)
                            {
                                command.Parameters[0].Value = data.Ticker;
                                command.Parameters[1].Value = data.TaxStatus;
                                command.Parameters[2].Value = data.QualifyingDividends;
                                command.Parameters[3].Value = data.OvrDividendYield;
                                command.Parameters[4].Value = data.OvrDividendExDate;
                                command.Parameters[5].Value = data.OvrNextDividendExDate;
                                command.Parameters[6].Value = data.RecallDays;

                                command.ExecuteNonQuery();
                            }
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
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="portfolio"></param>
        /// <param name="broker"></param>
        /// <param name="ticker"></param>
        /// <param name="currency"></param>
        /// <param name="country"></param>
        /// <param name="geoLevel1"></param>
        /// <param name="geoLevel2"></param>
        /// <param name="geoLevel3"></param>
        /// <param name="assetClassLevel1"></param>
        /// <param name="assetClassLevel2"></param>
        /// <param name="assetClassLevel3"></param>
        /// <param name="fundCategory"></param>
        /// <param name="securityType"></param>
        /// <returns></returns>
        public IList<Trade> GetTradeHistory(string startDate, string endDate, string portfolio, string broker, string ticker,
            string currency, string country, string geoLevel1, string geoLevel2, string geoLevel3,
            string assetClassLevel1, string assetClassLevel2, string assetClassLevel3,
            string fundCategory, string securityType)
        {
            IList<Trade> list = new List<Trade>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetTradeHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_StartDate", DateUtils.ConvertToDate(startDate));
                        command.Parameters.AddWithValue("p_EndDate", DateUtils.ConvertToDate(endDate));
                        command.Parameters.AddWithValue("p_Portfolio", portfolio);
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Broker", broker);
                        command.Parameters.AddWithValue("p_Currency", currency);
                        command.Parameters.AddWithValue("p_Country", country);
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_GeoLevel1", geoLevel1);
                        command.Parameters.AddWithValue("p_GeoLevel2", geoLevel2);
                        command.Parameters.AddWithValue("p_GeoLevel3", geoLevel3);
                        command.Parameters.AddWithValue("p_AssetClassLevel1", assetClassLevel1);
                        command.Parameters.AddWithValue("p_AssetClassLevel2", assetClassLevel2);
                        command.Parameters.AddWithValue("p_AssetClassLevel3", assetClassLevel3);
                        command.Parameters.AddWithValue("p_FundCategory", fundCategory);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Trade data = new Trade
                                {
                                    Portfolio = reader["Portfolio"] as string,
                                    Broker = reader["Broker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    TradeCurrency = reader["TradeCurrency"] as string,
                                    FundCurrency = reader["FundCurrency"] as string,
                                    Country = reader["Country"] as string,
                                    TradeType = reader["TradeType"] as string,
                                    SecurityName = reader["SecurityName"] as string,
                                    GeoLevel1 = reader["GeoLevel1"] as string,
                                    GeoLevel2 = reader["GeoLevel2"] as string,
                                    GeoLevel3 = reader["GeoLevel3"] as string,
                                    FundClassLevel1 = reader["AssetClassLevel1"] as string,
                                    FundClassLevel2 = reader["AssetClassLevel2"] as string,
                                    FundClassLevel3 = reader["AssetClassLevel3"] as string,
                                    SecurityType = reader["SecurityType"] as string,
                                    FundCategory = reader["FundCategory"] as string,

                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),

                                    PublishedNav = (reader.IsDBNull(reader.GetOrdinal("PublishedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedNav")),
                                    PublishedNavDiscount = (reader.IsDBNull(reader.GetOrdinal("PublishedNavDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedNavDiscount")),
                                    PublishedNavDiscountReported = (reader.IsDBNull(reader.GetOrdinal("PublishedNavDiscountRpt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedNavDiscountRpt")),
                                    PublishedNavDate = (reader.IsDBNull(reader.GetOrdinal("PublishedNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PublishedNavDate")),

                                    EstimatedNav = (reader.IsDBNull(reader.GetOrdinal("EstimatedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNav")),
                                    EstimatedNavDiscount = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNavDiscount")),
                                    EstimatedNavDiscountReported = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavDiscountRpt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNavDiscountRpt")),
                                    EstimatedNavDate = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EstimatedNavDate")),

                                    InterpolatedNav = (reader.IsDBNull(reader.GetOrdinal("InterpolatedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InterpolatedNav")),
                                    InterpolatedNavDiscount = (reader.IsDBNull(reader.GetOrdinal("InterpolatedNavDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InterpolatedNavDiscount")),
                                    InterpolatedNavDiscountReported = (reader.IsDBNull(reader.GetOrdinal("InterpolatedNavDiscountRpt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InterpolatedNavDiscountRpt")),
                                    InterpolatedNavDate = (reader.IsDBNull(reader.GetOrdinal("InterpolatedNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("InterpolatedNavDate"))
                                };

                                data.PublishedNavDateAsString = DateUtils.ConvertDate(data.PublishedNavDate, "yyyy-MM-dd");
                                data.EstimatedNavDateAsString = DateUtils.ConvertDate(data.EstimatedNavDate, "yyyy-MM-dd");
                                data.InterpolatedNavDateAsString = DateUtils.ConvertDate(data.InterpolatedNavDate, "yyyy-MM-dd");
                                data.TradeDateAsString = DateUtils.ConvertDate(data.TradeDate, "yyyy-MM-dd");

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
        /// <param name="asofDate"></param>
        /// <returns></returns>
        public IDictionary<string, Trade> GetTradeHistory(DateTime asofDate)
        {
            IDictionary<string, Trade> dict = new Dictionary<string, Trade>();
            string positionKey = null;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetTradeHistoryByDateQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_AsofDate", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Trade data = new Trade();
                                positionKey = reader["PositionKey"] as string;
                                data.PositionKey = positionKey;
                                data.Portfolio = reader["Portfolio"] as string;
                                data.Broker = reader["Broker"] as string;
                                data.Ticker = reader["Ticker"] as string;
                                data.SecTicker = reader["SecTicker"] as string;
                                data.YellowKey = reader["YellowKey"] as string;
                                data.TradeCurrency = reader["Currency"] as string;
                                data.SecurityName = reader["SecurityName"] as string;
                                data.Figi = reader["Figi"] as string;

                                data.Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position"));
                                data.Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price"));
                                data.MarketValue = (reader.IsDBNull(reader.GetOrdinal("MarketValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketValue"));
                                data.FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate"));
                                data.TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate"));

                                data.TradeDateAsString = DateUtils.ConvertDate(data.TradeDate, "yyyy-MM-dd");

                                if (!dict.ContainsKey(data.PositionKey))
                                    dict.Add(data.PositionKey, data);
                                else
                                    _logger.LogError("Duplicate trade key: " + data.PositionKey);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query: " + positionKey);
                throw;
            }

            return dict;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundHoldingSummary> GetFundHoldingSummary()
        {
            IDictionary<string, FundHoldingSummary> dict = new Dictionary<string, FundHoldingSummary>();
            string ticker = null;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundHoldingsSummaryQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHoldingSummary data = new FundHoldingSummary();
                                ticker = reader["Ticker"] as string;
                                data.Ticker = ticker;
                                data.FundDate = (reader.IsDBNull(reader.GetOrdinal("PortDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortDate"));

                                data.Currency1 = reader["Currency1"] as string;
                                data.Currency2 = reader["Currency2"] as string;
                                data.Currency3 = reader["Currency3"] as string;
                                data.Currency4 = reader["Currency4"] as string;
                                data.Currency5 = reader["Currency5"] as string;

                                data.Currency1Exp = (reader.IsDBNull(reader.GetOrdinal("Currency1Exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Currency1Exposure"));
                                data.Currency2Exp = (reader.IsDBNull(reader.GetOrdinal("Currency2Exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Currency2Exposure"));
                                data.Currency3Exp = (reader.IsDBNull(reader.GetOrdinal("Currency3Exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Currency3Exposure"));
                                data.Currency4Exp = (reader.IsDBNull(reader.GetOrdinal("Currency4Exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Currency4Exposure"));
                                data.Currency5Exp = (reader.IsDBNull(reader.GetOrdinal("Currency5Exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Currency5Exposure"));

                                data.FundDateAsString = DateUtils.ConvertDate(data.FundDate, "yyyy-MM-dd");

                                dict.Add(ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query for ticker: " + ticker);
                throw;
            }

            return dict;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<PositionLongShortDetail> GetPositionLongShortDetails()
        {
            IList<PositionLongShortDetail> list = new List<PositionLongShortDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPositionLongShortDetailsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PositionLongShortDetail data = new PositionLongShortDetail
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecName = reader["SecName"] as string,
                                    LongShortPosOppFlag = reader["LongShortPosOPP"] as string,
                                    LongShortPosTacFlag = reader["LongShortPosTAC"] as string,
                                    LongShortPosFlag = reader["LongShortPos"] as string,

                                    AsofDate = (reader.IsDBNull(reader.GetOrdinal("AsofDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsofDate")),

                                    TotalPosOpp = (reader.IsDBNull(reader.GetOrdinal("OPPTotal"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OPPTotal")),
                                    LongPosOpp = (reader.IsDBNull(reader.GetOrdinal("OPPLong"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OPPLong")),
                                    ShortPosOpp = (reader.IsDBNull(reader.GetOrdinal("OPPShort"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OPPShort")),

                                    TotalPosTac = (reader.IsDBNull(reader.GetOrdinal("TACTotal"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TACTotal")),
                                    LongPosTac = (reader.IsDBNull(reader.GetOrdinal("TACLong"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TACLong")),
                                    ShortPosTac = (reader.IsDBNull(reader.GetOrdinal("TACShort"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TACShort"))
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
        public IDictionary<string, FundSharesTendered> GetSharesTendered()
        {
            IDictionary<string, FundSharesTendered> dict = new Dictionary<string, FundSharesTendered>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSharesTenderedQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSharesTendered data = new FundSharesTendered
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundName = reader["FundName"] as string,
                                    Broker = reader["Broker"] as string,
                                    SharesTendered = (reader.IsDBNull(reader.GetOrdinal("SharesTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesTendered")),
                                    RedemptionDate = reader.IsDBNull(reader.GetOrdinal("RedemptionDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RedemptionDate")),
                                    PaymentDate = reader.IsDBNull(reader.GetOrdinal("PaymentDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PaymentDate"))
                                };
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

        public IList<PositionReconTO> GetPositionRecon(DateTime asofDate)
        {
            IList<PositionReconTO> list = new List<PositionReconTO>();

            try
            {
                string sql = GetPositionReconQuery;
                sql += " where EndDate = (select max(EndDate) from almitasc_ACTradingPM.portfoliomanager_dailyrecon where EndDate <= '" + DateUtils.ConvertDate(asofDate, "yyyy-MM-dd") + "')";
                sql += " order by abs(Variance) desc, Ticker, Portfolio";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Double? db;
                                PositionReconTO data = new PositionReconTO
                                {
                                    Id = Convert.ToInt64(reader["Id"]),
                                    FundName = reader["FundName"] as string,
                                    Broker = reader["Broker"] as string,
                                    BrokerSecurityId = reader["BrokerSystemId"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    IsSwap = reader["IsSwap"] as string,
                                    Notes = reader["Notes"] as string,
                                    BrokerStartPosition = (reader.IsDBNull(reader.GetOrdinal("BrokerStartPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerStartPosition")),
                                    TradedPosition = (reader.IsDBNull(reader.GetOrdinal("TradedPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradedPosition")),
                                    BrokerEndPosition = (reader.IsDBNull(reader.GetOrdinal("BrokerEndPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerEndPosition")),
                                    DerivedPosition = (reader.IsDBNull(reader.GetOrdinal("DerivedPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DerivedPosition")),
                                    OverwritePosition = ((reader.IsDBNull(reader.GetOrdinal("OverwritePosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OverwritePosition"))).ToString(),
                                    Variance = (reader.IsDBNull(reader.GetOrdinal("Variance"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Variance")),

                                    StartDate = reader.IsDBNull(reader.GetOrdinal("StartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartDate")),
                                    EndDate = reader.IsDBNull(reader.GetOrdinal("EndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndDate")),
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

        public IList<PositionTO> GetAlmHoldingsByDate(DateTime asofDate, string fundName)
        {
            IList<PositionTO> list = new List<PositionTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetAlmHoldingsByDateQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_AsofDate", asofDate);
                        command.Parameters.AddWithValue("p_Portfolio", fundName);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PositionTO data = new PositionTO
                                {
                                    AsOfDt = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Fund = reader["Fund"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Curr = reader["Curr"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    Fx = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Pos = (reader.IsDBNull(reader.GetOrdinal("Pos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Pos")),
                                    CashPos = (reader.IsDBNull(reader.GetOrdinal("CashPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("CashPos")),
                                    SwapPos = (reader.IsDBNull(reader.GetOrdinal("SwapPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SwapPos")),
                                    TotMvLocal = (reader.IsDBNull(reader.GetOrdinal("TotMVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotMVLocal")),
                                    TotMvUSD = (reader.IsDBNull(reader.GetOrdinal("TotMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotMVUSD")),
                                    CashMvUSD = (reader.IsDBNull(reader.GetOrdinal("CashMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashMVUSD")),
                                    SwapMvUSD = (reader.IsDBNull(reader.GetOrdinal("SwapMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapMVUSD")),
                                    TotWt = (reader.IsDBNull(reader.GetOrdinal("TotWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotWt")),
                                    CashWt = (reader.IsDBNull(reader.GetOrdinal("CashWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashWt")),
                                    SwapWt = (reader.IsDBNull(reader.GetOrdinal("SwapWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapWt")),
                                    TotOwnWt = (reader.IsDBNull(reader.GetOrdinal("TotOwnWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotOwnWt")),
                                    CashOwnWt = (reader.IsDBNull(reader.GetOrdinal("CashOwnWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashOwnWt")),
                                    FundNav = (reader.IsDBNull(reader.GetOrdinal("FundMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundMV")),
                                    FundNavDt = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    ShOut = (reader.IsDBNull(reader.GetOrdinal("ShOut"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ShOut")),
                                    Rank = (reader.IsDBNull(reader.GetOrdinal("Rnk"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Rnk")),
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["CUSIP"] as string,
                                    SecDesc = reader["SecurityName"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    AssetLevel1 = reader["AssetClassLevel1"] as string,
                                    AssetLevel2 = reader["AssetClassLevel2"] as string,
                                    AssetLevel3 = reader["AssetClassLevel3"] as string,
                                    GeoLevel1 = reader["GeoLevel1"] as string,
                                    GeoLevel2 = reader["GeoLevel2"] as string,
                                    GeoLevel3 = reader["GeoLevel3"] as string,
                                    Sec13F = reader["Security13FFlag"] as string,
                                    PfdExchSym = reader["PfdExchSym"] as string,
                                    ParentCompName = reader["ParentCompName"] as string,
                                    YKey = reader["YellowKey"] as string,
                                };

                                data.PosInd = (data.Pos < 0) ? "S" : "L";
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

        public IList<HoldingExposureTO> GetHoldingsByDate(DateTime asofDate)
        {
            IList<HoldingExposureTO> list = new List<HoldingExposureTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetHoldingsByDateQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_AsofDate", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                HoldingExposureTO data = new HoldingExposureTO
                                {
                                    Symbol = reader["Symbol"] as string,
                                    AsOfDt = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Curr = reader["Curr"] as string,
                                    PriceLocal = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("PriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceUSD")),
                                    Fx = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Pos = (reader.IsDBNull(reader.GetOrdinal("Pos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Pos")),
                                    CashPos = (reader.IsDBNull(reader.GetOrdinal("CashPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("CashPos")),
                                    SwapPos = (reader.IsDBNull(reader.GetOrdinal("SwapPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SwapPos")),
                                    FundOppPos = (reader.IsDBNull(reader.GetOrdinal("OppPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPos")),
                                    FundOppCashPos = (reader.IsDBNull(reader.GetOrdinal("OppCashPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppCashPos")),
                                    FundOppSwapPos = (reader.IsDBNull(reader.GetOrdinal("OppSwapPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppSwapPos")),
                                    FundTacPos = (reader.IsDBNull(reader.GetOrdinal("TacPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPos")),
                                    FundTacCashPos = (reader.IsDBNull(reader.GetOrdinal("TacCashPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacCashPos")),
                                    FundTacSwapPos = (reader.IsDBNull(reader.GetOrdinal("TacSwapPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacSwapPos")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("TotMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotMVUSD")),
                                    FundOppMV = (reader.IsDBNull(reader.GetOrdinal("OppMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppMVUSD")),
                                    FundTacMV = (reader.IsDBNull(reader.GetOrdinal("TacMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacMVUSD")),
                                    Wt = (reader.IsDBNull(reader.GetOrdinal("TotWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotWt")),
                                    FundOppWt = (reader.IsDBNull(reader.GetOrdinal("OppWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppWt")),
                                    FundTacWt = (reader.IsDBNull(reader.GetOrdinal("TacWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacWt")),
                                    ShOut = (reader.IsDBNull(reader.GetOrdinal("ShOut"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ShOut")),
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["CUSIP"] as string,
                                    SecDesc = reader["SecurityName"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    AssetClsLvl1 = reader["AssetClassLevel1"] as string,
                                    AssetClsLvl2 = reader["AssetClassLevel2"] as string,
                                    AssetClsLvl3 = reader["AssetClassLevel3"] as string,
                                    GeoLvl1 = reader["GeoLevel1"] as string,
                                    GeoLvl2 = reader["GeoLevel2"] as string,
                                    GeoLvl3 = reader["GeoLevel3"] as string,
                                    Sec13F = reader["Security13FFlag"] as string,
                                    PfdExchSym = reader["PfdExchSym"] as string,
                                    ParentCompName = reader["ParentCompName"] as string,
                                    YKey = reader["YellowKey"] as string,
                                    Cntry = reader["Country"] as string,
                                    PaymentRank = reader["PaymentRank"] as string,
                                    TrdStatus = reader["MktStatus"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    CashSynthetic = reader["CashSynthetic"] as string,
                                    Sector = reader["FundGroup"] as string,

                                    //FundOpp
                                    FundOppPosJPM = (reader.IsDBNull(reader.GetOrdinal("OppPosJPM"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosJPM")),
                                    FundOppCashPosJPM = (reader.IsDBNull(reader.GetOrdinal("OppCashPosJPM"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppCashPosJPM")),
                                    FundOppSwapPosJPM = (reader.IsDBNull(reader.GetOrdinal("OppSwapPosJPM"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppSwapPosJPM")),
                                    FundOppPosFidelity = (reader.IsDBNull(reader.GetOrdinal("OppPosFidelity"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosFidelity")),
                                    FundOppPosIB = (reader.IsDBNull(reader.GetOrdinal("OppPosIB"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosIB")),
                                    FundOppPosEDF = (reader.IsDBNull(reader.GetOrdinal("OppPosEDF"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosEDF")),
                                    FundOppPosScotia = (reader.IsDBNull(reader.GetOrdinal("OppPosScotia"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosScotia")),
                                    FundOppPosTD = (reader.IsDBNull(reader.GetOrdinal("OppPosTD"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosTD")),
                                    FundOppPosJefferies = (reader.IsDBNull(reader.GetOrdinal("OppPosJeff"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosJeff")),
                                    FundOppPosBMO = (reader.IsDBNull(reader.GetOrdinal("OppPosBMO"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosBMO")),
                                    FundOppPosUBS = (reader.IsDBNull(reader.GetOrdinal("OppPosUBS"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosUBS")),
                                    FundOppPosMS = (reader.IsDBNull(reader.GetOrdinal("OppPosMS"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosMS")),
                                    FundOppPosBAML = (reader.IsDBNull(reader.GetOrdinal("OppPosBAML"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosBAML")),
                                    FundOppPosSTON = (reader.IsDBNull(reader.GetOrdinal("OppPosSTON"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPosSTON")),
                                    //FundTac
                                    FundTacPosJPM = (reader.IsDBNull(reader.GetOrdinal("TacPosJPM"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosJPM")),
                                    FundTacCashPosJPM = (reader.IsDBNull(reader.GetOrdinal("TacCashPosJPM"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacCashPosJPM")),
                                    FundTacSwapPosJPM = (reader.IsDBNull(reader.GetOrdinal("TacSwapPosJPM"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacSwapPosJPM")),
                                    FundTacPosFidelity = (reader.IsDBNull(reader.GetOrdinal("TacPosFidelity"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosFidelity")),
                                    FundTacPosIB = (reader.IsDBNull(reader.GetOrdinal("TacPosIB"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosIB")),
                                    FundTacPosEDF = (reader.IsDBNull(reader.GetOrdinal("TacPosEDF"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosEDF")),
                                    FundTacPosScotia = (reader.IsDBNull(reader.GetOrdinal("TacPosScotia"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosScotia")),
                                    FundTacPosTD = (reader.IsDBNull(reader.GetOrdinal("TacPosTD"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosTD")),
                                    FundTacPosJefferies = (reader.IsDBNull(reader.GetOrdinal("TacPosJeff"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosJeff")),
                                    FundTacPosBMO = (reader.IsDBNull(reader.GetOrdinal("TacPosBMO"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosBMO")),
                                    FundTacPosUBS = (reader.IsDBNull(reader.GetOrdinal("TacPosUBS"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosUBS")),
                                    FundTacPosMS = (reader.IsDBNull(reader.GetOrdinal("TacPosMS"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosMS")),
                                    FundTacPosBAML = (reader.IsDBNull(reader.GetOrdinal("TacPosBAML"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosBAML")),
                                    FundTacPosSTON = (reader.IsDBNull(reader.GetOrdinal("TacPosSTON"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPosSTON")),
                                };

                                data.PosInd = (data.Pos < 0) ? "S" : "L";
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

        public void UpdateReconData(IList<PositionReconTO> list)
        {
            StringBuilder sCommand = new StringBuilder("");
            StringBuilder sCommandUpdate = new StringBuilder("update almitasc_ACTradingPM.portfoliomanager_dailyrecon set ");
            int sbcount = 0;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {

                            _logger.LogInformation("Saving data to almitasc_ACTradingPM.portfoliomanager_dailyrecon");

                            IList<string> Rows = new List<string>();
                            IList<string> Rowsupdate = new List<string>();
                            StringBuilder sbupdate = new StringBuilder();


                            foreach (PositionReconTO data in list)
                            {

                                sbcount++;
                                double d;
                                DateTime _startdate = data.StartDate.GetValueOrDefault(DateTime.Now);
                                DateTime _enddate = data.EndDate.GetValueOrDefault(DateTime.Now);
                                DateTime dt = DateTime.Now;
                                string mdate = dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                                if (!string.IsNullOrEmpty(data.OverwritePosition))
                                    sbupdate.Append("OverwritePosition = " + data.OverwritePosition).Append(DELIMITER);
                                else
                                    sbupdate.Append("OverwritePosition = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Notes))
                                    sbupdate.Append(string.Concat("Notes = '", data.Notes, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("Notes = null").Append(DELIMITER);


                                sbupdate.Append(string.Concat(" datemodified = CURRENT_TIMESTAMP()"));

                                sbupdate.Append(string.Concat(" where Id = " + data.Id + ";"));


                                string row = sCommandUpdate.ToString() + sbupdate.ToString() + "\n";
                                Rowsupdate.Add(string.Join("", row));
                                sbupdate.Clear();


                            }

                            if (sbcount > 0)
                                sCommand.Append(string.Join("", Rowsupdate));

                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
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

        public IList<EODPositionTO> GetEODHoldingsByDate(DateTime asofDate)
        {
            IList<EODPositionTO> list = new List<EODPositionTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetEODHoldingsByDateQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_AsofDate", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EODPositionTO data = new EODPositionTO
                                {
                                    AsOfDt = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Fund = reader["Fund"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    YKey = reader["YKey"] as string,
                                    Curr = reader["Curr"] as string,
                                    OpenPos = (reader.IsDBNull(reader.GetOrdinal("OpenPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenPos")),
                                    OpenCashPos = (reader.IsDBNull(reader.GetOrdinal("OpenCashPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenCashPos")),
                                    OpenSwapPos = (reader.IsDBNull(reader.GetOrdinal("OpenSwapPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenSwapPos")),
                                    OpenPrc = (reader.IsDBNull(reader.GetOrdinal("OpenPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenPrice")),
                                    OpenFx = (reader.IsDBNull(reader.GetOrdinal("OpenFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenFxRate")),
                                    OpenMV = (reader.IsDBNull(reader.GetOrdinal("OpenMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OpenMV")),
                                    TradePos = (reader.IsDBNull(reader.GetOrdinal("TradePos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePos")),
                                    TradeCashPos = (reader.IsDBNull(reader.GetOrdinal("TradeCashPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeCashPos")),
                                    TradeSwapPos = (reader.IsDBNull(reader.GetOrdinal("TradeSwapPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeSwapPos")),
                                    TradePrc = (reader.IsDBNull(reader.GetOrdinal("TradePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePrice")),
                                    TradeFx = (reader.IsDBNull(reader.GetOrdinal("TradeFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeFxRate")),
                                    TradeMV = (reader.IsDBNull(reader.GetOrdinal("TradeMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeMV")),
                                    EODPos = (reader.IsDBNull(reader.GetOrdinal("EODPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EODPos")),
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

            return list;
        }

        public IList<FundFuturesExposureTO> GetFuturesHoldings(DateTime asofDate, string fundName)
        {
            IList<FundFuturesExposureTO> list = new List<FundFuturesExposureTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFuturesHoldingsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_FundName", fundName);
                        command.Parameters.AddWithValue("p_Date", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundFuturesExposureTO data = new FundFuturesExposureTO
                                {
                                    FundName = reader["FundName"] as string,
                                    Broker = reader["Broker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Ykey = reader["Ykey"] as string,
                                    SecDesc = reader["SecurityName"] as string,
                                    Curr = reader["Curr"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    Duration = (reader.IsDBNull(reader.GetOrdinal("DurAdjMid"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DurAdjMid")),
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("PriceMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMultiplier")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("fxrate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrate")),
                                    RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId")),
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

        public IList<AlmHoldingPerfTO> GetAlmPositionsByDate(DateTime asofDate)
        {
            IList<AlmHoldingPerfTO> list = new List<AlmHoldingPerfTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetALMPositionsByDateQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_AsofDate", asofDate);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                AlmHoldingPerfTO data = new AlmHoldingPerfTO
                                {
                                    Symbol = reader["Symbol"] as string,
                                    AsOfDt = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Curr = reader["Curr"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["CUSIP"] as string,
                                    SecDesc = reader["SecurityName"] as string,
                                    PfdExchSym = reader["PfdExchSym"] as string,
                                    Sector = reader["FundGroup"] as string,
                                    Cntry = reader["Country"] as string,
                                    YKey = reader["YKey"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    ClsPrcLcl = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    ClsPrc = (reader.IsDBNull(reader.GetOrdinal("PriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceUSD")),
                                    ClsFX = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Pos = (reader.IsDBNull(reader.GetOrdinal("Pos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Pos")),
                                    CashPos = (reader.IsDBNull(reader.GetOrdinal("CashPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("CashPos")),
                                    SwapPos = (reader.IsDBNull(reader.GetOrdinal("SwapPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SwapPos")),
                                    FundOppPos = (reader.IsDBNull(reader.GetOrdinal("OppPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppPos")),
                                    FundOppCashPos = (reader.IsDBNull(reader.GetOrdinal("OppCashPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppCashPos")),
                                    FundOppSwapPos = (reader.IsDBNull(reader.GetOrdinal("OppSwapPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("OppSwapPos")),
                                    FundTacPos = (reader.IsDBNull(reader.GetOrdinal("TacPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacPos")),
                                    FundTacCashPos = (reader.IsDBNull(reader.GetOrdinal("TacCashPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacCashPos")),
                                    FundTacSwapPos = (reader.IsDBNull(reader.GetOrdinal("TacSwapPos"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TacSwapPos")),
                                    ClsMV = (reader.IsDBNull(reader.GetOrdinal("TotMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotMVUSD")),
                                    FundOppClsMV = (reader.IsDBNull(reader.GetOrdinal("OppMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppMVUSD")),
                                    FundTacClsMV = (reader.IsDBNull(reader.GetOrdinal("TacMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacMVUSD")),
                                    ClsWt = (reader.IsDBNull(reader.GetOrdinal("TotWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotWt")),
                                    FundOppClsWt = (reader.IsDBNull(reader.GetOrdinal("OppWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppWt")),
                                    FundTacClsWt = (reader.IsDBNull(reader.GetOrdinal("TacWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacWt")),
                                    FundOppClsPrc = (reader.IsDBNull(reader.GetOrdinal("OppPriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppPriceUSD")),
                                    FundTacClsPrc = (reader.IsDBNull(reader.GetOrdinal("TacPriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacPriceUSD")),
                                    DataIssue = (reader.IsDBNull(reader.GetOrdinal("DataIssue"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DataIssue")),
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

        ////////////////////////////////private const string GetAlmHoldingsQuery = "spGetALMHoldingDetails";
        ////////////////////////////////private const string GetAlmHoldingsByDateQuery = "spGetALMPositionsByDate";
        ////////////////////////////////private const string GetHoldingsByDateQuery = "spGetALMPositionsByDateNew";
        ///////////////////////////////////private const string GetEODHoldingsByDateQuery = "spGetEODHoldings";
        private const string GetAlmHoldingsQuery = "Reporting.spGetALMHoldingDetails";
        private const string GetAlmHoldingsByDateQuery = "Reporting.spGetALMPositionsByDate";
        private const string GetHoldingsByDateQuery = "Reporting.spGetALMPositionsByDateNew";
        private const string GetALMPositionsByDateQuery = "Reporting.spGetALMPositions";
        private const string GetEODHoldingsByDateQuery = "Reporting.spGetEODHoldings";
        private const string GetAlmHoldingsSharesRecallQuery = "call almitasc_ACTradingBBGData.spGetALMHoldingsSharesRecall";
        private const string GetActivistHoldingsQuery = "spGetActivistLatestHoldings";
        private const string GetFundHoldersQuery = "spGetFundHolders";
        private const string SaveSharesRecallOverridesQuery = "spSaveShareRecallOverrides";
        private const string GetActivistHoldersQuery = "spGetActivistHoldingsNew";
        private const string GetActivistHoldersHistoryQuery = "spGetActivistHoldingsHistory";
        private const string GetActivistScoresQuery = "spGetActivistScores";
        private const string SaveActivistScoresQuery = "almitasc_ACTradingBBGData.spUpdateFundHolderScores";
        private const string GetTradeHistoryQuery = "spGetTradeHistoryNew";
        private const string GetTradeHistoryByDateQuery = "spGetTradeHistoryByDate";
        private const string GetFundHoldingsSummaryQuery = "call almitasc_ACTradingBBGData.spGetFundPortSummary";
        private const string GetPositionLongShortDetailsQuery = "call almitasc_ACTradingBBGData.spGetPositionCheckReport";
        private const string GetSharesTenderedQuery = "select * from almitasc_ACTradingBBGData.FundSharesTenderHist where PaymentDate > current_date()";
        private const string GetPositionReconQuery = "select distinct"
                                + " (case when portfolio = 'Opportunity' then 'Opp' when portfolio = 'Tactical' then 'Tac' else null end) as FundName"
                                + " ,primebroker as Broker"
                                + " ,brokersystemid as BrokerSystemId"
                                + " ,ticker as Ticker"
                                + " ,yellowkey as YellowKey"
                                + " ,brokerstartposition as BrokerStartPosition"
                                + " ,trade as TradedPosition"
                                + " ,brokerendposition as BrokerEndPosition"
                                + " ,derivedposition as DerivedPosition"
                                + " ,variance as Variance"
                                + " ,startdate as StartDate"
                                + " ,enddate as EndDate"
                                + " ,(case when ticker like 'SwapUnderlying#%' then 'Y' else 'N' end) as IsSwap, Id, Notes, OverwritePosition"
                                + " from almitasc_ACTradingPM.portfoliomanager_dailyrecon";
        private const string GetFuturesHoldingsQuery = "spALMFutures";
        private const string GetActivistSummaryQuery = "almitasc_ACTradingBBGData.spGetActivistHoldingsSummary";
        private const string GetActivistMasterQuery = "select * from almitasc_ACTradingBBGData.globalcef_fundholdermaster";
    }
}
