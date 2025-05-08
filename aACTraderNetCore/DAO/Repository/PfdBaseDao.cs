using aACTrader.Common;
using aACTrader.DAO.Interface;
using aCommons.Pfd;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class PfdBaseDao : IPfdBaseDao
    {
        private readonly ILogger<PfdBaseDao> _logger;
        private const string DELIMITER = ",";

        public PfdBaseDao(ILogger<PfdBaseDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing PfdBaseDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, PfdSecurity> GetPfdSecurityMaster()
        {
            IDictionary<string, PfdSecurity> dict = new Dictionary<string, PfdSecurity>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPfdSecurityMasterQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PfdSecurity data = new PfdSecurity
                                {
                                    Figi = reader["figi"] as string,
                                    BBGId = reader["bbgid"] as string,
                                    CUSIP = reader["id_cusip"] as string,
                                    CompanyNameLong = reader["long_comp_name"] as string,
                                    IssuerTicker = reader["issuer_ticker"] as string,
                                    IssuerParentTicker = reader["issuer_parent_ticker"] as string,
                                    MaturityDt = reader.IsDBNull(reader.GetOrdinal("maturity")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("maturity")),
                                    Currency = reader["crncy"] as string,
                                    AmountOutstanding = (reader.IsDBNull(reader.GetOrdinal("amt_outstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("amt_outstanding")),
                                    SecurityInArrears = reader["security_in_arrears"] as string,
                                    Series = reader["series"] as string,
                                    RtgDbrs = reader["rtg_dbrs"] as string,
                                    RtgMoody = reader["rtg_moody"] as string,
                                    RtgSP = reader["rtg_sp"] as string,
                                    CreditLevel = reader["creditlevel"] as string,
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("multiplier")),
                                    ResetIndex = reader["idx"] as string,
                                    FloatSpread = (reader.IsDBNull(reader.GetOrdinal("flt_spread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("flt_spread")),
                                    Cpn = (reader.IsDBNull(reader.GetOrdinal("cpn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("cpn")),
                                    NextRefixDt = reader.IsDBNull(reader.GetOrdinal("nxtrefixdt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("nxtrefixdt")),
                                    RefixFreq = (reader.IsDBNull(reader.GetOrdinal("refixfreq"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("refixfreq")),
                                    LifeRefixFloor = (reader.IsDBNull(reader.GetOrdinal("liferefixfloor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("liferefixfloor")),
                                    PrevCpnDt = reader.IsDBNull(reader.GetOrdinal("prev_cpn_dt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("prev_cpn_dt")),
                                    NxtCpnDt = reader.IsDBNull(reader.GetOrdinal("nxt_cpn_dt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("nxt_cpn_dt")),
                                    CpnFreq = (reader.IsDBNull(reader.GetOrdinal("cpn_freq"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("cpn_freq")),
                                    DayCntDes = reader["day_cnt_des"] as string,
                                    IntAccDays = (reader.IsDBNull(reader.GetOrdinal("int_acc_days"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("int_acc_days")),
                                    IntAcc = (reader.IsDBNull(reader.GetOrdinal("int_acc"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("int_acc")),
                                    NxtCallDt = reader.IsDBNull(reader.GetOrdinal("nxt_call_dt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("nxt_call_dt")),
                                    NxtCallPrice = (reader.IsDBNull(reader.GetOrdinal("nxt_call_px"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("nxt_call_px")),
                                    Par = (reader.IsDBNull(reader.GetOrdinal("par"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("par")),
                                    Exchangeable = reader["exchangeable"] as string,
                                    NxtExchDt = reader.IsDBNull(reader.GetOrdinal("nxt_exch_dt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("nxt_exch_dt")),
                                    ExchBBGId = reader["exchbbgid"] as string,
                                    ExchMultiplier = (reader.IsDBNull(reader.GetOrdinal("exchmultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("exchmultiplier")),
                                    ExchIndex = reader["exchidx"] as string,
                                    ExchSpread = (reader.IsDBNull(reader.GetOrdinal("exchspread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("exchspread")),
                                    ExchCpn = (reader.IsDBNull(reader.GetOrdinal("updcpn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("updcpn")),
                                    ExchNxtRefixDt = reader.IsDBNull(reader.GetOrdinal("exchnxtrefixdt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("exchnxtrefixdt")),
                                    ExchRefixFreq = (reader.IsDBNull(reader.GetOrdinal("exchrefixfreq"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("exchrefixfreq")),
                                    ExchLifeRefixFloor = (reader.IsDBNull(reader.GetOrdinal("exchliferefixfloor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("exchliferefixfloor")),
                                    HighDt52Week = reader.IsDBNull(reader.GetOrdinal("high_dt_52week")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("high_dt_52week")),
                                    High52Week = (reader.IsDBNull(reader.GetOrdinal("high_52week"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("high_52week")),
                                    LowDt52Week = reader.IsDBNull(reader.GetOrdinal("low_dt_52week")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("low_dt_52week")),
                                    Low52Week = (reader.IsDBNull(reader.GetOrdinal("low_52week"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("low_52week")),
                                    ChgPctYtd = (reader.IsDBNull(reader.GetOrdinal("chg_pct_ytd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("chg_pct_ytd")),
                                    TotalDebtToCurrentEV = (reader.IsDBNull(reader.GetOrdinal("total_debt_to_current_ev"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("total_debt_to_current_ev")),
                                    CDSSpreadTicker5Yr = reader["cds_spread_ticker_5y"] as string,
                                    GicsSectorName = reader["gics_sector_name"] as string,
                                    IssueDt = reader.IsDBNull(reader.GetOrdinal("issue_dt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("issue_dt")),
                                    NVCC = reader["nvcc"] as string,
                                    CumNonCum = reader["cum_noncum"] as string,
                                    TargetSpread = (reader.IsDBNull(reader.GetOrdinal("targetspread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("targetspread")),
                                    FixedFloat = reader["fixfloat"] as string,
                                    Benchmark = reader["benchmark"] as string,
                                    TermPremium = (reader.IsDBNull(reader.GetOrdinal("term_premium"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("term_premium")),
                                    RecoveryRate = (reader.IsDBNull(reader.GetOrdinal("recovery_rate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("recovery_rate")),
                                    DvdAmt = (reader.IsDBNull(reader.GetOrdinal("dvd_sh_last"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dvd_sh_last")),
                                    DvdExDt = reader.IsDBNull(reader.GetOrdinal("dvd_ex_dt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("dvd_ex_dt")),
                                    DvdPayDt = reader.IsDBNull(reader.GetOrdinal("dvd_pay_dt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("dvd_pay_dt")),
                                    DvdFreq = reader["dvd_freq"] as string,
                                    QualifyingDividends = reader["qlfy_dvds"] as string,

                                    FundCategory = reader["fundcategory"] as string,
                                    GeoLevel1 = reader["geolevel1"] as string,
                                    GeoLevel2 = reader["geolevel2"] as string,
                                    GeoLevel3 = reader["geolevel3"] as string,
                                    AssetClassLevel1 = reader["assetclasslevel1"] as string,
                                    AssetClassLevel2 = reader["assetclasslevel2"] as string,
                                    AssetClassLevel3 = reader["assetclasslevel3"] as string,
                                    Country = reader["country"] as string,
                                    CountryCode = reader["countrycode"] as string
                                };

                                data.MaturityDtAsString = DateUtils.ConvertDate(data.MaturityDt, "yyyy-MM-dd");
                                data.NextRefixDtAsString = DateUtils.ConvertDate(data.NextRefixDt, "yyyy-MM-dd");
                                data.PrevCpnDtAsString = DateUtils.ConvertDate(data.PrevCpnDt, "yyyy-MM-dd");
                                data.NxtCpnDtAsString = DateUtils.ConvertDate(data.NxtCpnDt, "yyyy-MM-dd");
                                data.NxtCallDtAsString = DateUtils.ConvertDate(data.NxtCallDt, "yyyy-MM-dd");
                                data.NxtExchDtAsString = DateUtils.ConvertDate(data.NxtExchDt, "yyyy-MM-dd");
                                data.HighDt52WeekAsString = DateUtils.ConvertDate(data.HighDt52Week, "yyyy-MM-dd");
                                data.LowDt52WeekAsString = DateUtils.ConvertDate(data.LowDt52Week, "yyyy-MM-dd");
                                data.IssueDtAsString = DateUtils.ConvertDate(data.IssueDt, "yyyy-MM-dd");
                                data.ExchNxtRefixDtAsString = DateUtils.ConvertDate(data.ExchNxtRefixDt, "yyyy-MM-dd");
                                data.DvdExDtAsString = DateUtils.ConvertDate(data.DvdExDt, "yyyy-MM-dd");
                                data.DvdPayDtAsString = DateUtils.ConvertDate(data.DvdPayDt, "yyyy-MM-dd");

                                if (!dict.ContainsKey(data.BBGId))
                                    dict.Add(data.BBGId, data);
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
        public IDictionary<string, IRCurve> GetInterestRateCurves()
        {
            IDictionary<string, IRCurve> dict = new Dictionary<string, IRCurve>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                IRCurve data;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetForwardRateCurvesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string curveName = reader["CurveName"] as string;
                                string curveType = reader["CurveType"] as string;
                                int period = reader.GetInt32(reader.GetOrdinal("Period"));
                                int tenor = reader.GetInt32(reader.GetOrdinal("Tenor"));

                                if (!dict.TryGetValue(curveName, out data))
                                {
                                    data = new IRCurve(GlobalConstants.NUM_PERIODS);
                                    dict.Add(curveName, data);
                                }

                                data.Period[period] = period;
                                if (reader.IsDBNull(reader.GetOrdinal("DiscountFactor")))
                                {
                                    data.DiscountFactor[period] = 0;
                                }
                                else
                                {
                                    data.DiscountFactor[period] = reader.GetDouble(reader.GetOrdinal("DiscountFactor")) / 100.0;
                                }

                                if (curveType.Equals("Spot", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    data.DiscountRate[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                }
                                else if (curveType.Equals("Forward", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (tenor == 1)
                                        data.ForwardRate1M[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                    else if (tenor == 3)
                                        data.ForwardRate3M[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                    else if (tenor == 60)
                                        data.ForwardRate5YR[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
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
        /// <param name="scenarioName"></param>
        /// <returns></returns>
        public IDictionary<string, IRCurve> GetInterestRateCurvesByScenario(string scenarioName)
        {
            IDictionary<string, IRCurve> dict = new Dictionary<string, IRCurve>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                IRCurve data;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetForwardRateCurvesByScenarioQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_ScenarioName", scenarioName);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string curveName = reader["CurveName"] as string;
                                string curveType = reader["CurveType"] as string;
                                int period = reader.GetInt32(reader.GetOrdinal("Period"));
                                int tenor = reader.GetInt32(reader.GetOrdinal("Tenor"));

                                if (!dict.TryGetValue(curveName, out data))
                                {
                                    data = new IRCurve(GlobalConstants.NUM_PERIODS);
                                    dict.Add(curveName, data);
                                }

                                data.Period[period] = period;
                                data.ScenarioName = reader["Scenario"] as string;
                                if (reader.IsDBNull(reader.GetOrdinal("DiscountFactor")))
                                {
                                    data.DiscountFactor[period] = 0;
                                }
                                else
                                {
                                    data.DiscountFactor[period] = reader.GetDouble(reader.GetOrdinal("DiscountFactor")) / 100.0;
                                }

                                if (curveType.Equals("Spot", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    data.DiscountRate[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                }
                                else if (curveType.Equals("Forward", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (tenor == 1)
                                        data.ForwardRate1M[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                    else if (tenor == 3)
                                        data.ForwardRate3M[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                    else if (tenor == 60)
                                        data.ForwardRate5YR[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
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
        public IDictionary<string, IRCurve> GetInterestRateCurvesNew()
        {
            IDictionary<string, IRCurve> dict = new Dictionary<string, IRCurve>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                IRCurve data;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetNewForwardRateCurvesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string curveName = reader["CurveName"] as string;
                                string curveType = reader["CurveType"] as string;
                                int period = reader.GetInt32(reader.GetOrdinal("Period"));
                                int tenor = reader.GetInt32(reader.GetOrdinal("Tenor"));

                                if (!dict.TryGetValue(curveName, out data))
                                {
                                    data = new IRCurve(GlobalConstants.NUM_PERIODS);
                                    dict.Add(curveName, data);
                                }

                                data.Period[period] = period;
                                if (reader.IsDBNull(reader.GetOrdinal("DiscountFactor")))
                                {
                                    data.DiscountFactor[period] = 0;
                                }
                                else
                                {
                                    data.DiscountFactor[period] = reader.GetDouble(reader.GetOrdinal("DiscountFactor")) / 100.0;
                                }

                                if (curveType.Equals("Spot", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    data.DiscountRate[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                }
                                else if (curveType.Equals("Forward", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (tenor == 1)
                                        data.ForwardRate1M[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                    else if (tenor == 3)
                                        data.ForwardRate3M[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
                                    else if (tenor == 60)
                                        data.ForwardRate5YR[period] = reader.GetDouble(reader.GetOrdinal("DiscountRate")) / 100.0;
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
        public IDictionary<string, RateIndexProxy> GetIndexProxies()
        {
            IDictionary<string, RateIndexProxy> dict = new Dictionary<string, RateIndexProxy>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetIndexProxyQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RateIndexProxy data = new RateIndexProxy
                                {
                                    Index = reader["IndexName"] as string,
                                    ProxyIndex = reader["IndexProxy"] as string,
                                    Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier")),
                                    Margin = (reader.IsDBNull(reader.GetOrdinal("Margin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Margin"))
                                };

                                dict.Add(data.Index, data);
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
        public IDictionary<string, IDictionary<int, SpotRate>> GetSpotRates()
        {
            IDictionary<string, IDictionary<int, SpotRate>> dict = new Dictionary<string, IDictionary<int, SpotRate>>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSpotRateQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string curveName = reader["CurveName"] as string;

                                IDictionary<int, SpotRate> spotRatesDict;
                                if (!dict.TryGetValue(curveName, out spotRatesDict))
                                {
                                    spotRatesDict = new Dictionary<int, SpotRate>();
                                    dict.Add(curveName, spotRatesDict);
                                }

                                SpotRate data = new SpotRate
                                {
                                    FIGI = reader["FIGI"] as string,
                                    Tenor = reader["Tenor"] as string,
                                    SortOrder = (reader.IsDBNull(reader.GetOrdinal("SortOrder"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SortOrder")),
                                    Value = (reader.IsDBNull(reader.GetOrdinal("RateVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RateVal"))
                                };

                                spotRatesDict.Add(data.SortOrder.GetValueOrDefault(), data);
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
        public IDictionary<string, IDictionary<int, SpotRate>> GetSpotRatesNew()
        {
            IDictionary<string, IDictionary<int, SpotRate>> dict = new Dictionary<string, IDictionary<int, SpotRate>>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetNewSpotRateQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string curveName = reader["CurveName"] as string;

                                IDictionary<int, SpotRate> spotRatesDict;
                                if (!dict.TryGetValue(curveName, out spotRatesDict))
                                {
                                    spotRatesDict = new Dictionary<int, SpotRate>();
                                    dict.Add(curveName, spotRatesDict);
                                }

                                SpotRate data = new SpotRate
                                {
                                    Tenor = reader["Tenor"] as string,
                                    SortOrder = (reader.IsDBNull(reader.GetOrdinal("SortOrder"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SortOrder")),
                                    Value = (reader.IsDBNull(reader.GetOrdinal("SpotRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SpotRate"))
                                };

                                if (!spotRatesDict.ContainsKey(data.SortOrder.GetValueOrDefault()))
                                    spotRatesDict.Add(data.SortOrder.GetValueOrDefault(), data);
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
        /// <param name="overrides"></param>
        public void SavePfdSecurityMasterOverrides(IList<PfdSecurity> overrides)
        {
            int updatedRecords = 0;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        using (MySqlCommand command = new MySqlCommand(SavePfdSecurityOverridesQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.Parameters.Add(new MySqlParameter("p_Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_BBGId", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Cusip", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_YellowKey", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_LongCompName", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_IssuerTicker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_IssuerParentTicker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Currency", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_AmtOutstanding", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_SecurityInArrears", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Series", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_RtgDBRS", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_RtgMoodys", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_RtgSP", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_FixFloat", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Multiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ResetIndex", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Spread", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_Cpn", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_CpnFreq", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_MaturityDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_IssueDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_CreditLevel", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_NextRefixDt", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_RefixFreq", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_LifeRefixFloor", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_LifeRefixCap", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_PrevCpnDt", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_NextCpnDt", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_DayCntDes", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Callable", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_NextCallDt", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_NextCallPrice", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_RedemptionVal", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_Exchangeable", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_NextExchDt", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_ExchBBGId", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ExchFixFloat", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ExchMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExchResetIndex", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ExchSpread", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExchRate", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExchNextRefixDt", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_ExchRefixFreq", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExchLifeRefixFloor", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_TermPremium", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_RecoveryRate", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_Benchmark", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_QualifyingDvds", MySqlDbType.VarChar));

                            foreach (PfdSecurity data in overrides)
                            {
                                command.Parameters[0].Value = data.Ticker;
                                command.Parameters[1].Value = data.BBGId;
                                command.Parameters[2].Value = data.CUSIP;
                                command.Parameters[3].Value = data.YellowKey;
                                command.Parameters[4].Value = data.CompanyNameLong;
                                command.Parameters[5].Value = data.IssuerTicker;
                                command.Parameters[6].Value = data.IssuerParentTicker;
                                command.Parameters[7].Value = data.Currency;
                                command.Parameters[8].Value = data.AmountOutstanding;
                                command.Parameters[9].Value = data.SecurityInArrears;
                                command.Parameters[10].Value = data.Series;
                                command.Parameters[11].Value = data.RtgDbrs;
                                command.Parameters[12].Value = data.RtgMoody;
                                command.Parameters[13].Value = data.RtgSP;
                                command.Parameters[14].Value = data.FixedFloat;
                                command.Parameters[15].Value = data.Multiplier;
                                command.Parameters[16].Value = data.ResetIndex;
                                command.Parameters[17].Value = data.FloatSpread;
                                command.Parameters[18].Value = data.Cpn;
                                command.Parameters[19].Value = data.CpnFreq;
                                command.Parameters[20].Value = data.MaturityDt;
                                command.Parameters[21].Value = data.IssueDt;
                                command.Parameters[22].Value = data.CreditLevel;
                                command.Parameters[23].Value = data.NextRefixDt;
                                command.Parameters[24].Value = data.RefixFreq;
                                command.Parameters[25].Value = data.LifeRefixFloor;
                                command.Parameters[26].Value = data.LifeRefixCap;
                                command.Parameters[27].Value = data.PrevCpnDt;
                                command.Parameters[28].Value = data.NxtCpnDt;
                                command.Parameters[29].Value = data.DayCntDes;
                                command.Parameters[30].Value = data.Callable;
                                command.Parameters[31].Value = data.NxtCallDt;
                                command.Parameters[32].Value = data.NxtCallPrice;
                                command.Parameters[33].Value = data.Par;
                                command.Parameters[34].Value = data.Exchangeable;
                                command.Parameters[35].Value = data.NxtExchDt;
                                command.Parameters[36].Value = data.ExchBBGId;
                                command.Parameters[37].Value = data.ExchFixedFloat;
                                command.Parameters[38].Value = data.ExchMultiplier;
                                command.Parameters[39].Value = data.ExchIndex;
                                command.Parameters[40].Value = data.ExchSpread;
                                command.Parameters[41].Value = data.ExchCpn;
                                command.Parameters[42].Value = data.ExchNxtRefixDt;
                                command.Parameters[43].Value = data.ExchRefixFreq;
                                command.Parameters[44].Value = data.ExchLifeRefixFloor;
                                command.Parameters[45].Value = data.TermPremium;
                                command.Parameters[46].Value = data.RecoveryRate;
                                command.Parameters[47].Value = data.Benchmark;
                                command.Parameters[48].Value = data.QualifyingDividends;

                                command.ExecuteNonQuery();

                                updatedRecords++;
                            }
                        }
                    }

                    _logger.LogInformation("Updating Pfd Security Master: " + updatedRecords);
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
        /// <param name="curveMembers"></param>
        public void SaveIRCurveRates(IList<IRCurveMember> curveMembers)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.IRCurveMember " +
                    "(CurveName, Tenor, TenorMonths, SpotRate, SortOrder) values ");

                _logger.LogInformation("Saving interest rate members...");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.IRCurveMember");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.IRCurveMember";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (IRCurveMember data in curveMembers)
                        {
                            if (!string.IsNullOrEmpty(data.CurveName))
                                sb.Append(string.Concat("'", data.CurveName, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (!string.IsNullOrEmpty(data.Tenor))
                                sb.Append(string.Concat("'", data.Tenor, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            int tenorMonths = TenorMonthsMap[data.Tenor];
                            sb.Append(tenorMonths).Append(DELIMITER);

                            if (data.SpotRate.HasValue)
                                sb.Append(data.SpotRate).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            int sortOrder = TenorMap[data.Tenor];
                            sb.Append(sortOrder);

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

                    _logger.LogInformation("Saved interest rate curve members");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving interest rate curve members");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="analytics"></param>
        public void SavePfdSecurityAnalytics(IList<PfdSecurityAnalytic> analytics)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.PfdSecurityAnalytic " +
                    "(BBGId, Figi, EffectiveDate, LastPrice, BidPrice, AskPrice, SpreadLastPrice, SpreadBidPrice, SpreadAskPrice, AdjSpreadLastPrice, AdjSpreadBidPrice, AdjSpreadAskPrice, TheoreticalSpread) values ");

                _logger.LogInformation("Saving pfd security analytics...");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string effectiveDate = analytics[0].EffectiveDateAsString;
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.PfdSecurityAnalytic");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.PfdSecurityAnalytic where EffectiveDate = '" + effectiveDate + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (PfdSecurityAnalytic data in analytics)
                        {
                            if (!string.IsNullOrEmpty(data.BBGId))
                                sb.Append(string.Concat("'", data.BBGId, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (!string.IsNullOrEmpty(data.Figi))
                                sb.Append(string.Concat("'", data.Figi, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (!string.IsNullOrEmpty(data.EffectiveDateAsString))
                                sb.Append(string.Concat("'", data.EffectiveDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.LastPrice.HasValue)
                                sb.Append(data.LastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.BidPrice.HasValue)
                                sb.Append(data.BidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AskPrice.HasValue)
                                sb.Append(data.AskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.SpreadLastPrice.HasValue)
                                sb.Append(data.SpreadLastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.SpreadBidPrice.HasValue)
                                sb.Append(data.SpreadBidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.SpreadAskPrice.HasValue)
                                sb.Append(data.SpreadAskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AdjSpreadLastPrice.HasValue)
                                sb.Append(data.AdjSpreadLastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AdjSpreadBidPrice.HasValue)
                                sb.Append(data.AdjSpreadBidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AdjSpreadAskPrice.HasValue)
                                sb.Append(data.AdjSpreadAskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.TheoreticalSpread.HasValue)
                                sb.Append(data.TheoreticalSpread);
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

                    _logger.LogInformation("Saved pfd security analytics");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving pfd security analytics");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="analytics"></param>
        public void SavePfdSecurityAnalyticsNew(IList<PfdSecurityAnalytic> analytics)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.PfdSecurityAnalyticNew " +
                    "(BBGId, Figi, EffectiveDate, LastPrice, BidPrice, AskPrice, SpreadLastPrice, SpreadBidPrice, SpreadAskPrice, AdjSpreadLastPrice, AdjSpreadBidPrice, AdjSpreadAskPrice, TheoreticalSpread) values ");

                _logger.LogInformation("Saving pfd security analytics...");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string effectiveDate = analytics[0].EffectiveDateAsString;
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.PfdSecurityAnalyticNew where EffectiveDate = '" + effectiveDate + "'");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.PfdSecurityAnalyticNew where EffectiveDate = '" + effectiveDate + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (PfdSecurityAnalytic data in analytics)
                        {
                            if (!string.IsNullOrEmpty(data.BBGId))
                                sb.Append(string.Concat("'", data.BBGId, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (!string.IsNullOrEmpty(data.Figi))
                                sb.Append(string.Concat("'", data.Figi, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (!string.IsNullOrEmpty(data.EffectiveDateAsString))
                                sb.Append(string.Concat("'", data.EffectiveDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.LastPrice.HasValue)
                                sb.Append(data.LastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.BidPrice.HasValue)
                                sb.Append(data.BidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AskPrice.HasValue)
                                sb.Append(data.AskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.SpreadLastPrice.HasValue)
                                sb.Append(data.SpreadLastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.SpreadBidPrice.HasValue)
                                sb.Append(data.SpreadBidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.SpreadAskPrice.HasValue)
                                sb.Append(data.SpreadAskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AdjSpreadLastPrice.HasValue)
                                sb.Append(data.AdjSpreadLastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AdjSpreadBidPrice.HasValue)
                                sb.Append(data.AdjSpreadBidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.AdjSpreadAskPrice.HasValue)
                                sb.Append(data.AdjSpreadAskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            if (data.TheoreticalSpread.HasValue)
                                sb.Append(data.TheoreticalSpread);
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

                    _logger.LogInformation("Saved pfd security analytics");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving pfd security analytics");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<PfdSecurityAnalytic> GetPfdSecurityAnalytics(string ticker, string startDate, string endDate)
        {
            IList<PfdSecurityAnalytic> list = new List<PfdSecurityAnalytic>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = @"select a.*
                        from
                        (
                        select
	                        n.BBGId
                            ,n.Figi
                            ,n.EffectiveDate
                            ,n.LastPrice
                            ,n.BidPrice
                            ,n.AskPrice
                            ,n.SpreadLastPrice
                            ,n.SpreadBidPrice
                            ,n.SpreadAskPrice
                            ,n.AdjSpreadLastPrice
                            ,n.AdjSpreadBidPrice
                            ,n.AdjSpreadAskPrice
                            ,n.TheoreticalSpread
                        from almitasc_ACTradingBBGData.PfdSecurityAnalytic n
                        union all
                        select	
	                        o.BBGId
                            ,null as Figi
                            ,o.asofdate as EffectiveDate
                            ,o.last_price as LastPrice
                            ,null as BidPrice
                            ,null as AskPrice
                            ,o.max_zspread as SpreadLastPrice
                            ,null as SpreadBidPrice
                            ,null as SpreadAskPrice
                            ,null as AdjSpreadLastPrice
                            ,null as AdjSpreadBidPrice
                            ,null as AdjSpreadAskPrice
                            ,null as TheoreticalSpread
                        from almitasc_ACTrading.globalpfd_pricinghistory o
                        ) a
                        where 1=1";

                    if (!string.IsNullOrEmpty(startDate))
                        sql = sql + " and a.EffectiveDate >= '" + startDate + "'";
                    if (!string.IsNullOrEmpty(endDate))
                        sql = sql + " and a.EffectiveDate <= '" + endDate + "'";
                    if (!string.IsNullOrEmpty(ticker))
                        sql = sql + " and a.BBGId = '" + ticker + "'";

                    sql += " order by a.BBGId, a.EffectiveDate desc";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PfdSecurityAnalytic data = new PfdSecurityAnalytic
                                {
                                    BBGId = reader["BBGId"] as string,
                                    Figi = reader["Figi"] as string,

                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    BidPrice = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice")),
                                    AskPrice = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice")),
                                    SpreadLastPrice = (reader.IsDBNull(reader.GetOrdinal("SpreadLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SpreadLastPrice")),
                                    SpreadBidPrice = (reader.IsDBNull(reader.GetOrdinal("SpreadBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SpreadBidPrice")),
                                    SpreadAskPrice = (reader.IsDBNull(reader.GetOrdinal("SpreadAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SpreadAskPrice")),
                                    AdjSpreadLastPrice = (reader.IsDBNull(reader.GetOrdinal("AdjSpreadLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjSpreadLastPrice")),
                                    AdjSpreadBidPrice = (reader.IsDBNull(reader.GetOrdinal("AdjSpreadBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjSpreadBidPrice")),
                                    AdjSpreadAskPrice = (reader.IsDBNull(reader.GetOrdinal("AdjSpreadAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjSpreadAskPrice")),
                                    TheoreticalSpread = (reader.IsDBNull(reader.GetOrdinal("TheoreticalSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TheoreticalSpread")),

                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

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
        public IDictionary<string, PfdSecurityExt> GetPfdSecurityExtData()
        {
            IDictionary<string, PfdSecurityExt> dict = new Dictionary<string, PfdSecurityExt>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPfdSecurityExtQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PfdSecurityExt data = new PfdSecurityExt
                                {
                                    BBGId = reader["bbgid"] as string,
                                    SpreadAdjType = (reader.IsDBNull(reader.GetOrdinal("spread_adj_type"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("spread_adj_type")),
                                    SpreadAdj = (reader.IsDBNull(reader.GetOrdinal("spread_adj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("spread_adj"))
                                };

                                if (!dict.ContainsKey(data.BBGId))
                                    dict.Add(data.BBGId, data);
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
        public IDictionary<string, PfdSecurityMap> GetPfdTickerBloombergCodeMap()
        {
            IDictionary<string, PfdSecurityMap> dict = new Dictionary<string, PfdSecurityMap>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPfdSecurityMapQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PfdSecurityMap data = new PfdSecurityMap
                                {
                                    Ticker = reader["SedolTicker"] as string,
                                    BBGId = reader["BBGTicker"] as string,
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

        private const string GetPfdSecurityMasterQuery = "call almitasc_ACTradingBBGData.spGetPfdSecurityMasterProd";
        private const string GetPfdSecurityExtQuery = "select m.bbgid, m.spread_adj_type, d.spread_adj"
                                        + " from almitasc_ACTradingBBGData.globalmarket_canadianpfd_spreadadjtype m"
                                        + " join almitasc_ACTradingBBGData.globalmarket_canadianpfd_spreadadj d on (m.spread_adj_type = d.spread_adj_type)"
                                        + " where m.spread_adj_type is not null and d.spread_adj is not null";
        private const string GetInterestRateCurvesQuery = "select * from almitasc_ACTradingBBGData.RateCurveVal order by CurveName, CurveType, Tenor, Period asc";
        private const string GetForwardRateCurvesQuery = "call almitasc_ACTradingBBGData.spPfdRateCurves";
        private const string GetForwardRateCurvesByScenarioQuery = "spPfdRateCurvesByScenario";
        private const string GetNewForwardRateCurvesQuery = "call almitasc_ACTradingBBGData.spGetPfdRateCurves";
        private const string GetIndexProxyQuery = "select * from almitasc_ACTradingBBGData.RateIndexProxy";
        private const string GetSpotRateQuery = "call almitasc_ACTradingBBGData.spGetPfdSpotRates";
        private const string GetNewSpotRateQuery = "call almitasc_ACTradingBBGData.spGetPfdSpotRatesNew";
        private const string SavePfdSecurityOverridesQuery = "spSavePfdSecurityOverrides";
        private const string GetPfdSecurityMapQuery = "call almitasc_ACTradingBBGData.spGetPfdBloombergTickers";

        private readonly IDictionary<string, int> TenorMap = new Dictionary<string, int>(StringComparer.CurrentCultureIgnoreCase) {
            { "1 Mo", 1},
            { "2 Mo", 2},
            { "3 Mo", 3},
            { "6 Mo", 4},
            { "9 Mo", 5},
            { "1 Yr", 6},
            { "2 Yr", 7},
            { "3 Yr", 8},
            { "4 Yr", 9},
            { "5 Yr", 10},
            { "7 Yr", 11},
            { "9 Yr", 12},
            { "10 Yr", 13},
            { "12 Yr", 14},
            { "15 Yr", 15},
            { "20 Yr", 16},
            { "30 Yr", 17},
            { "50 Yr", 18}
        };

        private readonly IDictionary<string, int> TenorMonthsMap = new Dictionary<string, int>(StringComparer.CurrentCultureIgnoreCase) {
            { "1 Mo", 1},
            { "2 Mo", 2},
            { "3 Mo", 3},
            { "6 Mo", 6},
            { "9 Mo", 9},
            { "1 Yr", 12},
            { "2 Yr", 24},
            { "3 Yr", 36},
            { "4 Yr", 48},
            { "5 Yr", 60},
            { "7 Yr", 84},
            { "9 Yr", 108},
            { "10 Yr", 120},
            { "12 Yr", 144},
            { "15 Yr", 180},
            { "20 Yr", 240 },
            { "30 Yr", 360},
            { "50 Yr", 600}
        };
    }
}
