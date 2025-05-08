using aACTrader.DAO.Interface;
using aCommons.Agile;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class AgileDao : IAgileDao
    {
        private readonly ILogger<AgileDao> _logger;
        private const string DELIMITER = ",";

        public AgileDao(ILogger<AgileDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing AgileDao...");
        }

        public IList<AgileDailyPerf> GetDailyPerf(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<AgileDailyPerf> list = new List<AgileDailyPerf>();

            try
            {
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                string sql = GetDailyPerfQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and Ticker like '%" + ticker + "%'";

                if (!string.IsNullOrEmpty(startDateAsString))
                    sql += " and AsofDate >= '" + startDateAsString + "'";

                if (!string.IsNullOrEmpty(endDateAsString))
                    sql += " and AsofDate <= '" + endDateAsString + "'";

                sql += " Order by AsofDate desc, Ticker, FundName";


                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                AgileDailyPerf data = new AgileDailyPerf();
                                data.AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate"));
                                data.FundName = reader["Fund"] as string;
                                data.Account = reader["Account"] as string;
                                data.Ticker = reader["Ticker"] as string;
                                data.SecDesc = reader["SecDesc"] as string;
                                data.LongShort = reader["LongShort"] as string;
                                data.Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty"));
                                data.DailyPL = (reader.IsDBNull(reader.GetOrdinal("DailyPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPL"));
                                data.MTDPL = (reader.IsDBNull(reader.GetOrdinal("MTDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPL"));
                                data.PBDPL = (reader.IsDBNull(reader.GetOrdinal("PBDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PBDPL"));

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

        public IList<AgilePosition> GetPositions(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<AgilePosition> list = new List<AgilePosition>();

            try
            {
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                string sql = GetPositionsQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and Ticker like '%" + ticker + "%'";

                if (!string.IsNullOrEmpty(startDateAsString))
                    sql += " and AsofDate >= '" + startDateAsString + "'";

                if (!string.IsNullOrEmpty(endDateAsString))
                    sql += " and AsofDate <= '" + endDateAsString + "'";

                sql += " Order by AsofDate desc, Ticker, FundName";


                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                AgilePosition data = new AgilePosition();
                                data.AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate"));
                                data.FundName = reader["Fund"] as string;
                                data.FundShortName = reader["FundShortName"] as string;
                                data.Account = reader["Account"] as string;
                                data.LongShort = reader["LongShort"] as string;
                                data.DealId = reader["DealId"] as string;
                                data.InstrId = (reader.IsDBNull(reader.GetOrdinal("InstrId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("InstrId"));
                                data.InstrType1 = reader["InstrType1"] as string;
                                data.InstrType2 = reader["InstrType2"] as string;
                                data.InstrSubType = reader["InstrSubType"] as string;
                                data.Cusip = reader["Cusip"] as string;
                                data.ISIN = reader["ISIN"] as string;
                                data.Sedol = reader["Sedol"] as string;
                                data.Ticker = reader["Ticker"] as string;
                                data.SecDesc = reader["SecDesc"] as string;
                                data.Custodian = reader["Custodian"] as string;
                                data.CustodianAcct = reader["CustodianAcct"] as string;
                                data.CustodianAcctInstrId = reader["CustodianAcctInstrId"] as string;
                                data.Curr = reader["Curr"] as string;
                                data.Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty"));
                                data.FairValue = (reader.IsDBNull(reader.GetOrdinal("FairValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FairValue"));
                                data.FairValueLocal = (reader.IsDBNull(reader.GetOrdinal("FairValueLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FairValueLocal"));
                                data.NMV = (reader.IsDBNull(reader.GetOrdinal("NMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NMV"));
                                data.NMVLocal = (reader.IsDBNull(reader.GetOrdinal("NMVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NMVLocal"));
                                data.MTDPL = (reader.IsDBNull(reader.GetOrdinal("MTDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPL"));
                                data.MTDPLLocal = (reader.IsDBNull(reader.GetOrdinal("MTDPLLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPLLocal"));
                                data.MTDPLRealized = (reader.IsDBNull(reader.GetOrdinal("MTDRealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDRealizedPL"));
                                data.MTDPLUnRealized = (reader.IsDBNull(reader.GetOrdinal("MTDUnRealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDUnRealizedPL"));
                                data.MTDPLUnRealizedTotal = (reader.IsDBNull(reader.GetOrdinal("TotalMTDUnRealized"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalMTDUnRealized"));
                                data.MTDPLChng = (reader.IsDBNull(reader.GetOrdinal("MTDPLChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPLChng"));
                                data.CostPerLot = (reader.IsDBNull(reader.GetOrdinal("CostPerLot"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostPerLot"));
                                data.NetAvgCost = (reader.IsDBNull(reader.GetOrdinal("NetAvgCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAvgCost"));
                                data.NetAvgCostLocal = (reader.IsDBNull(reader.GetOrdinal("NetAvgCostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAvgCostLocal"));
                                data.DailyPL = (reader.IsDBNull(reader.GetOrdinal("DailyPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPL"));
                                data.ITDPL = (reader.IsDBNull(reader.GetOrdinal("ITDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ITDPL"));
                                data.YTDPL = (reader.IsDBNull(reader.GetOrdinal("YTDPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDPL"));
                                data.DailyPLPctNav = (reader.IsDBNull(reader.GetOrdinal("DailyPLPctNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPLPctNav"));
                                data.MTDPLPctNav = (reader.IsDBNull(reader.GetOrdinal("MTDPLPctNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDPLPctNav"));
                                data.YTDPLPctNav = (reader.IsDBNull(reader.GetOrdinal("YTDPLPctNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDPLPctNav"));

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

        public IList<AgileTrade> GetTrades(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<AgileTrade> list = new List<AgileTrade>();

            try
            {
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                string sql = GetTradesQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and Ticker like '%" + ticker + "%'";

                if (!string.IsNullOrEmpty(startDateAsString))
                    sql += " and AsofDate >= '" + startDateAsString + "'";

                if (!string.IsNullOrEmpty(endDateAsString))
                    sql += " and AsofDate <= '" + endDateAsString + "'";

                sql += " Order by AsofDate desc, Ticker, FundName";


                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                AgileTrade data = new AgileTrade();
                                data.AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate"));
                                data.TradeId = (reader.IsDBNull(reader.GetOrdinal("TradeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeId"));
                                data.FundName = reader["Fund"] as string;
                                data.Ticker = reader["Ticker"] as string;
                                data.SecDesc = reader["SecDesc"] as string;
                                data.InstrSubType = reader["InstrSubType"] as string;
                                data.TradeDate = reader.IsDBNull(reader.GetOrdinal("TradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate"));
                                data.SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate"));
                                data.TransType = reader["TransType"] as string;
                                data.Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty"));
                                data.Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price"));
                                data.GrossComm = (reader.IsDBNull(reader.GetOrdinal("GrossComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossComm"));
                                data.Fees = (reader.IsDBNull(reader.GetOrdinal("Fees"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Fees"));
                                data.NetProceeds = (reader.IsDBNull(reader.GetOrdinal("NetProceeds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetProceeds"));
                                data.Custodian = reader["Custodian"] as string;
                                data.CustodianAcct = reader["CustodianAcct"] as string;
                                data.TradeCanceled = reader["TradeCanceled"] as string;
                                data.CustodianAcct = reader["CustodianAcct"] as string;
                                data.Counterparty = reader["Counterparty"] as string;
                                data.FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate"));
                                data.TradeModifyDate = reader.IsDBNull(reader.GetOrdinal("TradeModifyDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeModifyDate"));

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

        private const string GetPositionsQuery = "select"
            + " (case when p.FundName = 'Almitas Opportunity Fund LP' then 'Opp'"
            + "       when p.FundName = 'Almitas Tactical Fund LP' then 'Tac'"
            + "       else p.FundName end) as Fund, p.*"
            + " from Agile.Position p";

        private const string GetTradesQuery = "select"
            + " (case when t.FundName = 'Almitas Opportunity Fund LP' then 'Opp'"
            + "       when t.FundName = 'Almitas Tactical Fund LP' then 'Tac'"
            + "       else t.FundName end) as Fund, t.*"
            + " from Agile.Trade t";

        private const string GetDailyPerfQuery = "select"
            + " (case when t.FundName = 'Almitas Opportunity Fund LP' then 'Opp'"
            + "       when t.FundName = 'Almitas Tactical Fund LP' then 'Tac'"
            + "       else t.FundName end) as Fund"
            + " ,(case when t.FundName = 'Almitas Opportunity Fund LP' then OppFundPBDPL"
            + "        when t.FundName = 'Almitas Tactical Fund LP' then TacFundPBDPL end) as PBDPL"
            + " ,t.*"
            + " from Agile.DailyPerf t";
    }
}
