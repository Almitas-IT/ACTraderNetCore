using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class DataOverrideDao : IDataOverrideDao
    {
        private readonly ILogger<DataOverrideDao> _logger;

        public DataOverrideDao(ILogger<DataOverrideDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing DataOverrideDao...");
        }

        public IDictionary<string, GlobalDataOverride> GetGlobalDataOverrides()
        {
            IDictionary<string, GlobalDataOverride> dict = new Dictionary<string, GlobalDataOverride>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetGlobalDataOverridesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                GlobalDataOverride data = new GlobalDataOverride
                                {
                                    Country = reader["Country"] as string,
                                    Sector = reader["Sector"] as string,
                                    UserName = reader["UserName"] as string,

                                    EAMultiplier = (reader.IsDBNull(reader.GetOrdinal("EAMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAMultiplier")),
                                    EAAdjFactor = (reader.IsDBNull(reader.GetOrdinal("EAAdjFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAAdjFactor")),
                                    EAPluralityStaggeredMultiplier = (reader.IsDBNull(reader.GetOrdinal("EAPluralityStaggeredMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAPluralityStaggeredMultiplier")),
                                    EAPluralityNonStaggeredMultiplier = (reader.IsDBNull(reader.GetOrdinal("EAPluralityNonStaggeredMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAPluralityNonStaggeredMultiplier")),
                                    EAMajorityStaggeredMultiplier = (reader.IsDBNull(reader.GetOrdinal("EAMajorityStaggeredMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAMajorityStaggeredMultiplier")),
                                    EAMajorityNonStaggeredMultiplier = (reader.IsDBNull(reader.GetOrdinal("EAMajorityNonStaggeredMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EAMajorityNonStaggeredMultiplier")),
                                    LastUpdated = (reader.IsDBNull(reader.GetOrdinal("LastUpdated"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastUpdated"))
                                };

                                data.LastUpdatedAsString = DateUtils.ConvertDate(data.LastUpdated, "yyyy-MM-dd");

                                if (string.IsNullOrEmpty(data.Sector))
                                    data.Sector = "All";
                                string key = string.Join(data.Country, "|", data.Sector);

                                dict.Add(data.Country, data);
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

        public void SaveGlobalOverrides(IList<GlobalDataOverride> overrides)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        using (MySqlCommand command = new MySqlCommand(SaveGlobalDataOverridesQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.Parameters.Add(new MySqlParameter("p_Country", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Sector", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_EAMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_EAAdjFactor", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_EAPluralityStaggeredMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_EAPluralityNonStaggeredMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_EAMajorityStaggeredMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_EAMajorityNonStaggeredMultiplier", MySqlDbType.Decimal));

                            foreach (GlobalDataOverride data in overrides)
                            {
                                command.Parameters[0].Value = data.Country;
                                command.Parameters[1].Value = data.Sector;
                                command.Parameters[2].Value = data.EAMultiplier;
                                command.Parameters[3].Value = data.EAAdjFactor;
                                command.Parameters[4].Value = data.EAPluralityStaggeredMultiplier;
                                command.Parameters[5].Value = data.EAPluralityNonStaggeredMultiplier;
                                command.Parameters[6].Value = data.EAMajorityStaggeredMultiplier;
                                command.Parameters[7].Value = data.EAMajorityNonStaggeredMultiplier;

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

        private const string GetGlobalDataOverridesQuery = "select * from almitasc_ACTradingBBGData.GlobalOverride";
        private const string SaveGlobalDataOverridesQuery = "call almitasc_ACTradingBBGData.spPopulateFundAlerts";
    }
}
