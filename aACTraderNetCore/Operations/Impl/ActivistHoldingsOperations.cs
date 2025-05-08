using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class ActivistHoldingsOperations
    {
        private readonly ILogger<ActivistHoldingsOperations> _logger;
        private readonly HoldingsDao _holdingsDao;
        private readonly CommonDao _commonDao;

        public ActivistHoldingsOperations(ILogger<ActivistHoldingsOperations> logger
            , HoldingsDao holdingsDao
            , CommonDao commonDao)
        {
            _logger = logger;
            _holdingsDao = holdingsDao;
            _commonDao = commonDao;
            _logger.LogInformation("Initializing ActivistHoldingsOperations...");
        }

        public IList<ActivistHolding> GetActivistHoldings(string holder, string ticker)
        {
            return _holdingsDao.GetActivistHoldings(holder, ticker);
        }

        public IList<ActivistHolding> GetActivistHoldingsHistory(string holder, string ticker)
        {
            return _holdingsDao.GetActivistHoldingsHistory(holder, ticker);
        }

        public IList<ActivistScore> GetActivistScores(string country)
        {
            return _holdingsDao.GetActivistScores(country);
        }

        public void SaveActivistScores(CachingService cachingService, IList<ActivistScore> activistScores)
        {
            try
            {
                _logger.LogInformation("Saving Activist Scores...");
                _holdingsDao.SaveActivistScores(activistScores);

                _logger.LogInformation("Populating Expected Alpha Model Scores...");
                IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict = _commonDao.GetFundAlphaModelScores();
                cachingService.Remove(CacheKeys.ALPHA_MODEL_SCORES);
                cachingService.Add(CacheKeys.ALPHA_MODEL_SCORES, fundAlphaModelScoresDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Update Scores In Fund Forecast...");
                IDictionary<string, FundForecast> fundForecastDict = cachingService.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                foreach (KeyValuePair<string, FundAlphaModelScores> kvp in fundAlphaModelScoresDict)
                {
                    string ticker = kvp.Key;
                    FundAlphaModelScores fundAlphaModelScores = kvp.Value;

                    if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                    {
                        fundForecast.AScore = fundAlphaModelScores.ActivistScore;
                        fundForecast.RawAScore = fundAlphaModelScores.RawActivistScore;
                        fundForecast.LiqCost = fundAlphaModelScores.LiquidityCost;
                        fundForecast.ShareBB = fundAlphaModelScores.ShareBuyback;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving activist scores");
            }
        }
    }
}