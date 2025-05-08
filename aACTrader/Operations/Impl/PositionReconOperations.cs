using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class PositionReconOperations
    {
        private readonly ILogger<PositionReconOperations> _logger;
        private readonly CommonDao _commonDao;
        private readonly HoldingsDao _holdingsDao;

        public PositionReconOperations(ILogger<PositionReconOperations> logger
            , CommonDao commonDao
            , HoldingsDao holdingsDao)
        {
            _logger = logger;
            _commonDao = commonDao;
            _holdingsDao = holdingsDao;
        }

        public IDictionary<string, PositionRecon> GenerateReconReport(string asofDateString)
        {
            IDictionary<string, PositionRecon> positionReconDict = new Dictionary<string, PositionRecon>();

            DateTime asofDate = DateUtils.ConvertToDate(asofDateString).GetValueOrDefault();

            // current holdings
            IDictionary<string, Holding> holdingsByPortDict = _commonDao.GetHoldingHistoryByPort(asofDate);

            // get max as of date of the holdings based on the as of date
            DateTime positionMaxDate = asofDate;
            foreach (KeyValuePair<string, Holding> kvp in holdingsByPortDict)
            {
                string positionKey = kvp.Key;
                Holding holding = kvp.Value;

                positionMaxDate = holding.AsofDate.GetValueOrDefault();
                break;
            }

            // previous day holdings
            IDictionary<string, Holding> previousDayHoldingsByPortDict = _commonDao.GetHoldingHistoryByPort(positionMaxDate);

            // get allocations
            IDictionary<string, Trade> tradeDict = _holdingsDao.GetTradeHistory(asofDate);

            // process previous day holdings
            foreach (KeyValuePair<string, Holding> kvp in previousDayHoldingsByPortDict)
            {
                string positionKey = kvp.Key;
                Holding holding = kvp.Value;

                PositionRecon positionRecon;
                if (!positionReconDict.TryGetValue(positionKey, out positionRecon))
                {
                    positionRecon = new PositionRecon
                    {
                        PositionKey = positionKey,
                        FundName = holding.FundName,
                        Broker = holding.Broker,
                        Ticker = holding.SecurityTicker,
                        BrokerTicker = holding.HoldingTicker,
                        BrokerSystemId = holding.BrokerySystemId,
                        YellowKey = holding.YellowKey,
                        Currency = holding.Currency,
                        SecurityName = holding.SecurityName,
                        Figi = holding.FIGI,

                        PreviousPositionDate = holding.AsofDate,
                        PreviousPositionDateAsString = DateUtils.ConvertDate(holding.AsofDate, "yyyy-MM-dd"),
                        PreviousPosition = holding,
                        PreviousPositionCount = 1
                    };

                    positionRecon.DerivedPosition = CommonUtils.AddNullableDoubles(positionRecon.DerivedPosition, holding.Position);
                    positionRecon.DerivedMarketValue = CommonUtils.AddNullableDoubles(positionRecon.DerivedMarketValue, holding.MarketValue);

                    positionReconDict.Add(positionKey, positionRecon);
                }
            }

            // process trades
            foreach (KeyValuePair<string, Trade> kvp in tradeDict)
            {
                string positionKey = kvp.Key;
                Trade trade = kvp.Value;

                PositionRecon positionRecon;
                if (!positionReconDict.TryGetValue(positionKey, out positionRecon))
                {
                    positionRecon = new PositionRecon
                    {
                        PositionKey = positionKey,
                        FundName = trade.Portfolio,
                        Broker = trade.Broker,
                        Ticker = trade.SecTicker,
                        BrokerTicker = trade.Ticker,
                        YellowKey = trade.YellowKey,
                        Currency = trade.TradeCurrency,
                        SecurityName = trade.SecurityName,
                        Figi = trade.Figi,

                        TradeDate = trade.TradeDate,
                        TradeDateAsString = DateUtils.ConvertDate(trade.TradeDate, "yyyy-MM-dd"),
                        Trade = trade,
                        TradeCount = 1
                    };

                    positionRecon.DerivedPosition = CommonUtils.AddNullableDoubles(positionRecon.DerivedPosition, trade.Position);
                    positionRecon.DerivedMarketValue = CommonUtils.AddNullableDoubles(positionRecon.DerivedMarketValue, trade.MarketValue);

                    positionReconDict.Add(positionKey, positionRecon);
                }
                else
                {
                    positionRecon.TradeDate = trade.TradeDate;
                    positionRecon.TradeDateAsString = DateUtils.ConvertDate(trade.TradeDate, "yyyy-MM-dd");
                    positionRecon.Trade = trade;
                    positionRecon.TradeCount = positionRecon.TradeCount.GetValueOrDefault() + 1;

                    positionRecon.DerivedPosition = CommonUtils.AddNullableDoubles(positionRecon.DerivedPosition, trade.Position);
                    positionRecon.DerivedMarketValue = CommonUtils.AddNullableDoubles(positionRecon.DerivedMarketValue, trade.MarketValue);
                }
            }

            // process holdings
            foreach (KeyValuePair<string, Holding> kvp in holdingsByPortDict)
            {
                string positionKey = kvp.Key;
                Holding holding = kvp.Value;

                PositionRecon positionRecon;
                if (!positionReconDict.TryGetValue(positionKey, out positionRecon))
                {
                    positionRecon = new PositionRecon
                    {
                        PositionKey = positionKey,
                        FundName = holding.FundName,
                        Broker = holding.Broker,
                        Ticker = holding.SecurityTicker,
                        BrokerTicker = holding.HoldingTicker,
                        BrokerSystemId = holding.BrokerySystemId,
                        YellowKey = holding.YellowKey,
                        Currency = holding.Currency,
                        SecurityName = holding.SecurityName,
                        Figi = holding.FIGI,

                        PositionDate = holding.AsofDate,
                        PositionDateAsString = DateUtils.ConvertDate(holding.AsofDate, "yyyy-MM-dd"),
                        Position = holding,
                        PositionCount = 1
                    };

                    positionReconDict.Add(positionKey, positionRecon);
                }
                else
                {
                    positionRecon.PositionDate = holding.AsofDate;
                    positionRecon.PositionDateAsString = DateUtils.ConvertDate(holding.AsofDate, "yyyy-MM-dd");
                    positionRecon.Position = holding;
                    positionRecon.PositionCount = positionRecon.PositionCount.GetValueOrDefault() + 1;
                }
            }

            foreach (KeyValuePair<string, PositionRecon> kvp in positionReconDict)
            {
                PositionRecon positionRecon = kvp.Value;

                //if (positionRecon.Ticker.Equals("NYMT US"))
                //    _logger.LogDebug(positionRecon.Ticker);

                // calculate position variance
                // don't check for currency positions
                if (!positionRecon.Ticker.Equals("USD", StringComparison.CurrentCultureIgnoreCase) &&
                    !positionRecon.Ticker.StartsWith("Swap", StringComparison.CurrentCultureIgnoreCase))
                {
                    positionRecon.PositionVariance = positionRecon.DerivedPosition;
                    if (positionRecon.Position != null && positionRecon.DerivedPosition.HasValue)
                        positionRecon.PositionVariance = CommonUtils.SubtractNullableDoubles(positionRecon.PositionVariance, positionRecon.Position.Position);
                    else if (positionRecon.Position != null && !positionRecon.DerivedPosition.HasValue)
                        positionRecon.PositionVariance = positionRecon.Position.Position;
                }
                else
                {
                    _logger.LogDebug("Ignoring Currency and Swaps for Position Variance check: " + positionRecon.Ticker);
                }

                // calculate price change
                if (positionRecon.Position != null
                    && positionRecon.Position.Price.HasValue
                    && positionRecon.PreviousPosition != null
                    && positionRecon.PreviousPosition.Price.HasValue)
                {
                    double priceChange = (positionRecon.Position.Price.GetValueOrDefault() / positionRecon.PreviousPosition.Price.GetValueOrDefault()) - 1;
                    if (!Double.IsInfinity(priceChange) && !Double.IsNaN(priceChange))
                        positionRecon.PriceChng = priceChange;
                }

                // calculate market value change
                if (positionRecon.Position != null
                    && positionRecon.Position.MarketValue.HasValue
                    && positionRecon.Position.MarketValue.GetValueOrDefault() > 0
                    && positionRecon.DerivedMarketValue.HasValue)
                {
                    double marketValueChange = (positionRecon.Position.MarketValue.GetValueOrDefault() / positionRecon.DerivedMarketValue.GetValueOrDefault()) - 1;
                    if (!Double.IsInfinity(marketValueChange) && !Double.IsNaN(marketValueChange))
                        positionRecon.MarketValueChng = marketValueChange;
                }

                positionRecon.PositionBreakFlag = 0;
                if (Math.Abs(positionRecon.PositionVariance.GetValueOrDefault()) > 0)
                    positionRecon.PositionBreakFlag = 1;

                //if (Math.Abs(positionRecon.PriceChng.GetValueOrDefault()) > 0.10)
                //    positionRecon.ReconBreakFlag = 8;

                positionRecon.MarketValueBreakFlag = 0;
                if (Math.Abs(positionRecon.MarketValueChng.GetValueOrDefault()) > 0.15)
                    positionRecon.MarketValueBreakFlag = 7;
            }

            return positionReconDict;
        }
    }
}