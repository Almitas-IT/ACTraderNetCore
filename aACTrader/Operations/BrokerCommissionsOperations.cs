using aCommons;
using aCommons.DTO;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace aACTrader.Operations
{
    public class BrokerCommissionOperations
    {
        private readonly ILogger<BrokerCommissionOperations> _logger;
        private readonly CachingService _cache;
        private const double DEFAULT_COMMISSION_PER_SHARE = 0.01; // (1 cent per share)
        private const double DEFAULT_COMMISSION_BPS = 0.0015; // 15 bps

        public BrokerCommissionOperations(ILogger<BrokerCommissionOperations> logger
            , CachingService cache)
        {
            this._logger = logger;
            this._cache = cache;
        }

        private double GetCommisionPerShare(double qty)
        {
            return qty * DEFAULT_COMMISSION_PER_SHARE;
        }

        private double GetCommisionPerShare(double qty, double commisionPerShare)
        {
            return qty * commisionPerShare;
        }

        private double GetCommisionByMV(double qty, double price, double commissionRate)
        {
            return qty * price * commissionRate;
        }

        private double GetCommisionByMV(double mv, double commissionRate)
        {
            return mv * commissionRate;
        }

        private double GetJPMCommission(TradeExecutionSummaryTO tradeExecution)
        {
            double commission = 0;
            if (tradeExecution.Bkr.Contains("EURO"))
                commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.001;
            else if ("USD".Equals(tradeExecution.Curr) || "CAD".Equals(tradeExecution.Curr))
            {
                if (tradeExecution.PB35MV.HasValue)
                {
                    commission = tradeExecution.PB35MV.GetValueOrDefault() * 0.0008;
                    commission += tradeExecution.PG35Qty.GetValueOrDefault() * 0.0035;
                }
                else
                    commission = tradeExecution.TotTrdQty.GetValueOrDefault() * 0.0035;
            }
            else
            {
                //if (tradeExecution.Bkr.Contains("EURO"))
                //    commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.001;
                //else
                commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.0005;
            }
            return commission;
        }

        private double GetJefferiesCommission(TradeExecutionSummaryTO tradeExecution)
        {
            double commission = 0;
            if (tradeExecution.Bkr.Contains("EURO") || tradeExecution.Bkr.Contains("-EU"))
                commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.0015;
            else if ("USD".Equals(tradeExecution.Curr) || "CAD".Equals(tradeExecution.Curr))
            {
                if (tradeExecution.PB35MV.HasValue)
                {
                    commission = tradeExecution.PB35MV.GetValueOrDefault() * 0.0008;
                    commission += tradeExecution.PG35Qty.GetValueOrDefault() * 0.0035;
                }
                else
                    commission = tradeExecution.TotTrdQty.GetValueOrDefault() * 0.0035;
            }
            else
            {
                //if (tradeExecution.Bkr.Contains("EURO") || tradeExecution.Bkr.Contains("-EU"))
                //    commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.0015;
                //else
                commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.0005;
            }
            return commission;
        }

        private double GetScotiaCommission(TradeExecutionSummaryTO tradeExecution)
        {
            double commission = 0;
            if (tradeExecution.ScoPB1MV.HasValue)
                commission = tradeExecution.ScoPB1MV.GetValueOrDefault() * 0.001;
            if (tradeExecution.ScoPB5Qty.HasValue)
                commission += tradeExecution.ScoPB5Qty.GetValueOrDefault() * 0.002;
            if (tradeExecution.ScoPG5Qty.HasValue)
                commission += tradeExecution.ScoPG5Qty.GetValueOrDefault() * 0.0035;

            //if (tradeExecution.ScoPB1MV.HasValue)
            //    commission = tradeExecution.ScoPB1MV.GetValueOrDefault() * 0.001;
            //else if (tradeExecution.ScoPB5Qty.HasValue)
            //    commission = tradeExecution.ScoPB5Qty.GetValueOrDefault() * 0.002;
            //else
            //    commission = tradeExecution.TotTrdQty.GetValueOrDefault() * 0.0035;
            return commission;
        }

        private double GetTDCommission(TradeExecutionSummaryTO tradeExecution)
        {
            double commission = 0;
            if ("CAD".Equals(tradeExecution.Curr))
                commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.0005;
            else
                commission = tradeExecution.TotTrdMV.GetValueOrDefault() * 0.00075;
            return commission;
        }

        public double GetCommission(TradeExecutionSummaryTO tradeExecution)
        {
            double commission = 0;
            if (string.IsNullOrEmpty(tradeExecution.BkrName))
            {
                _logger.LogInformation("Invalid Broker: " + tradeExecution.Bkr + "/" + tradeExecution.Strategy);
            }
            else
            {
                IDictionary<string, BrokerCommission> dict = _cache.Get<IDictionary<string, BrokerCommission>>(CacheKeys.BROKER_COMMISSION_RATES);
                if (dict.TryGetValue(tradeExecution.BkrName, out BrokerCommission data))
                {
                    if ("CommisionByMV".Equals(data.FuncName))
                        commission = GetCommisionByMV(tradeExecution.TotTrdMV.GetValueOrDefault(), data.CommRt.GetValueOrDefault());
                    else if ("CommisionPerShare".Equals(data.FuncName))
                        commission = GetCommisionPerShare(tradeExecution.TotTrdQty.GetValueOrDefault(), data.CommRt.GetValueOrDefault());
                    else if ("CommissionScotia".Equals(data.FuncName))
                        commission = GetScotiaCommission(tradeExecution);
                    else if ("CommissionJPM".Equals(data.FuncName))
                        commission = GetJPMCommission(tradeExecution);
                    else if ("CommissionJefferies".Equals(data.FuncName))
                        commission = GetJefferiesCommission(tradeExecution);
                    else if ("CommissionTD".Equals(data.FuncName))
                        commission = GetTDCommission(tradeExecution);
                }
            }
            return commission;
        }
    }
}
