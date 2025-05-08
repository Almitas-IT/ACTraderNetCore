using System;
using System.Runtime.Serialization;

namespace aACTrader.Model
{
    [DataContract]
    public class ServiceDataContract
    {
        [DataMember]
        public string Country { get; set; }
        [DataMember]
        public string SecurityType { get; set; }
        [DataMember]
        public string CEFInstrumentType { get; set; }
        [DataMember]
        public string Sector { get; set; }
        [DataMember]
        public string FundCategory { get; set; }
        [DataMember]
        public string Ticker { get; set; }
        [DataMember]
        public string StartDate { get; set; }
        [DataMember]
        public string EndDate { get; set; }
        [DataMember]
        public string ProfilePeriod { get; set; }
        [DataMember]
        public string RequestType { get; set; }
        [DataMember]
        public int Index { get; set; }
        [DataMember]
        public string MeasureType { get; set; }
    }

    [DataContract]
    public class TaxLotParameters
    {
        [DataMember]
        public string Ticker { get; set; }
        [DataMember]
        public string Account { get; set; }
        [DataMember]
        public string Broker { get; set; }
        [DataMember]
        public string MultiBrokerFlag { get; set; }
        public string AgeRangeFrom { get; set; }
        public string AgeRangeTo { get; set; }

    }

    [DataContract]
    public class DurationParamters
    {
        [DataMember]
        public double PriceBase { get; set; }
        [DataMember]
        public double PriceUp { get; set; }
        [DataMember]
        public double PriceDown { get; set; }
        [DataMember]
        public double YieldChange { get; set; }
    }

    [DataContract]
    public class InputParameters
    {
        [DataMember]
        public string FundTicker { get; set; }
        [DataMember]
        public string Ticker { get; set; }
        [DataMember]
        public string CurveName { get; set; }
        [DataMember]
        public double InputValue { get; set; }
        [DataMember]
        public string Exchangeable { get; set; }
        [DataMember]
        public string AsofDate { get; set; }
        [DataMember]
        public string StartDate { get; set; }
        [DataMember]
        public string EndDate { get; set; }
        [DataMember]
        public string Status { get; set; }
        [DataMember]
        public string AssetType { get; set; }
        [DataMember]
        public string AssetClassLevel1 { get; set; }
        [DataMember]
        public string Country { get; set; }
        [DataMember]
        public string Broker { get; set; }
        [DataMember]
        public string GroupBy { get; set; }
        [DataMember]
        public string ScenarioName { get; set; }
        [DataMember]
        public string Source { get; set; }
        [DataMember]
        public string FundName { get; set; }
        [DataMember]
        public string Currency { get; set; }
        [DataMember]
        public string ContractType { get; set; }
        [DataMember]
        public string Side { get; set; }
        [DataMember]
        public string AssetGroup { get; set; }
        [DataMember]
        public string TriggerType { get; set; }
        [DataMember]
        public string UserName { get; set; }
        [DataMember]
        public string JobName { get; set; }
        [DataMember]
        public Nullable<int> LookbackDays { get; set; }
        [DataMember]
        public string DvdDateField { get; set; }
        [DataMember]
        public string NavFreq { get; set; }
        [DataMember]
        public string ErrorThreshold { get; set; }
        [DataMember]
        public string FuncName { get; set; }
        [DataMember]
        public string FuncType { get; set; }
        [DataMember]
        public string FuncCategory { get; set; }
        [DataMember]
        public string DataSrc { get; set; }
        [DataMember]
        public string ReportType { get; set; }
        [DataMember]
        public string SecurityType { get; set; }

        [DataMember]
        public Nullable<int> RuleMstId { get; set; }
        [DataMember]
        public string FileName { get; set; }

        [DataMember]
        public string FileContent { get; set; }

    }

    [DataContract]
    public class CEFParameters
    {
        [DataMember]
        public string Market { get; set; }
        [DataMember]
        public string AssetType { get; set; }
        [DataMember]
        public string PaymentRank { get; set; }
        [DataMember]
        public string GroupByCountry { get; set; }
        [DataMember]
        public string GroupByCurrency { get; set; }
        [DataMember]
        public string GroupByGeo { get; set; }
        [DataMember]
        public string GroupByAssetClass { get; set; }
        [DataMember]
        public string SortBy { get; set; }
        [DataMember]
        public string SortOrder { get; set; }
        [DataMember]
        public string Field { get; set; }
        [DataMember]
        public string Operator { get; set; }
        [DataMember]
        public Nullable<double> Value1 { get; set; }
        [DataMember]
        public Nullable<double> Value2 { get; set; }
        [DataMember]
        public string GroupBy1 { get; set; }
        [DataMember]
        public string GroupBy2 { get; set; }
        [DataMember]
        public string GroupBy3 { get; set; }
        [DataMember]
        public string GroupBy4 { get; set; }
        [DataMember]
        public string ViewName { get; set; }
    }

    [DataContract]
    public class FundAlertParameters
    {
        [DataMember]
        public string Category { get; set; }
        [DataMember]
        public string StartDate { get; set; }
        [DataMember]
        public string EndDate { get; set; }
        [DataMember]
        public string GroupByAssetClass { get; set; }
        [DataMember]
        public string AssetClassLevel { get; set; }
        [DataMember]
        public string GroupByState { get; set; }
        [DataMember]
        public string GroupByCurrency { get; set; }
        [DataMember]
        public string GroupByCountry { get; set; }
        [DataMember]
        public string IncludeIRRFunds { get; set; }
    }

    [DataContract]
    public class ExposureReportParameters
    {
        [DataMember]
        public string Portfolio { get; set; }
        [DataMember]
        public string Broker { get; set; }
        [DataMember]
        public string GroupBy1 { get; set; }
        [DataMember]
        public string GroupBy2 { get; set; }
        [DataMember]
        public string GroupBy3 { get; set; }
        [DataMember]
        public string GroupBy4 { get; set; }
        [DataMember]
        public string GroupBy5 { get; set; }
        [DataMember]
        public string GroupBy6 { get; set; }

        [DataMember]
        public string ShowDetails { get; set; }
    }

    [DataContract]
    public class ActivistReportParameters
    {
        [DataMember]
        public string ActivistName { get; set; }
        [DataMember]
        public string StartDate { get; set; }
        [DataMember]
        public string EndDate { get; set; }
        [DataMember]
        public string ReportType { get; set; }
        [DataMember]
        public string Ticker { get; set; }
        [DataMember]
        public string Country { get; set; }
    }

    [DataContract]
    public class TradeHistoryReportParameters
    {
        [DataMember]
        public string StartDate { get; set; }
        [DataMember]
        public string EndDate { get; set; }
        [DataMember]
        public string Portfolio { get; set; }
        [DataMember]
        public string Broker { get; set; }
        [DataMember]
        public string Ticker { get; set; }
        [DataMember]
        public string Currency { get; set; }
        [DataMember]
        public string Country { get; set; }
        [DataMember]
        public string SecurityType { get; set; }
        [DataMember]
        public string GeoLevel1 { get; set; }
        [DataMember]
        public string GeoLevel2 { get; set; }
        [DataMember]
        public string GeoLevel3 { get; set; }
        [DataMember]
        public string AssetClassLevel1 { get; set; }
        [DataMember]
        public string AssetClassLevel2 { get; set; }
        [DataMember]
        public string AssetClassLevel3 { get; set; }
        [DataMember]
        public string FundCategory { get; set; }
    }

    [DataContract]
    public class FundReturnParameters
    {
        [DataMember]
        public string Ticker { get; set; }
        [DataMember]
        public string ReturnType { get; set; }
        [DataMember]
        public string ReturnPeriod { get; set; }
    }

    [DataContract]
    public class SecurityReturnParameters
    {
        [DataMember]
        public string Ticker { get; set; }
        [DataMember]
        public string FromDate { get; set; }
        [DataMember]
        public string ToDate { get; set; }
    }


    [DataContract]
    public class HistoricalDiscountStatsParameters
    {
        [DataMember]
        public string StartDate { get; set; }
        [DataMember]
        public string EndDate { get; set; }
        [DataMember]
        public string GroupName { get; set; }
        [DataMember]
        public string SecurityType { get; set; }
        [DataMember]
        public string Country { get; set; }
        [DataMember]
        public string Period { get; set; }
    }

    [DataContract]
    public class FXReturnParameters
    {
        [DataMember]
        public string FromCurrency { get; set; }
        [DataMember]
        public string ToCurrency { get; set; }
        [DataMember]
        public string FromDate { get; set; }
        [DataMember]
        public string ToDate { get; set; }
    }

    [DataContract]
    public class OrderParameters
    {
        [DataMember]
        public string Environment { get; set; }
        [DataMember]
        public string ShowAllOrders { get; set; }
        [DataMember]
        public string Trader { get; set; }
        [DataMember]
        public string ALMTrader { get; set; }
        [DataMember]
        public string ShowActiveOrders { get; set; }
        [DataMember]
        public string Symbol { get; set; }
        [DataMember]
        public string TemplateName { get; set; }
        [DataMember]
        public string OrderDate { get; set; }
        [DataMember]
        public string MainOrderId { get; set; }
        [DataMember]
        public string OrderStatus { get; set; }
        [DataMember]
        public string TimeFilter { get; set; }
        [DataMember]
        public string ShowPairTrades { get; set; }
        [DataMember]
        public string StartDate { get; set; }
        [DataMember]
        public string EndDate { get; set; }

    }

    [DataContract]
    public class PriceFilterParameters
    {
        [DataMember]
        public string PriceThreshold { get; set; }
    }
}
