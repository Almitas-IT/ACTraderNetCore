using aCommons;
using aCommons.Admin;
using aCommons.Trading;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Mail;

namespace aACTrader.Operations.Impl
{
    public class EmailOperations
    {
        private readonly ILogger<EmailOperations> _logger;
        private readonly FundHistoryOperations _fundHistoryOperations;


        public EmailOperations(ILogger<EmailOperations> logger, FundHistoryOperations fundhistoryoperations)
        {
            _logger = logger;
            _fundHistoryOperations = fundhistoryoperations;
        }

        public void GenerateMorningMail(IList<MorningMailData> Morningmaillist)
        {
            try
            {
                _logger.LogInformation("Starting Nav mail preparation...");

                string messageBody = "<font>Nav Changes(set to 2%): </font><br><br>";

                string htmlTableStart = "<table style=\"border-collapse:collapse; text-align:center; font-size: 12px\" >";
                string htmlTableEnd = "</table>";
                string htmlHeaderRowStart = "<tr style=\"background-color:#6FA1D2; color:#ffffff;\">";
                string htmlHeaderRowEnd = "</tr>";
                string htmlTrStart = "<tr style=\"color:#555555;\">";
                string htmlTrEnd = "</tr>";
                string htmlTdStart = "<td style=\" border-color:#5c87b2; border-style:solid; border-width:thin; text-align:left; padding: 7px;\">";
                string htmlTdStartNumeric = "<td style=\" border-color:#5c87b2; border-style:solid; border-width:thin; text-align:right; padding: 5px;\">";
                string htmlTdEnd = "</td>";
                string htmlTdStartSpl = "<td style=\" border-color:#5c87b2; border-style:solid; border-width:thin; text-align:left; padding: 7px; color:red;\">";

                messageBody += htmlTableStart;
                messageBody += htmlHeaderRowStart;
                messageBody += htmlTdStart + "Ticker" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Prev Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Change" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Date" + htmlTdEnd;
                messageBody += htmlTdStart + "Prev Nav Date" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Source" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Update Time" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Freq" + htmlTdEnd;

                messageBody += htmlHeaderRowEnd;

                foreach (MorningMailData data in Morningmaillist)
                {
                    decimal perNvChange = 0;
                    //perNvChange = (Convert.ToDecimal(data.NewVal.GetValueOrDefault()) - Convert.ToDecimal(data.PrevVal.GetValueOrDefault())) / data.PrevVal.GetValueOrDefault();

                    messageBody += htmlTrStart;
                    messageBody += htmlTdStart + data.Ticker + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.00}", data.NewNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.00}", data.PrevNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.00%}", perNvChange) + htmlTdEnd;
                    messageBody += htmlTdStart + data.NewNavDate.GetValueOrDefault().ToString("MM/dd/yyyy") + htmlTdEnd;
                    messageBody += htmlTdStart + data.PrevNavDate.GetValueOrDefault().ToString("MM/dd/yyyy") + htmlTdEnd;
                    messageBody += htmlTdStart + data.NewNavSrc + htmlTdEnd;
                    //messageBody += htmlTdStart + data.NavUpdateTime + htmlTdEnd;
                    messageBody += htmlTdStart + data.UpdateNavFreq + htmlTdEnd;

                    messageBody += htmlTrEnd;
                }
                messageBody += htmlTableEnd;

                _logger.LogInformation("Completed Nav changes table...");

                IList<FundNavReportTO> Naverrors = new List<FundNavReportTO>();
                Naverrors = _fundHistoryOperations.GetLatestFundNavEstDetails("United States", 0.7, DateTime.Today);

                messageBody += "<br><br><br><font>Nav Estimation Errors(Threshold set to 0.7%): </font><br><br>";

                messageBody += htmlTableStart;
                messageBody += htmlHeaderRowStart;
                messageBody += htmlTdStart + "Ticker" + htmlTdEnd;
                messageBody += htmlTdStart + "Report Date" + htmlTdEnd;
                messageBody += htmlTdStart + "Prev Nav Date" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Date" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Source" + htmlTdEnd;
                messageBody += htmlTdStart + "Nav Freq" + htmlTdEnd;
                messageBody += htmlTdStart + "Est Nav Source" + htmlTdEnd;
                messageBody += htmlTdStart + "Prev Pub Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Pub Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Prev Adj PUb Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Pub Adj Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Est Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Pub Nav Change" + htmlTdEnd;
                messageBody += htmlTdStart + "Est Nav Change" + htmlTdEnd;
                messageBody += htmlTdStart + "Error" + htmlTdEnd;
                messageBody += htmlTdStart + "Port Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "ETF Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Proxy Nav" + htmlTdEnd;
                messageBody += htmlTdStart + "Pub PD" + htmlTdEnd;
                messageBody += htmlTdStart + "Est PD" + htmlTdEnd;
                messageBody += htmlTdStart + "PD Change" + htmlTdEnd;

                messageBody += htmlHeaderRowEnd;

                foreach (FundNavReportTO data in Naverrors)
                {
                    messageBody += htmlTdStart + data.Ticker + htmlTdEnd;
                    messageBody += htmlTdStart + data.Date.Value.ToString("MM/dd/yyyy") + htmlTdEnd;
                    messageBody += htmlTdStart + data.PrevPubNavDt.Value.ToString("MM/dd/yyyy") + htmlTdEnd;
                    messageBody += htmlTdStart + data.PubNavDt.Value.ToString("MM/dd/yyyy") + htmlTdEnd;
                    messageBody += htmlTdStart + data.NavSrc + htmlTdEnd;
                    messageBody += htmlTdStart + data.NavFreq + htmlTdEnd;
                    messageBody += htmlTdStart + data.EstNavSrc + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.PrevPubNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.PubNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.PrevPubAdjNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.PubAdjNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.EstNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.00%}", data.PubNavChng) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.00%}", data.EstNavChng) + htmlTdEnd;
                    messageBody += htmlTdStartSpl + String.Format("{0:#0.00%}", data.EstNavErr) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.PortNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.ETFNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000}", data.ProxyNav) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000%}", data.PubPD) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000%}", data.EstPD) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.000%}", data.PDChng) + htmlTdEnd;

                    messageBody += htmlTrEnd;
                }
                messageBody += htmlTableEnd;

                MailMessage message = new MailMessage();
                SmtpClient smtp = new SmtpClient();
                message.From = new MailAddress("pchintakuntha@almitascapital.com");
                message.To.Add(new MailAddress("pchintakuntha@almitascapital.com"));
                message.To.Add(new MailAddress("skanigiri@almitascapital.com"));
                message.Subject = "Morning Nav Mail Data";
                message.IsBodyHtml = true;
                message.Body = messageBody;
                smtp.Port = 587;
                smtp.Host = "smtp.gmail.com";
                smtp.EnableSsl = true;
                smtp.UseDefaultCredentials = false;
                smtp.Credentials = new NetworkCredential("pchintakuntha@almitascapital.com", "gind gcul gubp kcia");
                smtp.DeliveryMethod = SmtpDeliveryMethod.Network;
                smtp.Send(message);

                _logger.LogInformation("Completed Nav Est Errors table...");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending Morning Nav Email");
            }
        }

        public void SendEmail(string input, string subject)
        {
            try
            {
                MailMessage message = new MailMessage();
                SmtpClient smtp = new SmtpClient();
                message.From = new MailAddress("pchintakuntha@almitascapital.com");
                message.To.Add(new MailAddress("pchintakuntha@almitascapital.com"));
                message.To.Add(new MailAddress("skanigiri@almitascapital.com"));
                message.Subject = subject;
                message.IsBodyHtml = true;
                message.Body = input;
                smtp.Port = 587;
                smtp.Host = "smtp.gmail.com";
                smtp.EnableSsl = true;
                smtp.UseDefaultCredentials = false;
                smtp.Credentials = new NetworkCredential("pchintakuntha@almitascapital.com", "gind gcul gubp kcia");
                smtp.DeliveryMethod = SmtpDeliveryMethod.Network;
                smtp.Send(message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending Email");
            }
        }

        public string GenerateMessage(IList<LogData> list)
        {
            try
            {
                string messageBody = "<font>The following are the records: </font><br><br>";

                string htmlTableStart = "<table style=\"border-collapse:collapse; text-align:center;\" >";
                string htmlTableEnd = "</table>";
                string htmlHeaderRowStart = "<tr style=\"background-color:#6FA1D2; color:#ffffff;\">";
                string htmlHeaderRowEnd = "</tr>";
                string htmlTrStart = "<tr style=\"color:#555555;\">";
                string htmlTrEnd = "</tr>";
                string htmlTdStart = "<td style=\" border-color:#5c87b2; border-style:solid; border-width:thin; padding: 5px;\">";
                string htmlTdEnd = "</td>";

                messageBody += htmlTableStart;
                messageBody += htmlHeaderRowStart;
                messageBody += htmlTdStart + "Service" + htmlTdEnd;
                messageBody += htmlTdStart + "Function" + htmlTdEnd;
                messageBody += htmlTdStart + "Ticker" + htmlTdEnd;
                messageBody += htmlTdStart + "Time" + htmlTdEnd;
                messageBody += htmlTdStart + "Message" + htmlTdEnd;
                messageBody += htmlHeaderRowEnd;

                foreach (LogData data in list)
                {
                    messageBody += htmlTrStart;
                    messageBody += htmlTdStart + data.ServiceName + htmlTdEnd;
                    messageBody += htmlTdStart + data.FunctionName + htmlTdEnd;
                    messageBody += htmlTdStart + data.Ticker + htmlTdEnd;
                    messageBody += htmlTdStart + data.EntryTime + htmlTdEnd;
                    messageBody += htmlTdStart + data.Message + htmlTdEnd;
                    messageBody += htmlTrEnd;
                }
                messageBody += htmlTableEnd;
                return messageBody;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating Html");
                return null;
            }
        }

        public string GenerateTradeExecutionEmail(IList<OrderSummary> allTrades, IList<OrderSummary> newTrades)
        {
            try
            {
                string messageBody = "<font>New Trade(s): " + newTrades.Count + " </font><br><br>";

                string htmlTableStart = "<table style=\"border-collapse:collapse; text-align:center; font-size: 12px\" >";
                string htmlTableEnd = "</table>";
                string htmlHeaderRowStart = "<tr style=\"background-color:#6FA1D2; color:#ffffff;\">";
                string htmlHeaderRowEnd = "</tr>";
                string htmlTrStart = "<tr style=\"color:#555555;\">";
                string htmlTrEnd = "</tr>";
                string htmlTdStart = "<td style=\" border-color:#5c87b2; border-style:solid; border-width:thin; text-align:left; padding: 5px;\">";
                string htmlTdStartNumeric = "<td style=\" border-color:#5c87b2; border-style:solid; border-width:thin; text-align:right; padding: 5px;\">";
                string htmlTdEnd = "</td>";

                //new trades
                messageBody += htmlTableStart;
                messageBody += htmlHeaderRowStart;
                messageBody += htmlTdStart + "Date" + htmlTdEnd;
                messageBody += htmlTdStart + "Symbol" + htmlTdEnd;
                messageBody += htmlTdStart + "Side" + htmlTdEnd;
                messageBody += htmlTdStart + "Type" + htmlTdEnd;
                messageBody += htmlTdStart + "Ord Px" + htmlTdEnd;
                messageBody += htmlTdStart + "Trade Px" + htmlTdEnd;
                messageBody += htmlTdStart + "Ord Qty" + htmlTdEnd;
                messageBody += htmlTdStart + "Trade Qty" + htmlTdEnd;
                messageBody += htmlTdStart + "% Comp" + htmlTdEnd;
                messageBody += htmlTdStart + "Trader" + htmlTdEnd;
                messageBody += htmlTdStart + "Dest" + htmlTdEnd;
                messageBody += htmlTdStart + "Strategy" + htmlTdEnd;
                messageBody += htmlTdStart + "Order Time" + htmlTdEnd;
                messageBody += htmlTdStart + "Update Time" + htmlTdEnd;
                messageBody += htmlTdStart + "Main Order Id" + htmlTdEnd;
                messageBody += htmlTdStart + "Sedol" + htmlTdEnd;
                messageBody += htmlTdStart + "Currency" + htmlTdEnd;

                messageBody += htmlHeaderRowEnd;

                foreach (OrderSummary data in newTrades)
                {
                    double qtyFilled = 0;
                    if (data.OrdQty.GetValueOrDefault() > 0)
                        qtyFilled = (data.TrdCumQty.GetValueOrDefault() / data.OrdQty.GetValueOrDefault());
                    messageBody += htmlTrStart;
                    messageBody += htmlTdStart + data.OrdDtAsString + htmlTdEnd;
                    messageBody += htmlTdStart + data.BBGSym + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdSide + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdTyp + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + data.OrdPr + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:N}", data.AvgTrdPr) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#,#}", data.OrdQty) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#,#}", data.TrdCumQty) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.00%}", qtyFilled) + htmlTdEnd;
                    messageBody += htmlTdStart + data.Trader + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdDest + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdBkrStrat + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + data.OrdTm + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + data.OrdStatusUpdTm + htmlTdEnd;
                    messageBody += htmlTdStart + data.MainOrdId + htmlTdEnd;
                    messageBody += htmlTdStart + data.Sedol + htmlTdEnd;
                    messageBody += htmlTdStart + data.Curr + htmlTdEnd;
                    messageBody += htmlTrEnd;
                }
                messageBody += htmlTableEnd;

                //all trades
                messageBody += "<br><br>";
                messageBody += "<font>All Trades: " + allTrades.Count + " </font><br><br>";

                messageBody += htmlTableStart;
                messageBody += htmlHeaderRowStart;
                messageBody += htmlTdStart + "Date" + htmlTdEnd;
                messageBody += htmlTdStart + "Symbol" + htmlTdEnd;
                messageBody += htmlTdStart + "Side" + htmlTdEnd;
                messageBody += htmlTdStart + "Type" + htmlTdEnd;
                messageBody += htmlTdStart + "Ord Px" + htmlTdEnd;
                messageBody += htmlTdStart + "Trade Px" + htmlTdEnd;
                messageBody += htmlTdStart + "Ord Qty" + htmlTdEnd;
                messageBody += htmlTdStart + "Trade Qty" + htmlTdEnd;
                messageBody += htmlTdStart + "% Comp" + htmlTdEnd;
                messageBody += htmlTdStart + "Trader" + htmlTdEnd;
                messageBody += htmlTdStart + "Dest" + htmlTdEnd;
                messageBody += htmlTdStart + "Strategy" + htmlTdEnd;
                messageBody += htmlTdStart + "Order Time" + htmlTdEnd;
                messageBody += htmlTdStart + "Update Time" + htmlTdEnd;
                messageBody += htmlTdStart + "Main Order Id" + htmlTdEnd;
                messageBody += htmlTdStart + "Sedol" + htmlTdEnd;
                messageBody += htmlTdStart + "Currency" + htmlTdEnd;

                messageBody += htmlHeaderRowEnd;

                foreach (OrderSummary data in allTrades)
                {
                    double qtyFilled = 0;
                    if (data.OrdQty.GetValueOrDefault() > 0)
                        qtyFilled = (data.TrdCumQty.GetValueOrDefault() / data.OrdQty.GetValueOrDefault());
                    messageBody += htmlTrStart;
                    messageBody += htmlTdStart + data.OrdDtAsString + htmlTdEnd;
                    messageBody += htmlTdStart + data.BBGSym + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdSide + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdTyp + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + data.OrdPr + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:N}", data.AvgTrdPr) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#,#}", data.OrdQty) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#,#}", data.TrdCumQty) + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + String.Format("{0:#0.00%}", qtyFilled) + htmlTdEnd;
                    messageBody += htmlTdStart + data.Trader + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdDest + htmlTdEnd;
                    messageBody += htmlTdStart + data.OrdBkrStrat + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + data.OrdTm + htmlTdEnd;
                    messageBody += htmlTdStartNumeric + data.OrdStatusUpdTm + htmlTdEnd;
                    messageBody += htmlTdStart + data.MainOrdId + htmlTdEnd;
                    messageBody += htmlTdStart + data.Sedol + htmlTdEnd;
                    messageBody += htmlTdStart + data.Curr + htmlTdEnd;
                    messageBody += htmlTrEnd;
                }
                messageBody += htmlTableEnd;
                return messageBody;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating Html");
                return null;
            }
        }
    }
}