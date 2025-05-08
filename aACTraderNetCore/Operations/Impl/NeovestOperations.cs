using System;
using System.Collections.Generic;
using System.Globalization;

namespace aACTrader.Operations.Impl
{
    public static class NeovestOperations
    {
        static readonly IDictionary<string, string> CallOptionMonthCodeDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase)
        {
            { "Jan", "A"},
            { "Feb", "B"},
            { "Mar", "C"},
            { "Apr", "D"},
            { "May", "E"},
            { "Jun", "F"},
            { "Jul", "G"},
            { "Aug", "H"},
            { "Sep", "I"},
            { "Oct", "J"},
            { "Nov", "K"},
            { "Dec", "L"},
        };

        static readonly IDictionary<string, string> PutOptionMonthCodeDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase)
        {
            { "Jan", "M"},
            { "Feb", "N"},
            { "Mar", "O"},
            { "Apr", "P"},
            { "May", "Q"},
            { "Jun", "R"},
            { "Jul", "S"},
            { "Aug", "T"},
            { "Sep", "U"},
            { "Oct", "V"},
            { "Nov", "W"},
            { "Dec", "X"},
        };

        public static string GetNeovestSymbol(string symbol)
        {
            try
            {
                string neovestSymbol = string.Empty;
                string[] values = symbol.Split(' ');

                //foreach (string val in values)
                //    Console.WriteLine(val);

                string ticker = values[0];
                string optionTypeStrikePrice = values[3];
                string optionType = optionTypeStrikePrice.Substring(0, 1);
                string strikePrice = optionTypeStrikePrice.Substring(1);
                DateTime expirationDate = DateTime.Parse(values[2], new CultureInfo("en-US", true));
                string monthName = expirationDate.ToString("MMM");
                string year = expirationDate.ToString("yy");
                string day = expirationDate.Day.ToString();

                //Console.WriteLine("Ticker: " + ticker);
                //Console.WriteLine("Year: " + year);
                //Console.WriteLine("Month: " + monthName);
                //Console.WriteLine("Day: " + day);
                //Console.WriteLine("OptionType: " + optionType);
                //Console.WriteLine("StrikePrice: " + String.Format(CultureInfo.InvariantCulture, "{0:0.00000}", Convert.ToDouble(strikePrice)));

                string monthCode = string.Empty;
                if ("C".Equals(optionType, StringComparison.CurrentCultureIgnoreCase))
                    CallOptionMonthCodeDict.TryGetValue(monthName, out monthCode);
                else if ("P".Equals(optionType, StringComparison.CurrentCultureIgnoreCase))
                    PutOptionMonthCodeDict.TryGetValue(monthName, out monthCode);
                //Console.WriteLine("Month Code: " + monthCode);

                string formattedStrikePrice = (String.Format(CultureInfo.InvariantCulture, "{0:0.00000}", Convert.ToDouble(strikePrice))).Substring(0, 7);
                neovestSymbol = string.Join("", ticker, monthCode, day, year, formattedStrikePrice);
                //Console.WriteLine("Neovest Symbol: " + neovestSymbol);
                return neovestSymbol;
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}