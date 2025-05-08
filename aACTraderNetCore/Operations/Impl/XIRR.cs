using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class XIRR
    {
        private const Double DaysPerYear = 365.0;
        private const int MaxIterations = 100;
        private const double DefaultTolerance = 1E-6;
        private const double DefaultGuess = 0.1;

        private static readonly Func<IEnumerable<CashItem>, Double> NewthonsMethod =
            cf => NewtonsMethodImplementation(cf, Xnpv, XnpvPrime);

        private static readonly Func<IEnumerable<CashItem>, Double> BisectionMethod =
            cf => BisectionMethodImplementation(cf, Xnpv);

        public static double RunScenario(IEnumerable<CashItem> cashFlow)
        {
            double result = 0;

            try
            {
                try
                {
                    result = CalcXirr(cashFlow, NewthonsMethod);
                }
                catch (InvalidOperationException)
                {
                    // Failed: try another algorithm
                    result = CalcXirr(cashFlow, BisectionMethod);
                }
            }
            catch (ArgumentException)
            {
            }
            catch (InvalidOperationException)
            {
            }

            return result;
        }

        private static double CalcXirr(IEnumerable<CashItem> cashFlow, Func<IEnumerable<CashItem>, double> method)
        {
            if (cashFlow.Count(cf => cf.Amount > 0) == 0)
                throw new ArgumentException("Add at least one positive item");

            if (cashFlow.Count(c => c.Amount < 0) == 0)
                throw new ArgumentException("Add at least one negative item");

            var result = method(cashFlow);

            if (Double.IsInfinity(result))
                throw new InvalidOperationException("Could not calculate: Infinity");

            if (Double.IsNaN(result))
                throw new InvalidOperationException("Could not calculate: Not a number");

            return result;
        }

        private static Double NewtonsMethodImplementation(IEnumerable<CashItem> cashFlow,
                                                          Func<IEnumerable<CashItem>, Double, Double> f,
                                                          Func<IEnumerable<CashItem>, Double, Double> df,
                                                          Double guess = DefaultGuess,
                                                          Double tolerance = DefaultTolerance,
                                                          int maxIterations = MaxIterations)
        {
            var x0 = guess;
            var i = 0;
            Double error;
            do
            {
                var dfx0 = df(cashFlow, x0);
                if (Math.Abs(dfx0 - 0) < Double.Epsilon)
                    throw new InvalidOperationException("Could not calculate: No solution found. df(x) = 0");

                var fx0 = f(cashFlow, x0);
                var x1 = x0 - fx0 / dfx0;
                error = Math.Abs(x1 - x0);

                x0 = x1;
            } while (error > tolerance && ++i < maxIterations);
            if (i == maxIterations)
                throw new InvalidOperationException("Could not calculate: No solution found. Max iterations reached.");

            return x0;
        }

        internal static Double BisectionMethodImplementation(IEnumerable<CashItem> cashFlow,
                                                             Func<IEnumerable<CashItem>, Double, Double> f,
                                                             Double tolerance = DefaultTolerance,
                                                             int maxIterations = MaxIterations)
        {
            // From "Applied Numerical Analysis" by Gerald
            var brackets = Brackets.Find(Xnpv, cashFlow);
            if (Math.Abs(brackets.First - brackets.Second) < Double.Epsilon)
                throw new ArgumentException("Could not calculate: bracket failed");

            Double f3;
            Double result;
            var x1 = brackets.First;
            var x2 = brackets.Second;

            var i = 0;
            do
            {
                var f1 = f(cashFlow, x1);
                var f2 = f(cashFlow, x2);

                if (Math.Abs(f1) < Double.Epsilon && Math.Abs(f2) < Double.Epsilon)
                    throw new InvalidOperationException("Could not calculate: No solution found");

                if (f1 * f2 > 0)
                    throw new ArgumentException("Could not calculate: bracket failed for x1, x2");

                result = (x1 + x2) / 2;
                f3 = f(cashFlow, result);

                if (f3 * f1 < 0)
                    x2 = result;
                else
                    x1 = result;
            } while (Math.Abs(x1 - x2) / 2 > tolerance && Math.Abs(f3) > Double.Epsilon && ++i < maxIterations);

            if (i == maxIterations)
                throw new InvalidOperationException("Could not calculate: No solution found");

            return result;
        }

        private static Double Xnpv(IEnumerable<CashItem> cashFlow, Double rate)
        {
            if (rate <= -1)
                rate = -1 + 1E-10; // Very funky ... Better check what an IRR <= -100% means

            var startDate = cashFlow.OrderBy(i => i.Date).First().Date;
            return
                (from item in cashFlow
                 let days = -(item.Date - startDate).Days
                 select item.Amount * Math.Pow(1 + rate, days / DaysPerYear)).Sum();
        }

        private static Double XnpvPrime(IEnumerable<CashItem> cashFlow, Double rate)
        {
            var startDate = cashFlow.OrderBy(i => i.Date).First().Date;
            return (from item in cashFlow
                    let daysRatio = -(item.Date - startDate).Days / DaysPerYear
                    select item.Amount * daysRatio * Math.Pow(1.0 + rate, daysRatio - 1)).Sum();
        }

        public struct Brackets
        {
            public readonly Double First;
            public readonly Double Second;

            public Brackets(Double first, Double second)
            {
                First = first;
                Second = second;
            }

            internal static Brackets Find(Func<IEnumerable<CashItem>, Double, Double> f,
                                          IEnumerable<CashItem> cashFlow,
                                          Double guess = DefaultGuess,
                                          int maxIterations = MaxIterations)
            {
                const Double bracketStep = 0.5;
                var leftBracket = guess - bracketStep;
                var rightBracket = guess + bracketStep;
                var i = 0;
                while (f(cashFlow, leftBracket) * f(cashFlow, rightBracket) > 0 && i++ < maxIterations)
                {
                    leftBracket -= bracketStep;
                    rightBracket += bracketStep;
                }

                return i >= maxIterations
                           ? new Brackets(0, 0)
                           : new Brackets(leftBracket, rightBracket);
            }
        }

        public struct CashItem
        {
            public DateTime Date;
            public Double Amount;

            public CashItem(DateTime date, Double amount)
            {
                Date = date;
                Amount = amount;
            }
        }
    }
}