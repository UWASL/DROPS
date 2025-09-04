using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public interface IDistribution
    {
        double GetSample();
        double GetMean();
        double GetVariance();

        double GetRate();

        double Count();
        double GetTail(double v);

        void Clear();
        void AddValue(double val);
        void AddValueFrequency(double val, double frequency);

        string GetSignature(double pLoad, double pUtilization, bool pOutputFlag);
        public string GetMeasurement(double pLoad, double pUtilization, bool pOutputFlag);

        public string ToString();

        public double GetValueByIndex(ulong index);

        public int PairsCount();

        public KeyValuePair<double, (double, double)> GetValueFreqPairByIndex(int index);
    };

    public static class RandomSource
    {
        static private Random _myRandom;

        public static void Init()
        {
            _myRandom = new Random(0);
            // _myRandom = new Random(DateTime.Now.Millisecond);
        }

        public static double GetNext()
        {
            return _myRandom.NextDouble();
        }

    }

    public class DistributionUniform(double pMin, double pMax) : IDistribution
    {
        private readonly double _min = pMin;
        private readonly double _max = pMax;
        private readonly double _range = pMax - pMin;
        //Debug.Assert(_range > 0);

        public double GetSample()
        {
            double val = _range * RandomSource.GetNext() + _min;
            Debug.Assert(_min <= val);
            Debug.Assert(val <= _max);
            return val;
        }

        public double GetMean()
        {
            return (_min + _max) / 2.0;
        }

        public double GetVariance()
        {
            return _range / 12.0;
        }

        //public static readonly string Name = "DistUni";

        public override string ToString()
        {
            return String.Format("DistUni: m {0:0.00}, v {1:0.00}, r {2:00.00}, range ({3:0.00},{4:0.00})",
                GetMean(), GetVariance(), GetRate(), _min, _max);
        }

        public double GetRate()
        {
            return 1.0 / GetMean();
        }

        public double Count()
        {
            return 0;
        }

        public double GetTail(double v)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public void AddValue(double val)
        {
            throw new NotImplementedException();
        }

        public void AddValueFrequency(double val, double frequency)
        {
            throw new NotImplementedException();
        }

        public string GetSignature(double pLoad, double pUtilization, bool pOutputFlag)
        {
            throw new NotImplementedException();
        }

        public string GetMeasurement(double pLoad, double pUtilization, bool pOutputFlag)
        {
            throw new NotImplementedException();
        }

        public double GetValueByIndex(ulong index)
        {
            throw new NotImplementedException();
        }

        public int PairsCount()
        {
            throw new NotImplementedException();
        }

        public KeyValuePair<double, (double, double)> GetValueFreqPairByIndex(int index)
        {
            throw new NotImplementedException();
        }
    }
}