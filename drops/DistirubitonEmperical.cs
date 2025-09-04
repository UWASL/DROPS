using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public interface IDistributionEmpirical : IDistribution
    {

    }

    public class RoundedDouble : IEquatable<RoundedDouble>, IComparable
    {
        private const double Tolerance = 0.005;

        public double Value { get; }
        private readonly int _roundedHashCode;

        public RoundedDouble(double value, int decimalPlaces)
        {
            Value = Math.Round(value, decimalPlaces);
            _roundedHashCode = Value.GetHashCode();
        }

        public override int GetHashCode()
        {
            return _roundedHashCode;
        }

        public bool Equals(RoundedDouble other)
        {
            if (other is not RoundedDouble)
                return false;

            return Math.Abs(Value - other.Value) < Tolerance;
        }

        public int CompareTo(object? other)
        {
            if (other is not RoundedDouble)
                throw new ArgumentException("");
            var other2 = (RoundedDouble)other;
            if (Math.Abs(Value - other2.Value) < Tolerance)
            {
                return 0;
            }
            if (Value - other2.Value > 0)
            {
                return 1;
            }
            return -1;
        }
    }



    public class DistributionEmpirical : IDistributionEmpirical
    {
        protected double _sumX;
        protected double _sumXX;
        protected double count;
        protected double _min;
        protected double _max;

        public DistributionEmpirical()
        {
            _sumX = 0.0;
            _sumXX = 0.0;
            count = 0;
        }
        public void Clear()
        {
            _sumX = 0.0;
            _sumXX = 0.0;
            count = 0;
            _min = 0.0;
            _max = 0.0;
        }
        public double GetSample()
        {
            throw new NotImplementedException();
        }

        public virtual double GetMean()
        {
            if (count == 0)
            {
                throw new NotImplementedException();
            }
            else
            {
                return _sumX / (double)count;
            }
        }

        public virtual double GetVariance()
        {
            if (count == 0)
            {
                throw new NotImplementedException();
            }
            else
            {
                //return _sumXX- / (double)count;
                throw new NotImplementedException();
            }
        }

        public virtual double GetRate()
        {
            return 1.0 / GetMean();
        }

        protected void NoteMinMax(double val)
        {
            if (count == 0)
            {
                _min = val;
                _max = val;
            }
            else
            {
                if (_min > val) _min = val;
                if (_max < val) _max = val;
            }
        }

        public virtual void AddValue(double val)
        {
            NoteMinMax(val);
            _sumX += val;
            _sumXX += val * val;
            count++;
        }

        public void AddValueFrequency(double val, double frequency)
        {
            NoteMinMax(val);
            _sumX += val * frequency;
            _sumXX += val * val * frequency;
            count = count + frequency;
        }

        public virtual double GetTail(double percentage)
        {
            throw new NotImplementedException();
        }

        public double GetValueByIndex(ulong index)
        {
            throw new NotImplementedException();
        }
        //public static readonly string Name = "DistEmperical";

        public override string ToString()
        {

            String s;
            s = String.Format("XRES DistEmperical signature count:{0}, min:{1}, average:{2}, max:{3}, rate:{4}",
                    count, _min, GetMean(), _max, GetRate());
            return s;
        }

        public string GetMeasurement(double pLoad, double pUtilization, bool pOutputFlag)
        {
            var s = "";
            if (pOutputFlag)
            {
                s += String.Format("#load\tutil\tmin\taverage\tmax\trate\tcount\n");
            }
            s += String.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n", pLoad, pUtilization, _min, GetMean(), _max, GetRate(), count);
            return s;
        }

        public virtual string GetSignature(double pLoad, double pUtilization, bool pOutputFlag)
        {
            return ToString() + "\n" + GetMeasurement(pLoad, pUtilization, pOutputFlag);
        }

        public double Count()
        {
            return count;
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

    public class DistributionEmpiricalPractical : DistributionEmpirical, IDistributionEmpirical
    {
        private readonly DistributionEmpiricalFrequencyArray _frequencyDistribution;
        private readonly DistributionEmpiricalMemoryStored _arrayDistribution;

        public DistributionEmpiricalPractical() : base()
        {
            _frequencyDistribution = new DistributionEmpiricalFrequencyArray();
            _arrayDistribution = new DistributionEmpiricalMemoryStored();
        }

        public new double Count()
        {
            return _frequencyDistribution.GetCountFromFreq();
        }

        public int PairsCount()
        {
            return _frequencyDistribution.PairsCount();
        }

        public new void Clear()
        {
            base.Clear();
            _frequencyDistribution.Clear();
            _arrayDistribution.Clear();
        }

        public new double GetMean()
        {
            return _arrayDistribution.GetMean();
        }

        public KeyValuePair<double, (double, double)> GetValueFreqPairByIndex(int index)
        {
            return _frequencyDistribution.GetValueFreqPairByIndex(index);
        }

        public double GetValueByIndex(int index)
        {
            return _arrayDistribution.GetValueByIndex(index);
        }

        public new double GetSample()
        {
            return _arrayDistribution.GetSample();
        }

        public new void AddValue(double val)
        {
            base.AddValue(val);
            _frequencyDistribution.AddValue(val);
            _arrayDistribution.AddValue(val);
        }

        public new void AddValueFrequency(double val, double freq)
        {
            base.AddValueFrequency(val, freq);
            _frequencyDistribution.AddValueFrequency(val, freq);
            _arrayDistribution.AddValueFrequency(val, freq);
        }

        public new double GetTail(double pPercentile)
        {
            return _arrayDistribution.GetTail(pPercentile);
        }

        public new string ToString()
        {
            return _arrayDistribution.ToString();
        }

        public new string GetMeasurement(double pLoad, double pUtilization, bool pOutputFlag)
        {
            return _arrayDistribution.GetMeasurement(pLoad, pUtilization, pOutputFlag);
        }

        public new string GetSignature(double pLoad, double pUtilization, bool pOutputFlag)
        {
            return _arrayDistribution.GetSignature(pLoad, pUtilization, pOutputFlag);
        }
    }

    public class DistributionEmpiricalFrequencyArray : DistributionEmpirical, IDistributionEmpirical
    {
        private readonly SortedDictionary<RoundedDouble, (double, double)> _vals;
        private bool _isCumulativeFreqValid;
        public DistributionEmpiricalFrequencyArray()
            : base()
        {
            _vals = new SortedDictionary<RoundedDouble, (double, double)>();
            _isCumulativeFreqValid = false;
        }

        public double Count()
        {
            return base.count;
        }

        public int PairsCount()
        {
            return _vals.Count();
        }

        public new void Clear()
        {
            base.Clear();
            _vals.Clear();
            _isCumulativeFreqValid = false;
        }

        public int FindIndexBinarySearch(ulong index)
        {
            Debug.Assert(_isCumulativeFreqValid);
            int left = 0;
            int right = _vals.Count() - 1;
            int result = -1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;
                var valueFreqPair = GetValueFreqPairByIndex(mid);
                if (valueFreqPair.Value.Item2 > index)
                {
                    result = mid;
                    right = mid - 1;
                }
                else
                {
                    left = mid + 1;
                }
            }
            return result;
        }


        private void UpdateCumulativeFrequency()
        {
            if (_isCumulativeFreqValid)
            {
                return;
            }
            double cumulativeFrequency = 0;
            foreach (var key in _vals.Keys.ToList())
            {
                var (freq, cumFreq) = _vals[key];
                cumulativeFrequency += freq;
                _vals[key] = (freq, cumulativeFrequency);
            }
            _isCumulativeFreqValid = true;
            Debug.Assert((ulong)cumulativeFrequency == (ulong)Count());
        }

        public KeyValuePair<double, (double, double)> GetValueFreqPairByIndex(int index)
        {
            Debug.Assert(index < PairsCount());
            UpdateCumulativeFrequency();
            KeyValuePair<double, (double, double)> keyValuePair = new(_vals.ElementAt(index).Key.Value, _vals.ElementAt(index).Value);
            return keyValuePair;
        }

        public double GetCountFromFreq()
        {
            if (Count() == 0)
            {
                return 0;
            }
            var valueFreqPair = GetValueFreqPairByIndex(PairsCount() - 1);
            Debug.Assert((ulong)valueFreqPair.Value.Item2 == (ulong)Count());
            return valueFreqPair.Value.Item2;
        }

        new public double GetValueByIndex(ulong index)
        {
            Debug.Assert(index < (ulong)count);
            UpdateCumulativeFrequency();
            var valueIndex = FindIndexBinarySearch(index);
            Debug.Assert(valueIndex < PairsCount());
            var valueFreqPair = GetValueFreqPairByIndex(valueIndex);
            return valueFreqPair.Key;
        }

        public new double GetSample()
        {
            UpdateCumulativeFrequency();
            double probability = RandomSource.GetNext();
            ulong index = (ulong)(probability * Count());
            return GetValueByIndex(index);
        }

        public override void AddValue(double val)
        {
            var roundedDouble = new RoundedDouble(val, 1);
            base.AddValue(roundedDouble.Value);
            _isCumulativeFreqValid = false;
            if (!_vals.TryGetValue(roundedDouble, out (double, double) freqCumFreqPair))
            {
                freqCumFreqPair.Item1 = 0;
                _vals.Add(roundedDouble, freqCumFreqPair);
            }
            _vals[roundedDouble] = (++freqCumFreqPair.Item1, ++freqCumFreqPair.Item2);
        }

        new public void AddValueFrequency(double val, double freq)
        {
            base.AddValueFrequency(val, freq);
            _isCumulativeFreqValid = false;
            var roundedDouble = new RoundedDouble(val, 1);
            if (!_vals.TryGetValue(roundedDouble, out (double, double) freqCumFreqPair))
            {
                freqCumFreqPair.Item1 = 0;
                _vals.Add(roundedDouble, (freqCumFreqPair.Item1, 0));
            }
            _vals[roundedDouble] = (freqCumFreqPair.Item1 + freq, freqCumFreqPair.Item2);
        }


        private static void CheckPercentile(double i)
        {
            Debug.Assert(0 <= i);
            Debug.Assert(i <= 1);
        }

        public override double GetTail(double pPercentile)
        {
            Debug.Assert(count > 0);
            Debug.Assert((ulong)count == (ulong)GetCountFromFreq());

            CheckPercentile(pPercentile);
            if (!_isCumulativeFreqValid)
            {
                UpdateCumulativeFrequency();
            }
            double val;
            switch (pPercentile)
            {
                case 0.0:
                    val = GetValueByIndex(0);
                    Debug.Assert(val == _min);
                    break;
                case 1.0:
                    val = GetValueByIndex((ulong)GetCountFromFreq() - 1);
                    break;
                default:
                    ulong index = (ulong)Math.Ceiling(pPercentile * (count - 1));
                    val = GetValueByIndex(index);
                    break;
            }
            return val;
        }

        public override string ToString()
        {
            var s = String.Format("XRES DistEmperical signature " +
                                     "count:{0}, min:{1:00.00}, median:{2:00.00}, average:{3:00.00}, " +
                                     "p90:{4:00.00}, p95:{5:00.00}, p99:{6:00.00}, p999:{7:00.00}, " +
                                     "max:{8:00.00}, rate:{9:00.00}",
                count, GetTail(0), GetTail(0.5), GetMean(),
                GetTail(0.9), GetTail(0.95), GetTail(0.99), GetTail(0.999),
                GetTail(1), GetRate());
            return s;
        }

        public new string GetMeasurement(double pLoad, double pUtilization, bool pOutputFlag)
        {
            var s = "";
            if (pOutputFlag)
            {
                s += "#load\tutil\tmin\tmedian\taverage\tp90\tp95\tp99\tp999\tmax\trate\tcount\n";
            }
            s += String.Format("{0:00.00}\t{1:00.00}\t{2:00.00}\t{3:00.00}\t{4:00.00}\t{5:00.00}\t{6:00.00}\t{7:00.00}\t{8:00.00}\t{9:00.00}\t{10:00.00}\t{11}\n",
                pLoad, pUtilization, GetTail(0), GetTail(0.5), GetMean(), GetTail(0.9), GetTail(0.95), GetTail(0.99), GetTail(0.999), GetTail(1), GetRate(), count);
            return s;
        }

        public override string GetSignature(double pLoad, double pUtilization, bool pOutputFlag)
        {
            return ToString() + "\n" + GetMeasurement(pLoad, pUtilization, pOutputFlag);
        }
    }

    public class DistributionEmpiricalDoubleFrequencyArray : DistributionEmpirical, IDistributionEmpirical
    {
        private readonly SortedDictionary<RoundedDouble, (double, double)> _vals;
        private bool _isCumulativeFreqValid;
        public DistributionEmpiricalDoubleFrequencyArray()
            : base()
        {
            _vals = new SortedDictionary<RoundedDouble, (double, double)>();
            _isCumulativeFreqValid = false;
        }

        new public double Count()
        {
            return base.count;
        }

        new public int PairsCount()
        {
            return _vals.Count();
        }

        new public void Clear()
        {
            _vals.Clear();
            base.Clear();
            _isCumulativeFreqValid = false;
        }

         new public double GetValueByIndex(ulong index)
        {
            Debug.Assert(index < (ulong)count);
            UpdateCumulativeFrequency();
            var valueIndex = FindIndexBinarySearch(index);
            Debug.Assert(valueIndex < PairsCount());
            var valueFreqPair = GetValueFreqPairByIndex(valueIndex);
            return valueFreqPair.Key;
        }


        public int FindIndexBinarySearch(double index)
        {
            Debug.Assert(_isCumulativeFreqValid);
            int left = 0;
            int right = _vals.Count() - 1;
            int result = -1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;
                var valueFreqPair = GetValueFreqPairByIndex(mid);
                if (valueFreqPair.Value.Item2 > index)
                {
                    result = mid;
                    right = mid - 1;
                }
                else
                {
                    left = mid + 1;
                }
            }
            return result;
        }

        private void UpdateCumulativeFrequency()
        {
            if (_isCumulativeFreqValid)
            {
                return;
            }
            double cumulativeFrequency = 0;
            foreach (var key in _vals.Keys.ToList())
            {
                var (freq, cumFreq) = _vals[key];
                cumulativeFrequency += freq;
                _vals[key] = (freq, cumulativeFrequency);
            }
            _isCumulativeFreqValid = true;
            Debug.Assert((ulong)cumulativeFrequency == (ulong)Count());
        }

        new public KeyValuePair<double, (double, double)> GetValueFreqPairByIndex(int index)
        {
            Debug.Assert(index < PairsCount());
            UpdateCumulativeFrequency();
            KeyValuePair<double, (double, double)> keyValuePair = new(_vals.ElementAt(index).Key.Value, _vals.ElementAt(index).Value);
            return keyValuePair;
        }

        public double GetCountFromFreq()
        {
            if (Count() == 0)
            {
                return 0;
            }
            var valueFreqPair = GetValueFreqPairByIndex((int)(PairsCount() - 1));
            Debug.Assert((ulong)valueFreqPair.Value.Item2 == (ulong)Count());
            return valueFreqPair.Value.Item2;
        }

        public void AddValueFrequency(double val, double freq)
        {
            base.AddValueFrequency(val, freq);
            _isCumulativeFreqValid = false;
            var roundedDouble = new RoundedDouble(val, 2);
            if (!_vals.TryGetValue(roundedDouble, out (double, double) freqCumFreqPair))
            {
                freqCumFreqPair.Item1 = 0;
                _vals.Add(roundedDouble, (freqCumFreqPair.Item1, 0));
            }
            _vals[roundedDouble] = (freqCumFreqPair.Item1 + freq, freqCumFreqPair.Item2);
        }

        public override void AddValue(double val)
        {
            var roundedDouble = new RoundedDouble(val, 2);
            base.AddValue(roundedDouble.Value);
            _isCumulativeFreqValid = false;
            if (!_vals.TryGetValue(roundedDouble, out (double, double) freqCumFreqPair))
            {
                freqCumFreqPair.Item1 = 0;
                _vals.Add(roundedDouble, freqCumFreqPair);
            }
            _vals[roundedDouble] = (++freqCumFreqPair.Item1, ++freqCumFreqPair.Item2);
        }

        private static void CheckPercentile(double i)
        {
            Debug.Assert(0 <= i);
            Debug.Assert(i <= 1);
        }

        public override double GetTail(double pPercentile)
        {
            Debug.Assert(count > 0);
            Debug.Assert((ulong)count == (ulong)GetCountFromFreq());

            CheckPercentile(pPercentile);
            if (!_isCumulativeFreqValid)
            {
                UpdateCumulativeFrequency();
            }
            double val;
            switch (pPercentile)
            {
                case 0.0:
                    val = GetValueByIndex(0);
                    Debug.Assert(val == _min);
                    break;
                case 1.0:
                    val = GetValueByIndex((ulong)GetCountFromFreq() - 1);
                    break;
                default:
                    ulong index = (ulong)Math.Ceiling(pPercentile * (count - 1));
                    val = GetValueByIndex(index);
                    break;
            }
            return val;
        }

    }


    public class DistributionEmpiricalMemoryStored : DistributionEmpirical, IDistributionEmpirical
    {
        private readonly List<double> _vals;
        private bool _sorted;

        public DistributionEmpiricalMemoryStored()
            : base()
        {
            _vals = new List<double>();
            _sorted = false;
        }



        public new double Count()
        {
            return base.count;
        }

        public new void Clear()
        {
            base.Clear();
            _vals.Clear();
            _sorted = false;
        }

        public double GetValueByIndex(int index)
        {
            if (!_sorted)
            {
                _vals.Sort();
                _sorted = true;
            }
            return _vals[index];
        }

        public new double GetSample()
        {
            if (!_sorted)
            {
                _vals.Sort();
                _sorted = true;
            }
            double probability = RandomSource.GetNext();
            int index = (int)(probability * _vals.Count);
            return _vals[index];
        }

        public override void AddValue(double val)
        {
            base.AddValue(val);
            _sorted = false;
            _vals.Add(val);
        }

        new public void AddValueFrequency(double val, double freq)
        {
            for (ulong i = 0; i < freq; i++)
            {
                AddValue(val);
            }
        }


        private static void CheckPercentile(double i)
        {
            Debug.Assert(0 <= i);
            Debug.Assert(i <= 1);
        }

        public override double GetTail(double pPercentile)
        {
            Debug.Assert(count > 0);
            Debug.Assert((int)count == _vals.Count);

            CheckPercentile(pPercentile);

            if (!_sorted)
            {
                _vals.Sort();
                _sorted = true;
            }

            var val = 0.0;
            switch (pPercentile)
            {
                case 0.0:
                    val = _vals.First();
                    Debug.Assert(val == _min);
                    break;
                case 1.0:
                    val = _vals.Last();
                    break;
                default:
                    int index = (int)Math.Ceiling(pPercentile * (count - 1));
                    val = _vals.ElementAt(index);
                    break;
            }
            return val;
        }

        public override string ToString()
        {
            var s = String.Format("XRES DistEmperical signature " +
                                     "count:{0}, min:{1:00.00}, median:{2:00.00}, average:{3:00.00}, " +
                                     "p90:{4:00.00}, p95:{5:00.00}, p99:{6:00.00}, p999:{7:00.00}, " +
                                     "max:{8:00.00}, rate:{9:00.00}",
                count, GetTail(0), GetTail(0.5), GetMean(),
                GetTail(0.9), GetTail(0.95), GetTail(0.99), GetTail(0.999),
                GetTail(1), GetRate());
            return s;
        }

        public new string GetMeasurement(double pLoad, double pUtilization, bool pOutputFlag)
        {
            var s = "";
            if (pOutputFlag)
            {
                s += "#load\tutil\tmin\tmedian\taverage\tp90\tp95\tp99\tp999\tmax\trate\tcount\n";
            }
            s += String.Format("{0:00.00}\t{1:00.00}\t{2:00.00}\t{3:00.00}\t{4:00.00}\t{5:00.00}\t{6:00.00}\t{7:00.00}\t{8:00.00}\t{9:00.00}\t{10:00.00}\t{11}\n",
                pLoad, pUtilization, GetTail(0), GetTail(0.5), GetMean(), GetTail(0.9), GetTail(0.95), GetTail(0.99), GetTail(0.999), GetTail(1), GetRate(), count);
            return s;
        }

        public override string GetSignature(double pLoad, double pUtilization, bool pOutputFlag)
        {
            return ToString() + "\n" + GetMeasurement(pLoad, pUtilization, pOutputFlag);
        }
    }
}