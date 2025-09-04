using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public enum TraceType { AllocationTrace, PodLifeCycleTrace }
    public enum TraceLineType { Allocation, Deallocation }
    public class TraceLineFields
    {
        public TraceLineType TraceLineType;
        public DateTime RealTime;
        public double TraceRelativeTime;
        public double RelativeTimePoint;
        public double ReferenceTimePoint;
        public string Runtime;
        public string RuntimeVersion;
        public double Cores;
        public int Pods;
        public double AllocationLatency;
        public string PodUUID;

        public TraceLineFields() { }

        public TraceLineFields(TraceLineFields pTraceLine)
        {
            TraceLineType = pTraceLine.TraceLineType;
            RealTime = pTraceLine.RealTime;
            RelativeTimePoint = pTraceLine.RelativeTimePoint;
            ReferenceTimePoint = pTraceLine.ReferenceTimePoint;
            Runtime = pTraceLine.Runtime;
            RuntimeVersion = pTraceLine.RuntimeVersion;
            Cores = pTraceLine.Cores;
            Pods = pTraceLine.Pods;
            AllocationLatency = pTraceLine.AllocationLatency;
            PodUUID = pTraceLine.PodUUID;
            TraceRelativeTime = pTraceLine.TraceRelativeTime;
        }

        public static PoolLabel ConvertToPoolLabel(string runtime, string runtimeVersion, double cores)
        {
            return new PoolLabel(new AllocationLabel(runtime, runtimeVersion), cores);
        }

        public static PoolLabel ConvertToPoolLabel(TraceLineFields traceLine)
        {
            return ConvertToPoolLabel(traceLine.Runtime, traceLine.RuntimeVersion, traceLine.Cores);
        }
        public static AllocationLabel ConvertToAllocationLabel(TraceLineFields traceLine)
        {
            return new AllocationLabel(traceLine.Runtime, traceLine.RuntimeVersion);
        }

        public override string ToString()
        {
            return String.Format("RealTime {0}, RelativeTimePoint {1:0.00}, ReferenceTimePoint {2:0.00}, Runtime {4}, RuntimeVersion {5} Cores {6} Pods{7} Latency {8}",
                                    RealTime, RelativeTimePoint, ReferenceTimePoint, Runtime, RuntimeVersion, Cores, Pods, AllocationLatency);
        }
    }

    public class PodLifeCycleLineFields
    {
        public string PodUUID;
        public DateTime CreationRealTime;
        public double CreationRelativeTime;
        public double CreationDuration;
        public double PendingDuration;
        public double ReadyDuration;
        public double AllocationDuration;
        public double SpecializationDuration;
        public double UserWorkloadDuration;
        public double DeletionDuration;
        public double RecyclingDuration;
        public bool HasErrors;

        public PodLifeCycleLineFields() { HasErrors = false; }

        public void Assert()
        {
            Debug.Assert(CreationRelativeTime >= 0);
            Debug.Assert(CreationDuration >= 0);
            Debug.Assert(PendingDuration >= 0);
            Debug.Assert(ReadyDuration >= 0);
            Debug.Assert(AllocationDuration >= 0);
            Debug.Assert(SpecializationDuration >= 0);
            Debug.Assert(UserWorkloadDuration >= 0);
            Debug.Assert(DeletionDuration >= 0);
            Debug.Assert(RecyclingDuration >= 0);
        }
    }

    public class SmoothedTrace
    {
        public double WindowSize;
        public List<(int, double)> Trace;
        public static SmoothedTrace CreateSmoothedTrace(List<double> originalTrace, double WindowSize)
        {
            SmoothedTrace smoothedTrace = new SmoothedTrace
            {
                WindowSize = WindowSize
            };
            double referenceTS = 0;
            int totalWindowsCount = (int)(Math.Ceiling(originalTrace[originalTrace.Count - 1] - referenceTS) / WindowSize);
            smoothedTrace.Trace = Enumerable.Repeat((0, 0.0), totalWindowsCount + 1).ToList();
            for (int i = 0; i < originalTrace.Count; i++)
            {
                var ts = originalTrace[i];
                var diff = ts - referenceTS;
                int windowIdx = (int)(diff / WindowSize);
                if (diff % WindowSize == 0 && i != 0)
                {
                    windowIdx++;
                }
                if (smoothedTrace.Trace[windowIdx].Item2 == 0)
                {
                    smoothedTrace.Trace[windowIdx] = (i, smoothedTrace.Trace[windowIdx].Item2 + 1);
                }
                else
                {
                    smoothedTrace.Trace[windowIdx] = (smoothedTrace.Trace[windowIdx].Item1, smoothedTrace.Trace[windowIdx].Item2 + 1);
                }
            }

            for (int i = 0; i < totalWindowsCount; i++)
            {
                var element = smoothedTrace.Trace[i];
                smoothedTrace.Trace[i] = (element.Item1, element.Item2 / WindowSize);
            }
            if (smoothedTrace.Trace.Count >= 2)
                smoothedTrace.Trace = smoothedTrace.Trace.GetRange(0, smoothedTrace.Trace.Count - 1);

            return smoothedTrace;
        }
    }

    public class PoolEmpiricalDistributions
    {
        public List<double> ArrivalTimeList;
        public List<double> SampledRecyclingTimeList;
        public List<double> InputRecyclingTimeList;
        public SmoothedTrace SmoothedTrace;
        public Dictionary<double, IDistribution> RequestRateChangeDistribution;
        public Dictionary<double, IDistribution> AverageRequestRateDistribution;
        public Dictionary<double, IDistribution> PercentileToPoolSizeDistributionMap;
        public IDistribution IgnoredRequestsDistribution;
        public Dictionary<double, IDistribution>? PercentileToRecyclingBasedPoolSizeDistributionMap;
        public Dictionary<double, IDistribution>? hostRolePoolSizeDistribution;
        public Dictionary<int, IDistribution> BufferSizeDistributionMap;
        public Dictionary<int, IDistribution> ReplenishmentTimePointDistributionMap;
        public Dictionary<int, double> PoolSizeToSuccessRateMap;
        public SortedDictionary<double, double> RateChangeForDifferentWindowSizesList;
        public List<int> HoppingWindows;
        public int allocationRequestsCount;
        public int deallocationRequestsCount;
        public PodLifeCycleDistributions PodLifeCycleDistributions;
        public double OptimalWindowSize;

        public PoolEmpiricalDistributions(List<double> targetPercentiles)
        {
            ArrivalTimeList = new List<double>();
            SampledRecyclingTimeList = new List<double>();
            InputRecyclingTimeList = new List<double>();
            RequestRateChangeDistribution = new Dictionary<double, IDistribution>();
            AverageRequestRateDistribution = new Dictionary<double, IDistribution>();
            PercentileToPoolSizeDistributionMap = new Dictionary<double, IDistribution>();
            PercentileToRecyclingBasedPoolSizeDistributionMap = new Dictionary<double, IDistribution>();
            hostRolePoolSizeDistribution = new Dictionary<double, IDistribution>();
            PoolSizeToSuccessRateMap = new Dictionary<int, double>();
            BufferSizeDistributionMap = new Dictionary<int, IDistribution>();
            ReplenishmentTimePointDistributionMap = new Dictionary<int, IDistribution>();
            IgnoredRequestsDistribution = new DistributionEmpiricalDoubleFrequencyArray();
            HoppingWindows = new List<int>();
            foreach (var percentile in targetPercentiles)
            {
                PercentileToPoolSizeDistributionMap.Add(percentile, new DistributionEmpiricalDoubleFrequencyArray());
                RequestRateChangeDistribution.Add(percentile, new DistributionEmpiricalFrequencyArray());
                AverageRequestRateDistribution.Add(percentile, new DistributionEmpiricalFrequencyArray());
                PercentileToRecyclingBasedPoolSizeDistributionMap.Add(percentile, new DistributionEmpiricalFrequencyArray());
                hostRolePoolSizeDistribution.Add(percentile, new DistributionEmpiricalFrequencyArray());
            }
            PodLifeCycleDistributions = new PodLifeCycleDistributions(
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical(),
                new DistributionEmpiricalPractical()
            );
            RateChangeForDifferentWindowSizesList = new SortedDictionary<double, double>();
            allocationRequestsCount = 0;
            deallocationRequestsCount = 0;
        }
    }
    public class Trace
    {
        private readonly TraceType _traceType;
        private readonly TraceReader _traceReader;
        private readonly TraceReader _podLifeCycleTraceReader;
        public List<PoolGroupParameters> PoolGroupParametersList;
        public IDictionary<PoolLabel, PoolEmpiricalDistributions> PoolLabelToDistributions;
        public IDictionary<PoolLabel, List<TraceLineFields>> PoolLabelToTraceLines;
        public List<(PoolLabel, double)> requestsList;
        public List<(PoolLabel, double)> deallocationList;
        private double _referenceTimePoint;
        private DateTime _referenceDateTime;
        private IDictionary<string, TraceLineFields> PodUuidToTraceLine;
        public Trace(TraceType pTraceType, string pAllocationTracePath, string? pPodLifeCycleTracePath)
        {
            _traceType = pTraceType;
            _referenceTimePoint = 0.0;
            _traceReader = new TraceReader(pAllocationTracePath, Parameter.TraceSkipLinesCount, TraceType.AllocationTrace);
            if (pPodLifeCycleTracePath != null)
                _podLifeCycleTraceReader = new TraceReader(pPodLifeCycleTracePath, Parameter.TraceSkipLinesCount, TraceType.PodLifeCycleTrace);

            PoolLabelToDistributions = new Dictionary<PoolLabel, PoolEmpiricalDistributions>();
            PoolLabelToTraceLines = new Dictionary<PoolLabel, List<TraceLineFields>>();
            PodUuidToTraceLine = new Dictionary<string, TraceLineFields>();
            PoolGroupParametersList = new List<PoolGroupParameters>();
            requestsList = new List<(PoolLabel, double)>();
            deallocationList = new List<(PoolLabel, double)>();
        }


        public void SampleRecyclingTrace(Experiment exp)
        {
            var poolLabels = PoolLabelToTraceLines.Keys.ToList();
            deallocationList.Clear();
            foreach (var poolLabel in poolLabels)
            {
                var poolDistributions = PoolLabelToDistributions[poolLabel];
                var recyclingTrace = poolDistributions.SampledRecyclingTimeList;
                recyclingTrace.Clear();
            }
            var tmpList = new List<(PoolLabel, double)>();
            // foreach (var request in requestsList)
            for (var i = 0; i < requestsList.Count; i++)
            {
                var request = requestsList[i];
                var poolLabel = request.Item1;
                if (!PoolLabelToDistributions.ContainsKey(poolLabel))
                {
                    // requestsList.RemoveAt(i);
                    continue;
                }
                var poolDistributions = PoolLabelToDistributions[poolLabel];
                var recyclingTrace = poolDistributions.SampledRecyclingTimeList;
                var allocatedToRecycledDist = poolDistributions.PodLifeCycleDistributions._allocatedToRecycledDistribution;
                var podSupplyDist = poolDistributions.PodLifeCycleDistributions._supplyDelayDistribution;
                double allocatedToRecycledTime = 0.0;
                double supplyTime = 0.0;
                if (exp.RecyclingTraceSamplingApproach == RecyclingTraceSamplingApproach.Average)
                {
                    allocatedToRecycledTime = allocatedToRecycledDist.GetMean();
                    supplyTime = podSupplyDist.GetMean();
                }
                else if (exp.RecyclingTraceSamplingApproach == RecyclingTraceSamplingApproach.Random)
                {
                    allocatedToRecycledTime = allocatedToRecycledDist.GetSample();
                    supplyTime = podSupplyDist.GetSample();
                }
                var requestTime = request.Item2;
                recyclingTrace.Add(requestTime + allocatedToRecycledTime);
                deallocationList.Add((poolLabel, requestTime + allocatedToRecycledTime));
                tmpList.Add(request);
            }
            foreach (var poolLabel in poolLabels)
            {
                var poolDistributions = PoolLabelToDistributions[poolLabel];
                var recyclingTrace = poolDistributions.SampledRecyclingTimeList;
                recyclingTrace.Sort();
            }
            deallocationList = deallocationList.OrderBy(t => t.Item2).ToList();
            tmpList = tmpList.OrderBy(t => t.Item2).ToList();
            requestsList = tmpList;
        }

        public void ParsePodLifeCycleTrace(bool useCombinedPool)
        {
            int counter = 0;
            PodLifeCycleLineFields? podLifeCycleLine = _podLifeCycleTraceReader.ParsePodLifeCycleLine();
            while (podLifeCycleLine != null && PodUuidToTraceLine.Count > 0)
            {
                if (podLifeCycleLine.HasErrors)
                {
                    podLifeCycleLine = _podLifeCycleTraceReader.ParsePodLifeCycleLine();
                    continue;
                }
                if (PodUuidToTraceLine.ContainsKey(podLifeCycleLine.PodUUID))
                {
                    TraceLineFields allocationLine = PodUuidToTraceLine[podLifeCycleLine.PodUUID];
                    PoolLabel poolLabel = TraceLineFields.ConvertToPoolLabel(allocationLine.Runtime, allocationLine.RuntimeVersion, allocationLine.Cores);
                    var podLifeCycleDistributions = PoolLabelToDistributions[poolLabel].PodLifeCycleDistributions;
                    PodLifeCycleDistributions? combinedPoolPodLifeCycleDistributions = null;
                    if (useCombinedPool)
                    {
                        combinedPoolPodLifeCycleDistributions = PoolLabelToDistributions[Parameter.CombinedPoolLabel].PodLifeCycleDistributions;
                    }

                    if (podLifeCycleLine.CreationDuration > 0)
                    {
                        podLifeCycleDistributions._creationDemandDistribution.AddValue(podLifeCycleLine.CreationDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._creationDemandDistribution.AddValue(podLifeCycleLine.CreationDuration);
                    }
                    if (podLifeCycleLine.PendingDuration > 0)
                    {
                        podLifeCycleDistributions._pendingDemandDistribution.AddValue(podLifeCycleLine.PendingDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._pendingDemandDistribution.AddValue(podLifeCycleLine.PendingDuration);
                    }
                    if (podLifeCycleLine.ReadyDuration > 0)
                    {
                        podLifeCycleDistributions._idleDurationDistribution.AddValue(podLifeCycleLine.ReadyDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._idleDurationDistribution.AddValue(podLifeCycleLine.ReadyDuration);
                    }
                    if (podLifeCycleLine.AllocationDuration > 0)
                    {
                        podLifeCycleDistributions._allocatedDemandDistribution.AddValue(podLifeCycleLine.AllocationDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._allocatedDemandDistribution.AddValue(podLifeCycleLine.AllocationDuration);
                    }
                    if (podLifeCycleLine.SpecializationDuration >= 0)
                    {
                        podLifeCycleDistributions._specializedDemandDistribution.AddValue(podLifeCycleLine.SpecializationDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._specializedDemandDistribution.AddValue(podLifeCycleLine.SpecializationDuration);
                    }
                    if (podLifeCycleLine.UserWorkloadDuration > 0)
                    {
                        podLifeCycleDistributions._userWorkloadDemandDistribution.AddValue(podLifeCycleLine.UserWorkloadDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._userWorkloadDemandDistribution.AddValue(podLifeCycleLine.UserWorkloadDuration);
                    }
                    if (podLifeCycleLine.DeletionDuration > 0)
                    {
                        podLifeCycleDistributions._deleteDemandDistribution.AddValue(podLifeCycleLine.DeletionDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._deleteDemandDistribution.AddValue(podLifeCycleLine.DeletionDuration);
                    }
                    if (podLifeCycleLine.RecyclingDuration > 0)
                    {
                        podLifeCycleDistributions._vmRecyclingDemandDistribution.AddValue(podLifeCycleLine.RecyclingDuration);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._vmRecyclingDemandDistribution.AddValue(podLifeCycleLine.RecyclingDuration);
                    }
                    if (podLifeCycleLine.CreationDuration > 0 && podLifeCycleLine.PendingDuration > 0)
                    {
                        var totalSupplyDelay = podLifeCycleLine.CreationDuration + podLifeCycleLine.PendingDuration;
                        podLifeCycleDistributions._supplyDelayDistribution.AddValue(totalSupplyDelay);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._supplyDelayDistribution.AddValue(totalSupplyDelay);
                    }

                    if (podLifeCycleLine.DeletionDuration > 0 && podLifeCycleLine.RecyclingDuration > 0)
                    {
                        var totalDeleteRecycleDelay = podLifeCycleLine.DeletionDuration + podLifeCycleLine.RecyclingDuration;
                        podLifeCycleDistributions._deleteRecycleDelayDistribution.AddValue(totalDeleteRecycleDelay);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._deleteRecycleDelayDistribution.AddValue(totalDeleteRecycleDelay);
                    }

                    if (podLifeCycleLine.CreationDuration > 0
                        && podLifeCycleLine.PendingDuration > 0
                        && podLifeCycleLine.AllocationDuration > 0
                        && podLifeCycleLine.SpecializationDuration >= 0
                        && podLifeCycleLine.UserWorkloadDuration > 0
                        && podLifeCycleLine.DeletionDuration > 0
                        && podLifeCycleLine.RecyclingDuration > 0)
                    {
                        var total = podLifeCycleLine.CreationDuration + podLifeCycleLine.PendingDuration
                                    // + podLifeCycleLine.ReadyDuration
                                    + podLifeCycleLine.AllocationDuration + podLifeCycleLine.SpecializationDuration
                                    + podLifeCycleLine.UserWorkloadDuration + podLifeCycleLine.DeletionDuration
                                    + podLifeCycleLine.RecyclingDuration;

                        podLifeCycleDistributions._fullLifeCycleDistribution.AddValue(total);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._fullLifeCycleDistribution.AddValue(total);


                        var allocatedToRecycled = podLifeCycleLine.AllocationDuration + podLifeCycleLine.SpecializationDuration
                                    + podLifeCycleLine.UserWorkloadDuration + podLifeCycleLine.DeletionDuration
                                    + podLifeCycleLine.RecyclingDuration;

                        podLifeCycleDistributions._allocatedToRecycledDistribution.AddValue(allocatedToRecycled);
                        if (combinedPoolPodLifeCycleDistributions != null)
                            combinedPoolPodLifeCycleDistributions._allocatedToRecycledDistribution.AddValue(allocatedToRecycled);

                        podLifeCycleDistributions._lifeCyclesList.Add(new PodLifeCycleTimestamps(
                                0, podLifeCycleLine.CreationDuration, podLifeCycleLine.PendingDuration,
                                podLifeCycleLine.ReadyDuration, podLifeCycleLine.AllocationDuration,
                                podLifeCycleLine.SpecializationDuration,
                                podLifeCycleLine.UserWorkloadDuration, podLifeCycleLine.DeletionDuration,
                                podLifeCycleLine.RecyclingDuration));
                    }
                    PodUuidToTraceLine.Remove(podLifeCycleLine.PodUUID);
                }
                podLifeCycleLine = _podLifeCycleTraceReader.ParsePodLifeCycleLine();
                counter++;
            }
        }

        public void RemovePools(List<PoolLabel> pools)
        {
            var poolLabels = PoolLabelToTraceLines.Keys.ToList();
            foreach (var poolLabel in poolLabels)
            {
                if (pools.Contains(poolLabel))
                {
                    PoolLabelToTraceLines.Remove(poolLabel);
                    PoolLabelToDistributions.Remove(poolLabel);
                    if (PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                    {
                        PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                    }
                    if (PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                    {
                        PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                    }
                }
            }
        }

        public void RemoveAllPoolsExcept(List<PoolLabel> poolsToKeep)
        {
            var poolLabels = PoolLabelToTraceLines.Keys.ToList();
            foreach (var poolLabel in poolLabels)
            {
                if (!poolsToKeep.Contains(poolLabel))
                {
                    PoolLabelToTraceLines.Remove(poolLabel);
                    PoolLabelToDistributions.Remove(poolLabel);
                    if (PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                    {
                        PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                    }
                    if (PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                    {
                        PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                    }
                }
            }
        }

        public void Parse(Experiment exp)
        {
            ParseAllocationTrace(exp.TargetPercentiles, exp.UseCombinedPool);
            ParsePodLifeCycleTrace(exp.UseCombinedPool);
            Close();
            RemovePoolsWithNoData();
        }

        public static void RemoveUncommonPoolsWithStaticPools(Experiment exp)
        {
            var poolLabels = exp.Trace.PoolLabelToTraceLines.Keys.ToList();
            var testPoolLabels = exp.TestTrace.PoolLabelToTraceLines.Keys.ToList();
            var staticPoolLabels = exp.StaticPoolSizesMap;
            foreach (var poolLabel in staticPoolLabels.Keys.ToList())
            {
                if (poolLabels.Contains(poolLabel) && testPoolLabels.Contains(poolLabel))
                {
                    continue;
                }
                if (poolLabels.Contains(poolLabel))
                {
                    exp.Trace.PoolLabelToTraceLines.Remove(poolLabel);
                }
                if (testPoolLabels.Contains(poolLabel))
                {
                    exp.TestTrace.PoolLabelToDistributions.Remove(poolLabel);
                }

                if (exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                {
                    exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                }
                if (exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel)
                        && exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                {
                    exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                }

                if (exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                {
                    exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                }
                if (exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel)
                        && exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                {
                    exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                }

                exp.StaticPoolSizesMap.Remove(poolLabel);
            }

            foreach (var poolLabel in poolLabels)
            {
                if (staticPoolLabels.ContainsKey(poolLabel))
                {
                    continue;
                }

                if (poolLabels.Contains(poolLabel))
                {
                    exp.Trace.PoolLabelToTraceLines.Remove(poolLabel);
                }
                if (testPoolLabels.Contains(poolLabel))
                {
                    exp.TestTrace.PoolLabelToDistributions.Remove(poolLabel);
                }

                if (exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                {
                    exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                }
                if (exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                {
                    exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                }

                if (exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                {
                    exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                }
                if (exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                {
                    exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                }
            }
        }


        public static void RemoveUncommonPools(Experiment exp)
        {
            var poolLabels = exp.Trace.PoolLabelToTraceLines.Keys.ToList();
            var testPoolLabels = exp.TestTrace.PoolLabelToTraceLines.Keys.ToList();
            foreach (var poolLabel in testPoolLabels)
            {
                if (poolLabels.Contains(poolLabel))
                {
                    continue;
                }
                exp.TestTrace.PoolLabelToTraceLines.Remove(poolLabel);
                exp.TestTrace.PoolLabelToDistributions.Remove(poolLabel);
                if (exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                {
                    exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                }
                if (exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                {
                    exp.TestTrace.PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                }
            }

            foreach (var poolLabel in poolLabels)
            {
                if (testPoolLabels.Contains(poolLabel))
                {
                    continue;
                }
                exp.Trace.PoolLabelToTraceLines.Remove(poolLabel);
                exp.Trace.PoolLabelToDistributions.Remove(poolLabel);
                if (exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                {
                    exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                }
                if (exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                {
                    exp.Trace.PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                }
            }

            if (exp.StaticPoolSizesMap != null)
            {
                RemoveUncommonPoolsWithStaticPools(exp);
            }
        }

        public void RemovePoolsWithNoData()
        {
            var poolLabels = PoolLabelToTraceLines.Keys.ToList();
            foreach (var poolLabel in poolLabels)
            {
                var podLifeCycleDistributions = PoolLabelToDistributions[poolLabel].PodLifeCycleDistributions;
                if (PoolLabelToTraceLines[poolLabel].Count < 200 || podLifeCycleDistributions.MinLength() < 200)
                {
                    PoolLabelToTraceLines.Remove(poolLabel);
                    PoolLabelToDistributions.Remove(poolLabel);
                    if (PoolGroupParametersList[0].RuntimeToPoolParameters.ContainsKey(poolLabel.AllocationLabel))
                    {
                        PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Remove(poolLabel.Cores);
                    }
                    if (PoolGroupParametersList[0].RuntimeToPoolParameters[poolLabel.AllocationLabel].Count == 0)
                    {
                        PoolGroupParametersList[0].RuntimeToPoolParameters.Remove(poolLabel.AllocationLabel);
                    }
                }
            }
        }

        public void ParseAllocationTrace(List<double> percentileList, bool useCombinedPool)
        {
            int counter = 0;
            TraceLineFields? lineFields = _traceReader.ParseTraceLine();

            PoolGroupParameters poolGroupParameters = new PoolGroupParameters(new PoolGroupId(0));
            PoolGroupParametersList.Add(poolGroupParameters);

            if (useCombinedPool)
            {
                poolGroupParameters.RuntimeToPoolParameters.Add(Parameter.CombinedPoolAllocationLabel, new SortedList<double, PoolParameters>());
                PoolLabelToDistributions[Parameter.CombinedPoolLabel] = new PoolEmpiricalDistributions(percentileList);
                PoolLabelToTraceLines[Parameter.CombinedPoolLabel] = new List<TraceLineFields>();
            }

            double prevRelativeTimePoint = 0.0;
            while (lineFields != null)
            {
                if (lineFields.TraceLineType == TraceLineType.Allocation && lineFields.PodUUID != null)
                {
                    PodUuidToTraceLine[lineFields.PodUUID] = lineFields;
                }
                if (lineFields.TraceLineType == TraceLineType.Deallocation)
                {
                    if (!PopulateRuntimeFields(lineFields))
                    {
                        lineFields = _traceReader.ParseTraceLine();
                        continue;
                    }
                }
                AllocationLabel lineAllocationLabel = TraceLineFields.ConvertToAllocationLabel(lineFields);
                if (!poolGroupParameters.RuntimeToPoolParameters.ContainsKey(lineAllocationLabel))
                {
                    poolGroupParameters.RuntimeToPoolParameters.Add(lineAllocationLabel, new SortedList<double, PoolParameters>());
                }
                if (!poolGroupParameters.RuntimeToPoolParameters[lineAllocationLabel].ContainsKey(lineFields.Cores))
                {
                    poolGroupParameters.RuntimeToPoolParameters[lineAllocationLabel].Add(lineFields.Cores,
                                new PoolParameters(poolGroupParameters.PoolGroupId,
                                                    lineAllocationLabel, lineFields.Cores,
                                                    null, 0, 0, 0, 0,
                                                    0, 0, null));
                }
                var poolLabel = TraceLineFields.ConvertToPoolLabel(lineFields);
                if (!PoolLabelToDistributions.ContainsKey(poolLabel))
                {
                    PoolLabelToDistributions[poolLabel] = new PoolEmpiricalDistributions(percentileList);
                    PoolLabelToTraceLines[poolLabel] = new List<TraceLineFields>();
                }
                if (counter == 0)
                {
                    _referenceDateTime = lineFields.RealTime;
                }
                lineFields.RelativeTimePoint = _traceReader.ComputeTimeDiff(_referenceDateTime, lineFields.RealTime);
                lineFields.ReferenceTimePoint = _referenceTimePoint;
                PoolLabelToTraceLines[poolLabel].Add(lineFields);

                if (lineFields.TraceLineType == TraceLineType.Allocation)
                {
                    PoolLabelToDistributions[poolLabel].allocationRequestsCount++;
                    PoolLabelToDistributions[poolLabel].ArrivalTimeList.Add(lineFields.RelativeTimePoint);
                }
                else
                {
                    PoolLabelToDistributions[poolLabel].deallocationRequestsCount++;
                    var allocationTime = GetPodAllocationTime(lineFields);
                    PoolLabelToDistributions[poolLabel].InputRecyclingTimeList.Add(lineFields.RelativeTimePoint - allocationTime);
                }

                if (useCombinedPool)
                {
                    TraceLineFields singlePoolTraceLine = new TraceLineFields(lineFields);
                    singlePoolTraceLine.Runtime = Parameter.CombinedPoolAllocationLabel.Runtime;
                    singlePoolTraceLine.RuntimeVersion = Parameter.CombinedPoolAllocationLabel.RuntimeVersion;
                    singlePoolTraceLine.Cores = Parameter.CombinedPoolLabel.Cores;
                    PoolLabelToTraceLines[Parameter.CombinedPoolLabel].Add(singlePoolTraceLine);
                    if (lineFields.TraceLineType == TraceLineType.Allocation)
                    {
                        PoolLabelToDistributions[Parameter.CombinedPoolLabel].allocationRequestsCount++;
                        PoolLabelToDistributions[Parameter.CombinedPoolLabel].ArrivalTimeList.Add(singlePoolTraceLine.RelativeTimePoint);
                    }
                    else
                    {
                        PoolLabelToDistributions[Parameter.CombinedPoolLabel].deallocationRequestsCount++;
                        var allocationTime = GetPodAllocationTime(lineFields);
                        PoolLabelToDistributions[Parameter.CombinedPoolLabel].InputRecyclingTimeList.Add(lineFields.RelativeTimePoint - allocationTime);
                    }
                }
                Debug.Assert(prevRelativeTimePoint <= lineFields.RelativeTimePoint);

                if (lineFields.TraceLineType == TraceLineType.Allocation)
                {
                    requestsList.Add((new PoolLabel(lineAllocationLabel, lineFields.Cores), lineFields.RelativeTimePoint));
                    if (useCombinedPool)
                    {
                        requestsList.Add((Parameter.CombinedPoolLabel, lineFields.RelativeTimePoint));
                    }
                }
                else
                {
                    deallocationList.Add((new PoolLabel(lineAllocationLabel, lineFields.Cores), lineFields.RelativeTimePoint));
                    if (useCombinedPool)
                    {
                        deallocationList.Add((Parameter.CombinedPoolLabel, lineFields.RelativeTimePoint));
                    }
                }

                prevRelativeTimePoint = lineFields.RelativeTimePoint;
                lineFields = _traceReader.ParseTraceLine();
                counter++;
            }
        }

        private double GetPodAllocationTime(TraceLineFields lineFields)
        {
            string uuid = lineFields.PodUUID;
            if (!PodUuidToTraceLine.ContainsKey(uuid))
            {
                return -1;
            }
            TraceLineFields allocationLineFields = PodUuidToTraceLine[uuid];
            return allocationLineFields.RelativeTimePoint;
        }

        private bool PopulateRuntimeFields(TraceLineFields lineFields)
        {
            string uuid = lineFields.PodUUID;
            if (!PodUuidToTraceLine.ContainsKey(uuid))
            {
                return false;
            }
            TraceLineFields allocationLineFields = PodUuidToTraceLine[uuid];
            lineFields.Runtime = allocationLineFields.Runtime;
            lineFields.RuntimeVersion = allocationLineFields.RuntimeVersion;
            lineFields.Cores = allocationLineFields.Cores;
            return true;
        }

        public List<AllocationRequest> GetNextRequestsBatch(int batchIndex, double newRefernceTimePoint = -1.0)
        {
            List<AllocationRequest> requestsList = new List<AllocationRequest>();
            foreach (var (poolLabel, poolTraceLines) in PoolLabelToTraceLines)
            {
                if (batchIndex >= poolTraceLines.Count)
                {
                    continue;
                }
                for (int i = 0; i < poolTraceLines[batchIndex].Pods; i++)
                {
                    AllocationRequest request = LineFieldsToAllocationRequest(poolTraceLines[batchIndex], newRefernceTimePoint);
                    requestsList.Add(request);
                }
            }
            return requestsList;
        }

        public PodLifeCycleTimestamps SamplePodLifeCycle(PoolLabel poolLabel,
                                                        SamplingApproach samplingApproach,
                                                        bool ignorePodTransitionsExceptCreation)
        {
            PodLifeCycleTimestamps podLifeCycleTimestamps = new PodLifeCycleTimestamps(0);
            var lifeCycleDistributions = PoolLabelToDistributions[poolLabel].PodLifeCycleDistributions;
            if (samplingApproach == SamplingApproach.Average)
            {
                if (ignorePodTransitionsExceptCreation)
                {
                    podLifeCycleTimestamps._durationCreation = 0;
                    podLifeCycleTimestamps._durationPending = lifeCycleDistributions._supplyDelayDistribution.GetMean();
                    podLifeCycleTimestamps._durationReady = 0;
                    podLifeCycleTimestamps._durationAllocated = 0;
                    podLifeCycleTimestamps._durationSpecialized = 0;
                    podLifeCycleTimestamps._durationUserWorkload = 0;
                    podLifeCycleTimestamps._durationDeleted = 0;
                    podLifeCycleTimestamps._durationRecyclingVm = 0;
                }
                else
                {
                    podLifeCycleTimestamps._durationCreation = 0;
                    podLifeCycleTimestamps._durationPending = lifeCycleDistributions._supplyDelayDistribution.GetMean();
                    podLifeCycleTimestamps._durationReady = 0;
                    podLifeCycleTimestamps._durationAllocated = lifeCycleDistributions._allocatedDemandDistribution.GetMean();
                    podLifeCycleTimestamps._durationSpecialized = 0;
                    podLifeCycleTimestamps._durationUserWorkload = lifeCycleDistributions._userWorkloadDemandDistribution.GetMean();
                    podLifeCycleTimestamps._durationDeleted = lifeCycleDistributions._deleteRecycleDelayDistribution.GetMean();
                    podLifeCycleTimestamps._durationRecyclingVm = 0;
                }

            }
            else if (samplingApproach == SamplingApproach.Random)
            {
                if (ignorePodTransitionsExceptCreation)
                {
                    podLifeCycleTimestamps._durationCreation = 0;
                    podLifeCycleTimestamps._durationPending = lifeCycleDistributions._supplyDelayDistribution.GetSample();
                    podLifeCycleTimestamps._durationReady = 0;
                    podLifeCycleTimestamps._durationAllocated = 0;
                    podLifeCycleTimestamps._durationSpecialized = 0;
                    podLifeCycleTimestamps._durationUserWorkload = 0;
                    podLifeCycleTimestamps._durationDeleted = 0;
                    podLifeCycleTimestamps._durationRecyclingVm = 0;
                }
                else
                {
                    podLifeCycleTimestamps._durationCreation = 0;
                    podLifeCycleTimestamps._durationPending = lifeCycleDistributions._supplyDelayDistribution.GetSample();
                    podLifeCycleTimestamps._durationReady = 0;
                    podLifeCycleTimestamps._durationAllocated = lifeCycleDistributions._allocatedDemandDistribution.GetSample();
                    podLifeCycleTimestamps._durationSpecialized = 0;
                    podLifeCycleTimestamps._durationUserWorkload = lifeCycleDistributions._userWorkloadDemandDistribution.GetSample();
                    podLifeCycleTimestamps._durationDeleted = lifeCycleDistributions._deleteRecycleDelayDistribution.GetSample();
                    podLifeCycleTimestamps._durationRecyclingVm = 0;
                }
            }
            return podLifeCycleTimestamps;
        }

        public TraceLineFields? GetNextTraceLine(IDictionary<PoolLabel, int> poolLabelToIndex)
        {
            TraceLineFields? traceLine = null;
            foreach (var (poolLabel, poolTraceLines) in PoolLabelToTraceLines)
            {
                if (!poolLabelToIndex.ContainsKey(poolLabel))
                {
                    continue;
                }
                int requestIndex = poolLabelToIndex[poolLabel];
                if (requestIndex >= poolTraceLines.Count)
                {
                    continue;
                }
                var tmpTraceLine = poolTraceLines[requestIndex];
                if (traceLine == null || tmpTraceLine.RelativeTimePoint < traceLine.RelativeTimePoint)
                {
                    traceLine = tmpTraceLine;
                }
            }
            return traceLine;
        }

        public AllocationRequest? GetNextRequest(IDictionary<PoolLabel, int> poolLabelToIndex, double newRefernceTimePoint = -1.0)
        {
            AllocationRequest? request = null;
            foreach (var (poolLabel, poolTraceLines) in PoolLabelToTraceLines)
            {
                if (!poolLabelToIndex.ContainsKey(poolLabel))
                {
                    continue;
                }
                int requestIndex = poolLabelToIndex[poolLabel];
                if (requestIndex >= poolTraceLines.Count)
                {
                    continue;
                }
                AllocationRequest tmpRequest = LineFieldsToAllocationRequest(poolTraceLines[requestIndex], newRefernceTimePoint);
                if (request == null || tmpRequest.ArrivalTimePoint < request.ArrivalTimePoint)
                {
                    request = tmpRequest;
                }
            }

            return request;
        }

        public AllocationRequest LineFieldsToAllocationRequest(TraceLineFields lineFields, double newRefernceTimePoint = -1.0)
        {
            var allocationLabel = TraceLineFields.ConvertToAllocationLabel(lineFields);
            double arrivalTimePoint = lineFields.RelativeTimePoint;
            if (newRefernceTimePoint != -1.0)
            {
                arrivalTimePoint = lineFields.RelativeTimePoint + newRefernceTimePoint - lineFields.ReferenceTimePoint;
            }
            RequestType requestType = lineFields.TraceLineType == TraceLineType.Allocation ? RequestType.Allocation : RequestType.Deallocation;
            return new AllocationRequest(arrivalTimePoint, allocationLabel, 1, lineFields.Cores, requestType);
        }

        public void Close()
        {
            _traceReader.Close();
        }

        public override string ToString()
        {
            string str = "";
            str = str + String.Format("{0,-25} {1,-25} {2,-25} {3,-25}\n", "pool", "#allocations", "#trace line", "#life cycle");
            str = str + String.Format("{0,-25} {1,-25} {2,-25} {3,-25}\n", "-------", "-------", "-------", "-------");
            foreach (var (poolLabel, poolDistributions) in PoolLabelToDistributions)
            {

                List<int> length = [
                    (int)poolDistributions.PodLifeCycleDistributions._creationDemandDistribution.Count(),
                    (int)poolDistributions.PodLifeCycleDistributions._pendingDemandDistribution.Count(),
                    (int)poolDistributions.PodLifeCycleDistributions._supplyDelayDistribution.Count(),
                    (int)poolDistributions.PodLifeCycleDistributions._idleDurationDistribution.Count(),
                    (int)poolDistributions.PodLifeCycleDistributions._specializedDemandDistribution.Count(),
                    (int)poolDistributions.PodLifeCycleDistributions._userWorkloadDemandDistribution.Count(),
                    (int)poolDistributions.PodLifeCycleDistributions._deleteDemandDistribution.Count(),
                    (int)poolDistributions.PodLifeCycleDistributions._vmRecyclingDemandDistribution.Count(),
                    ];



                str = str + String.Format("{0,-25} {1,-25} {2,-25} {3,-25}\n",
                        poolLabel,
                        poolDistributions.allocationRequestsCount,
                        PoolLabelToTraceLines[poolLabel].Count,
                        length.Min()
                        );
            }
            return str;
        }

        internal string GetTraceSummaryText(Experiment exp, string? filePath)
        {
            var poolLabelToDistributions = PoolLabelToDistributions;
            string text = "Pool Label,Allocations Count,Deallocations Count,Pod Life Cycles Count\n";
            foreach (var (poolLabel, poolDistributions) in poolLabelToDistributions)
            {
                text += String.Format("{0},{1},{2},{3}\n",
                                poolLabel,
                                poolDistributions.allocationRequestsCount,
                                poolDistributions.deallocationRequestsCount,
                                poolDistributions.PodLifeCycleDistributions.MinLength());
            }
            text = text + "\n\n";
            text = text + String.Format("{0},{1},{2}\n",
                                            "", "Supply Delay Distribution", "Demand Rate Ditsribution");
            text = text + String.Format("{0},{1},{2},{3},{4},{5},",
                                            "Pool Label",
                                            "Avg", "P50", "P90", "P99", "P100");

            foreach (var percenile in exp.TargetPercentiles)
            {
                text = text + percenile + ",";
            }
            text = text + "\n";

            foreach (var (poolLabel, poolDistributions) in poolLabelToDistributions)
            {
                var podSupplyDistribution = poolDistributions.PodLifeCycleDistributions._supplyDelayDistribution;
                text = text + String.Format("{0},{1:0.00},{2:0.00},{3:0.00},{4:0.00},{5:0.00},",
                                            poolLabel,
                                            podSupplyDistribution.GetMean(),
                                            podSupplyDistribution.GetTail(0.5),
                                            podSupplyDistribution.GetTail(0.9),
                                            podSupplyDistribution.GetTail(0.99),
                                            podSupplyDistribution.GetTail(1.0)
                                            );

                foreach (var percentile in exp.TargetPercentiles)
                {
                    var maxTargetPercentile = exp.TargetPercentiles.Max();
                    var demandRateDistribution = poolDistributions.PercentileToPoolSizeDistributionMap[maxTargetPercentile];
                    if (exp.StaticPoolSizesMap == null)
                    {
                        if (demandRateDistribution is DistributionEmpiricalDoubleFrequencyArray && demandRateDistribution.Count() > 0)
                        {
                            DistributionEmpiricalDoubleFrequencyArray tmp = (DistributionEmpiricalDoubleFrequencyArray)demandRateDistribution;
                            text = text + String.Format("{0:0.00},", tmp.GetTail(percentile));
                        }
                        else
                        {
                            text = text + String.Format("{0:0.00},", 0);
                        }
                    }
                    else
                    {
                        text = text + String.Format("{0:0.00},", exp.StaticPoolSizesMap[poolLabel]);
                    }
                }
                text = text + "\n";
            }

            text = text + String.Format("PoolLabel, supply avg, allocation avg, user time avg, deletion avg\n");

            foreach (var (poolLabel, poolDistributions) in poolLabelToDistributions)
            {
                var lifeCycleDistributions = poolDistributions.PodLifeCycleDistributions;
                text = text + String.Format("{0},{1:0.00},{2:0.00},{3:0.00},{4:0.00}\n",
                                            poolLabel,
                                            lifeCycleDistributions._supplyDelayDistribution.GetMean(),
                                            lifeCycleDistributions._allocatedDemandDistribution.GetMean(),
                                            lifeCycleDistributions._userWorkloadDemandDistribution.GetMean(),
                                            lifeCycleDistributions._deleteRecycleDelayDistribution.GetMean()
                                            );
            }

            try
            {
                if (filePath != null)
                {
                    StreamWriter outputFile = new StreamWriter(filePath);
                    outputFile.WriteLine(text);
                    outputFile.Close();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            return text;
        }

    }

    public class TraceReader
    {
        private readonly TraceType _traceType;
        private readonly string _tracePath;
        private StreamReader reader;

        private int _linesCounter;
        public TraceReader(string pTracePath, int pSkipLines, TraceType pTraceType)
        {
            _traceType = pTraceType;
            _tracePath = pTracePath;
            try
            {
                reader = new StreamReader(_tracePath);
                for (int i = 0; i < pSkipLines; i++)
                {
                    ReadLine();
                }
                _linesCounter = 0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Environment.Exit(-1);
            }
        }

        public PodLifeCycleLineFields? ParsePodLifeCycleLine()
        {
            string? line = ReadLine();
            if (line == null)
            {
                return null;
            }
            PodLifeCycleLineFields? lineFields = LineToPodLifeCycle(line);
            return lineFields;
        }

        public TraceLineFields? ParseTraceLine()
        {
            string? line = ReadLine();
            if (line == null)
            {
                return null;
            }
            TraceLineFields? lineFields = LineToTraceFields(line);
            return lineFields;
        }

        public string? ReadLine()
        {
            try
            {
                var line = reader.ReadLine();
                _linesCounter++;
                return line;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Environment.Exit(-1);
            }
            return null;
        }

        public PodLifeCycleLineFields? LineToPodLifeCycle(string line)
        {
            try
            {
                var temp = line.Split(",");
                PodLifeCycleLineFields? podLifeCycle = new PodLifeCycleLineFields();
                podLifeCycle.PodUUID = temp[0].Trim();
                double pendingStartPoint = double.Parse(temp[2].Trim());
                double readyStartPoint = double.Parse(temp[3].Trim());
                double allocationStartPoint = double.Parse(temp[4].Trim());
                double allocationEndPoint = double.Parse(temp[5].Trim());
                double specializationStartPoint = double.Parse(temp[6].Trim());
                double specializationEndPoint = double.Parse(temp[7].Trim());
                double deleteStartPoint = double.Parse(temp[8].Trim());
                double recyclingStartPoint = double.Parse(temp[9].Trim());
                double recyclingEndPoint = double.Parse(temp[10].Trim());

                if (pendingStartPoint <= 0
                    || pendingStartPoint <= 0
                    || readyStartPoint <= 0
                    || allocationStartPoint <= 0
                    || allocationEndPoint <= 0
                    || specializationStartPoint <= 0
                    || specializationEndPoint <= 0
                    || deleteStartPoint <= 0
                    || recyclingStartPoint <= 0
                    || recyclingEndPoint <= 0
                    )
                {
                    podLifeCycle.HasErrors = true;
                    return podLifeCycle;
                }

                pendingStartPoint /= 1000.0;
                readyStartPoint /= 1000.0;
                allocationStartPoint /= 1000.0;
                allocationEndPoint /= 1000.0;
                specializationStartPoint /= 1000.0;
                specializationEndPoint /= 1000.0;
                deleteStartPoint /= 1000.0;
                recyclingStartPoint /= 1000.0;
                recyclingEndPoint /= 1000.0;

                double realDeleteTimePoint = deleteStartPoint;
                if (deleteStartPoint == 0)
                {
                    podLifeCycle.DeletionDuration = 0;
                    realDeleteTimePoint = recyclingStartPoint;
                }
                else
                {
                    podLifeCycle.DeletionDuration = recyclingStartPoint - deleteStartPoint;
                    if (podLifeCycle.DeletionDuration <= 0)
                    {
                        podLifeCycle.DeletionDuration = 0;
                    }
                }

                if (recyclingStartPoint == 0)
                {
                    realDeleteTimePoint = recyclingEndPoint;
                    podLifeCycle.RecyclingDuration = 0;
                }
                else
                {
                    podLifeCycle.RecyclingDuration = recyclingEndPoint - recyclingStartPoint;
                    if (podLifeCycle.RecyclingDuration <= 0)
                    {
                        realDeleteTimePoint = 0;
                        podLifeCycle.RecyclingDuration = 0;
                    }
                }

                podLifeCycle.CreationDuration = pendingStartPoint;
                if (readyStartPoint == 0)
                {
                    podLifeCycle.PendingDuration = 0;
                    podLifeCycle.ReadyDuration = 0;
                    podLifeCycle.AllocationDuration = 0;
                    podLifeCycle.SpecializationDuration = 0;
                    podLifeCycle.UserWorkloadDuration = 0;
                    podLifeCycle.Assert();
                    return podLifeCycle;
                }
                podLifeCycle.PendingDuration = readyStartPoint - pendingStartPoint;
                if (podLifeCycle.PendingDuration <= 0)
                {
                    podLifeCycle.HasErrors = true;
                    return podLifeCycle;
                }

                if (allocationStartPoint == 0)
                {
                    if (realDeleteTimePoint == 0)
                    {
                        podLifeCycle.ReadyDuration = 0;
                    }
                    else
                    {
                        podLifeCycle.ReadyDuration = realDeleteTimePoint - readyStartPoint;
                    }
                    podLifeCycle.AllocationDuration = 0;
                    podLifeCycle.SpecializationDuration = 0;
                    podLifeCycle.UserWorkloadDuration = 0;
                    podLifeCycle.Assert();
                    return podLifeCycle;
                }
                podLifeCycle.ReadyDuration = allocationStartPoint - readyStartPoint;
                podLifeCycle.AllocationDuration = allocationEndPoint - allocationStartPoint;
                if (podLifeCycle.ReadyDuration <= 0)
                {
                    podLifeCycle.HasErrors = true;
                    return podLifeCycle;
                }

                if (specializationEndPoint == -1
                    || specializationEndPoint == 0
                    || specializationStartPoint == -1
                    || specializationStartPoint == 0)
                {
                    podLifeCycle.SpecializationDuration = 0;
                    podLifeCycle.UserWorkloadDuration = 0;
                    podLifeCycle.Assert();
                    return podLifeCycle;
                }
                podLifeCycle.SpecializationDuration = specializationEndPoint - specializationStartPoint;
                podLifeCycle.UserWorkloadDuration = realDeleteTimePoint - specializationEndPoint;
                if (podLifeCycle.UserWorkloadDuration < 0)
                {
                    podLifeCycle.UserWorkloadDuration = 0;
                }
                podLifeCycle.Assert();
                return podLifeCycle;
            }
            catch (Exception e)
            {
                Console.WriteLine(line);
                Console.WriteLine(e.ToString());
            }
            return null;
        }

        private TraceLineFields? LineToTraceFields(string line)
        {
            try
            {
                var temp = line.Split(",");
                TraceLineFields? traceLineFields = null;
                if (temp.Length != 13 && temp.Length != 8)
                {
                    Console.WriteLine("could not parse line: {0}", line);
                    throw new ArgumentOutOfRangeException();
                }
                traceLineFields = ParseTraceLineNewFormat(temp);
                // traceLineFields.Cores = 1.0;
                return traceLineFields;
            }
            catch (Exception e)
            {
                Console.WriteLine("could not parse line: {0}", line);
                Console.WriteLine(e.ToString());
            }
            return null;
        }

        private TraceLineFields? ParseTraceLineNewFormat(string[] stringArray)
        {
            var traceLineFields = new TraceLineFields();
            traceLineFields.TraceLineType = stringArray[3].Trim() == "Allocate" ? TraceLineType.Allocation : TraceLineType.Deallocation;
            string format = "yyyy-MM-dd HH:mm:ss.ffffff";
            traceLineFields.RealTime = ParseDateTime(stringArray[0], format);
            traceLineFields.TraceRelativeTime = ParseTraceRelativeTime(stringArray[1].Trim());
            traceLineFields.PodUUID = stringArray[2].Trim();
            if (traceLineFields.TraceLineType == TraceLineType.Allocation)
            {
                traceLineFields.Runtime = ParseRuntime(stringArray[11]);
                traceLineFields.RuntimeVersion = ParseRuntimeVersion(stringArray[12]);
                traceLineFields.Pods = 1;
                traceLineFields.Cores = ParseCoreCount(stringArray[6].Trim());
                traceLineFields.AllocationLatency = ParseLatency(stringArray[5].Trim().Split(" ")[0]);
            }
            else
            {
                traceLineFields.Runtime = null;
                traceLineFields.RuntimeVersion = null;
                traceLineFields.Pods = 1;
                traceLineFields.Cores = 0.0;
                traceLineFields.AllocationLatency = ParseLatency(stringArray[5].Trim().Split(" ")[0]);
            }
            return traceLineFields;
        }

        public double ComputeTimeDiff(DateTime dateTime1, DateTime dateTime2)
        {
            long ticksDifference = dateTime2.Ticks - dateTime1.Ticks;
            double differenceInSeconds = ticksDifference / (double)TimeSpan.TicksPerSecond;
            return differenceInSeconds;
        }

        public double ParseTraceRelativeTime(string traceRelativeTime)
        {
            return double.Parse(traceRelativeTime);
        }

        private double ParseCoreCount(string coresCountStr)
        {
            double cores;
            if (coresCountStr.Contains('m'))
            {
                coresCountStr = coresCountStr.Replace("m", "");
                cores = double.Parse(coresCountStr) / 1000.0;
            }
            else
            {
                cores = double.Parse(coresCountStr);
            }
            return cores;
        }

        private DateTime ParseDateTime(string dateTimeStr, string format)
        {
            DateTime dateTime;
            bool succeeded = DateTime.TryParseExact(dateTimeStr, format, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out dateTime);
            if (!succeeded)
            {
                Console.WriteLine("Unexpected time format!!!");
                Environment.Exit(1);
            }
            return dateTime;
        }

        private string ParseRuntime(string runtimeStr)
        {
            var runtime = runtimeStr.Split("=")[1];
            return runtime;
        }

        private string ParseRuntimeVersion(string runtimeVersionStr)
        {
            var runtimeVersion = runtimeVersionStr.Split("=")[1].Split("\"")[0];
            runtimeVersion = runtimeVersion.Split("--cores")[0];
            return runtimeVersion;
        }

        private double ParseLatency(string latencyStr)
        {
            return double.Parse(latencyStr) / 1000.0;
        }

        public void Close()
        {
            reader.Close();
        }
    }
}

