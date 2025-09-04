using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace ServerlessPoolOptimizer
{

    public class AnalysisHelper
    {

        public static int ComputePoissonPoolSize(double lambda, double creationTime, double epsilon)
        {
            if (lambda <= 0 || creationTime <= 0 || epsilon <= 0 || epsilon >= 1)
            {
                Console.WriteLine("lambda = {0}, creationTime = {1}, epsilon = {2}", lambda, creationTime, epsilon);
                throw new ArgumentException("All parameters must be positive, and epsilon must be between 0 and 1.");
            }

            double mu = lambda * creationTime;
            int N = (int)Math.Ceiling(mu);

            while (true)
            {
                double cdf = ComputePoissonCDF(N, mu);
                if (cdf >= 1 - epsilon)
                    return N;

                N++;
            }
        }

        private static double ComputePoissonCDF(int N, double mu)
        {
            double sum = 0.0;
            double term = Math.Exp(-mu);

            for (int k = 0; k <= N; k++)
            {
                if (k > 0)
                    term *= mu / k;

                sum += term;
            }

            return sum;
        }


        public static void CreateSmoothedTraces(Experiment exp, Trace trace, double windowSize)
        {
            foreach (var (poolLabel, poolDistributions) in trace.PoolLabelToDistributions)
            {
                poolDistributions.SmoothedTrace = SmoothedTrace.CreateSmoothedTrace(poolDistributions.ArrivalTimeList, windowSize);
            }
        }

        public static void CreateSmoothedTraces(Experiment exp, double windowSize)
        {
            CreateSmoothedTraces(exp, exp.Trace, windowSize);
            if (exp.PredictedTraceFile != null)
            {
                ParseSmoothedTraces(exp);
            }
            else
            {
                CreateSmoothedTraces(exp, exp.TestTrace, windowSize);
            }
        }

        public static void CoreDemandAnalysis(Experiment exp,
                                                    Dictionary<double, IDistribution> PercentileToHostRoleSizeDistributionMap,
                                                    int samplesCount)
        {
            Console.WriteLine("Performing core demand analysis...");
            Trace trace = exp.Trace;
            if (exp.PoolOptimizationMethod == PoolOptimizationMethod.DROPS)
            {
                DropsCoreDemandAnalysis(exp,
                                        exp.HostRoleInitializationDemandDistribution,
                                        PercentileToHostRoleSizeDistributionMap,
                                        samplesCount);
            }
            else
            {
                GenericCoreDemandAnalysis(exp,
                                        trace.requestsList,
                                        exp.HostRoleInitializationDemandDistribution,
                                        PercentileToHostRoleSizeDistributionMap,
                                        samplesCount);
            }

        }

        public static void PodDemandAnalysis(Experiment exp, int samplesCount)
        {
            Trace trace = exp.Trace;
            Console.WriteLine("Container pools optimization");
            foreach (var (poolLabel, poolDistributions) in trace.PoolLabelToDistributions)
            {
                var allocationsArrivalTimeList = trace.PoolLabelToDistributions[poolLabel].ArrivalTimeList;
                var supplyDemandDistribution = poolDistributions.PodLifeCycleDistributions._supplyDelayDistribution;

                AnalysisHelper.PodDemandAnalysis(exp,
                                                allocationsArrivalTimeList,
                                                supplyDemandDistribution,
                                                poolDistributions.PercentileToPoolSizeDistributionMap,
                                                samplesCount);

                AnalysisHelper.GenerateSuccessRateMap(poolDistributions.PercentileToPoolSizeDistributionMap[1.0],
                                                                        poolDistributions.PoolSizeToSuccessRateMap);

            }
        }

        public static void HostRoleAnalysisPerPool(
                                            Experiment exp,
                                            Trace trace,
                                            IDistribution hostRoleCreationDelayDistribution,
                                            int samplesCount
                                            )

        {
            foreach (var poolLabel in trace.PoolLabelToTraceLines.Keys)
            {
                Console.WriteLine("VM Demand Analysis for {0} Pool", poolLabel);
                var poolDistributions = trace.PoolLabelToDistributions[poolLabel];
                var hostRolePoolSizeDistDict = poolDistributions.hostRolePoolSizeDistribution;
                HostRoleAnalysis(exp, trace,
                                    hostRoleCreationDelayDistribution,
                                    samplesCount,
                                    null,
                                    null,
                                    hostRolePoolSizeDistDict,
                                    null,
                                    [poolLabel]);
            }
        }

        public static void HostRoleAnalysis(Experiment exp,
                                            Trace trace,
                                            IDistribution hostRoleCreationDelayDistribution,
                                            int samplesCount,
                                            Dictionary<double, IDistribution>? hostRoleDemandRateDistribution,
                                            Dictionary<double, IDistribution>? hostRoleRecyclingRateDistribution,
                                            Dictionary<double, IDistribution>? hostRoleDemandCountDistribution,
                                            Dictionary<double, IDistribution>? hostRoleRecyclingCountDistribution,
                                            List<PoolLabel>? poolLabels
                                            )
        {
            double windowSize;
            for (int i = 0; i < samplesCount; i++)
            {
                if (exp.SamplingApproach == SamplingApproach.Average)
                {
                    windowSize = hostRoleCreationDelayDistribution.GetMean();
                }
                else if (exp.SamplingApproach == SamplingApproach.Random)
                {
                    windowSize = hostRoleCreationDelayDistribution.GetSample();
                }
                else
                {
                    throw new IndexOutOfRangeException();
                }

                GetHostRolesDemandRecyclingDistributions(exp, trace,
                                                            windowSize,
                                                            hostRoleDemandRateDistribution,
                                                            hostRoleRecyclingRateDistribution,
                                                            hostRoleDemandCountDistribution,
                                                            hostRoleRecyclingCountDistribution,
                                                            poolLabels,
                                                            1
                                                        );
                if ((i + 1) % 100 == 0)
                {
                    Console.WriteLine("Completed {0} Host Role Analysis Iterations", i + 1);
                }
            }
        }

        private static void ComputeSuccessRateFromDist(Dictionary<int, double> poolSizeToSuccessRateMap,
                                                IDistribution poolSizeDistribution,
                                                double totalLoad)
        {
            double cumFreq = 0;
            double successRate = 0.0;

            for (int i = 0; i < poolSizeDistribution.PairsCount(); i++)
            {
                var sizeFreqPair = poolSizeDistribution.GetValueFreqPairByIndex(i);
                var poolSize = sizeFreqPair.Key;
                var freq = sizeFreqPair.Value.Item1;
                cumFreq += freq;
                successRate = cumFreq;
                if (!poolSizeToSuccessRateMap.ContainsKey((int)poolSize))
                {
                    poolSizeToSuccessRateMap.Add((int)poolSize, successRate / totalLoad);
                }
            }
        }


        public static void GenerateSuccessRateMap(IDistribution poolSizeDistribution,
                                                            Dictionary<int, double> poolSizeToSuccessRateMap)
        {

            var count = poolSizeDistribution.PairsCount();
            double totalCount = 0;

            double totalLoad = 0.0;

            for (int i = 0; i < count; i++)
            {
                var sizeFreqPair = poolSizeDistribution.GetValueFreqPairByIndex(i);
                var size = sizeFreqPair.Key;
                var freq = sizeFreqPair.Value.Item1;
                totalCount += freq;
                totalLoad += size * freq;
            }

            ComputeSuccessRateFromDist(poolSizeToSuccessRateMap, poolSizeDistribution, totalCount);
        }

        public static void GetHostRolesDemandRecyclingDistributions(
                                                        Experiment exp,
                                                        Trace trace,
                                                        double windowSize,
                                                        Dictionary<double, IDistribution>? hostRoleDemandRateDistribution,
                                                        Dictionary<double, IDistribution>? hostRoleRecyclingRateDistribution,
                                                        Dictionary<double, IDistribution>? hostRoleDemandCountDistribution,
                                                        Dictionary<double, IDistribution>? hostRoleRecyclingCountDistribution,
                                                        List<PoolLabel>? poolLabels,
                                                        ulong frequency
                                                        )
        {

            AllocationRequest? request = null;
            SimEvent? nextEvent = null;
            SimulationTime clock = new SimulationTime();
            Simulator simulator = new Simulator(clock);


            SortedSet<SimEvent> podRecyclingEvents = new SortedSet<SimEvent>(new ComparerAllowDuplicate<SimEvent>());
            SortedSet<SimEvent> processedEvents = new SortedSet<SimEvent>(new ComparerAllowDuplicate<SimEvent>());
            Dictionary<double, int> coreToCreatedPodsCount = new Dictionary<double, int>();
            Dictionary<double, int> coreToRecycledPodsCount = new Dictionary<double, int>();

            IDictionary<PoolLabel, int> poolLabelToIndex = new Dictionary<PoolLabel, int>();
            foreach (var poolLabel in trace.PoolLabelToTraceLines.Keys)
            {
                if (poolLabels == null || poolLabels.Contains(poolLabel))
                    poolLabelToIndex.Add(poolLabel, 0);
            }

            double windowStartTime = 0.0;
            double windowEndTime = windowStartTime;

            int counter = 0;

            while (true)
            {
                //window expansion phase
                while (windowEndTime - windowStartTime < windowSize)
                {
                    request = trace.GetNextRequest(poolLabelToIndex);
                    nextEvent = podRecyclingEvents.Min;
                    if (request == null && nextEvent == null)
                    {
                        // stop - has seen all requests & pods in the trace
                        break;
                    }
                    PoolLabel poolLabel;

                    if (request != null && nextEvent != null)
                    {
                        if (request.ArrivalTimePoint < nextEvent.GetTriggerTimePoint())
                        {
                            nextEvent = null;

                        }
                        else
                        {
                            request = null;
                        }
                    }

                    if (request != null)
                    {
                        windowEndTime = request.ArrivalTimePoint;
                    }

                    if (windowEndTime - windowStartTime >= windowSize)
                    {
                        // stop expansion - do not process request
                        break;
                    }

                    if (request != null)
                    {
                        // increment pool index to get next request
                        poolLabel = TraceLineFields.ConvertToPoolLabel(request.AllocationPoolGroupLabel.Runtime, request.AllocationPoolGroupLabel.RuntimeVersion, request.Cores);
                        poolLabelToIndex[poolLabel]++;
                        if (request.RequestType == RequestType.Deallocation)
                        {
                            // ignore deallocation requests
                            continue;
                        }
                        counter++;
                        // create pod recycling event 
                        var podLifeCycle = trace.SamplePodLifeCycle(poolLabel,
                                                                    exp.SamplingApproach,
                                                                    exp.IgnorePodTransitionsExceptCreation);
                        var recycleTimePoint = request.ArrivalTimePoint + podLifeCycle.GetTransitionEndTimePoint(PodState.Recycled);
                        Pod pod = new Pod(clock, 0, podLifeCycle, request.AllocationPoolGroupLabel, request.Cores, 0);
                        nextEvent = simulator.CreateEvent(EventType.PodBecomesRecycled, 0.0, recycleTimePoint, null, null, pod);

                        // add request the list to move window start index 
                        nextEvent = simulator.CreateEvent(EventType.RequestArrive, 0.0, request.ArrivalTimePoint, request, null, null);
                        processedEvents.Add(nextEvent);
                        // increment counter for pod size
                        if (!coreToCreatedPodsCount.ContainsKey(request.Cores))
                        {
                            coreToCreatedPodsCount.Add(request.Cores, 0);
                        }
                        coreToCreatedPodsCount[request.Cores]++;
                    }
                    else
                    {
                        podRecyclingEvents.Remove(nextEvent);
                        processedEvents.Add(nextEvent);
                        Pod pod = nextEvent.GetPod();
                        if (!coreToRecycledPodsCount.ContainsKey(pod._podCores))
                        {
                            coreToRecycledPodsCount.Add(pod._podCores, 0);
                        }
                        coreToRecycledPodsCount[pod._podCores]++;
                    }
                }
                if (hostRoleDemandRateDistribution != null)
                {
                    foreach (var (percentile, distribution) in hostRoleDemandRateDistribution)
                    {
                        var val = Utilities.ComputeNeededHostRoles(coreToCreatedPodsCount,
                                                                exp.HostRoleCores,
                                                                exp.MaxPodsPerHostRole);
                        val = val / windowSize;
                        distribution.AddValueFrequency(val * percentile, frequency);
                    }
                }
                if (hostRoleRecyclingRateDistribution != null)
                {
                    foreach (var (percentile, distribution) in hostRoleRecyclingRateDistribution)
                    {
                        var val = Utilities.ComputeNeededHostRoles(coreToRecycledPodsCount,
                                                                exp.HostRoleCores,
                                                                exp.MaxPodsPerHostRole);
                        val = val / windowSize;
                        distribution.AddValueFrequency(val * percentile, frequency);
                    }
                }
                if (hostRoleDemandCountDistribution != null)
                {
                    foreach (var (percentile, distribution) in hostRoleDemandCountDistribution)
                    {
                        var val = Utilities.ComputeNeededHostRoles(coreToCreatedPodsCount,
                                                                exp.HostRoleCores,
                                                                exp.MaxPodsPerHostRole);
                        distribution.AddValueFrequency(val * percentile, frequency);
                    }
                }
                if (hostRoleRecyclingCountDistribution != null)
                {
                    foreach (var (percentile, distribution) in hostRoleRecyclingCountDistribution)
                    {
                        var val = Utilities.ComputeNeededHostRoles(coreToRecycledPodsCount,
                                                                exp.HostRoleCores,
                                                                exp.MaxPodsPerHostRole);
                        distribution.AddValueFrequency(val * percentile, frequency);
                    }
                }

                if (request == null && nextEvent == null)
                {
                    break;
                }

                while (windowStartTime <= windowEndTime && windowEndTime - windowStartTime >= windowSize)
                {
                    nextEvent = processedEvents.Min;
                    if (nextEvent == null || nextEvent.GetTriggerTimePoint() > windowEndTime)
                    {
                        windowStartTime = windowEndTime;
                        foreach (var (size, count) in coreToCreatedPodsCount)
                        {
                            coreToCreatedPodsCount[size] = 0;
                        }
                        foreach (var (size, count) in coreToRecycledPodsCount)
                        {
                            coreToRecycledPodsCount[size] = 0;
                        }
                        break;
                    }
                    processedEvents.Remove(nextEvent);
                    windowStartTime = nextEvent.GetTriggerTimePoint();
                    if (nextEvent.GetEventType() == EventType.RequestArrive)
                    {
                        request = nextEvent.GetRequest();
                        coreToCreatedPodsCount[request.Cores]--;
                        Debug.Assert(coreToCreatedPodsCount[request.Cores] >= 0);

                    }
                    else if (nextEvent.GetEventType() == EventType.PodBecomesRecycled)
                    {
                        Pod pod = nextEvent.GetPod();
                        coreToRecycledPodsCount[pod._podCores]--;
                        Debug.Assert(coreToRecycledPodsCount[pod._podCores] >= 0);
                    }
                }
            }
        }

        public static void DropsCoreDemandAnalysis(
                                Experiment exp,
                                IDistribution supplyDemandDistribution,
                                Dictionary<double, IDistribution>? percentileToPoolSizeDistributionMap,
                                int samples)
        {
            for (int i = 0; i < samples; i++)
            {
                exp.Trace.SampleRecyclingTrace(exp);
                DropsPerRequestCoreDemandAnalysis(exp,
                                                    exp.Trace.requestsList,
                                                    exp.Trace.deallocationList,
                                                    supplyDemandDistribution,
                                                    exp.SamplingApproach,
                                                    percentileToPoolSizeDistributionMap
                                                );
            }
        }

        public static void GenericCoreDemandAnalysis(
                                Experiment exp,
                                List<(PoolLabel, double)> allocationsArrivalTimeList,
                                IDistribution supplyDemandDistribution,
                                Dictionary<double, IDistribution>? percentileToPoolSizeDistributionMap,
                                int samples)
        {
            double windowSize;
            var supplyDemandFreqDistribution = supplyDemandDistribution;
            for (int i = 0; i < samples; i++)
            {
                if (exp.SamplingApproach == SamplingApproach.Average)
                {
                    windowSize = supplyDemandFreqDistribution.GetMean();
                }
                else if (exp.SamplingApproach == SamplingApproach.Random)
                {
                    windowSize = supplyDemandDistribution.GetSample();
                }
                else
                {
                    windowSize = supplyDemandFreqDistribution.GetTail(1.0);
                }

                GenericPerRequestCoreDemandAnalysis(exp,
                                                    allocationsArrivalTimeList,
                                                    [],
                                                    windowSize,
                                                    1,
                                                    percentileToPoolSizeDistributionMap
                                                    );

                if ((i + 1) % 50 == 0)
                    Console.WriteLine("{0} samples Completed!", i + 1);
            }
        }

        public static void PodDemandAnalysis(
                                Experiment exp,
                                List<double> allocationsArrivalTimeList,
                                IDistribution supplyDemandDistribution,
                                Dictionary<double, IDistribution>? percentileToPoolSizeDistributionMap,
                                int samples)
        {
            for (int i = 0; i < samples; i++)
            {
                AnalysisHelper.PerRequestPodDemandAnalysis(allocationsArrivalTimeList, supplyDemandDistribution,
                                                                                    exp.SamplingApproach,
                                                                                    percentileToPoolSizeDistributionMap
                                                                                    );
            }
        }

        public static void GenericPerRequestCoreDemandAnalysis(Experiment exp,
                                                        List<(PoolLabel, double)> requestsArrivalTime,
                                                        List<(PoolLabel, double)> coreRecyclingArrivalTime,
                                                        double windowSize,
                                                        double frequency,
                                                        Dictionary<double, IDistribution>? percentileToPoolSizeDistributionMap)
        {

            List<(RequestType, (PoolLabel, double))> eventsList = new List<(RequestType, (PoolLabel, double))>();

            int allocationIdx = 0;
            int recyclingIdx = 0;

            while (allocationIdx < requestsArrivalTime.Count || recyclingIdx < coreRecyclingArrivalTime.Count)
            {
                if (allocationIdx < requestsArrivalTime.Count && recyclingIdx < coreRecyclingArrivalTime.Count)
                {
                    var nextAllocationTS = requestsArrivalTime[allocationIdx].Item2;
                    var nextRecyclingTS = coreRecyclingArrivalTime[recyclingIdx].Item2;
                    if (nextAllocationTS <= nextRecyclingTS)
                    {
                        eventsList.Add((RequestType.Allocation, requestsArrivalTime[allocationIdx]));
                        allocationIdx++;
                    }
                    else
                    {
                        eventsList.Add((RequestType.Deallocation, coreRecyclingArrivalTime[recyclingIdx]));
                        recyclingIdx++;
                    }
                }

                else if (allocationIdx < requestsArrivalTime.Count)
                {
                    eventsList.Add((RequestType.Allocation, requestsArrivalTime[allocationIdx]));
                    allocationIdx++;
                }

                else if (recyclingIdx < coreRecyclingArrivalTime.Count)
                {
                    eventsList.Add((RequestType.Deallocation, coreRecyclingArrivalTime[recyclingIdx]));
                    recyclingIdx++;
                }
            }

            int startIdx = 0;
            int endIdx = 0;
            Dictionary<double, int> podsCountMap = new Dictionary<double, int>();

            while (endIdx < eventsList.Count)
            {
                var eventItem = eventsList[endIdx];
                double cores = eventItem.Item2.Item1.Cores;
                bool addToDist = false;
                if (!podsCountMap.ContainsKey(cores))
                {
                    podsCountMap.Add(cores, 0);
                }

                if (eventItem.Item1 == RequestType.Allocation)
                {
                    podsCountMap[cores]++;
                    addToDist = true;
                }
                else
                {
                    if (exp.HostRoleOptimizationMethod == HostRoleOptimizationMethod.DROPS)
                    {
                        podsCountMap[cores]--;
                    }
                }

                while (startIdx <= endIdx && eventsList[endIdx].Item2.Item2 - eventsList[startIdx].Item2.Item2 >= windowSize)
                {
                    eventItem = eventsList[startIdx];
                    cores = eventItem.Item2.Item1.Cores;
                    if (eventItem.Item1 == RequestType.Allocation)
                    {
                        podsCountMap[cores]--;
                    }
                    else
                    {
                        if (exp.HostRoleOptimizationMethod == HostRoleOptimizationMethod.DROPS)
                        {
                            podsCountMap[cores]++;
                        }
                    }
                    startIdx++;
                }

                if (percentileToPoolSizeDistributionMap != null && addToDist)
                {
                    double hostRoleCount = Math.Ceiling(Utilities.ComputeNeededHostRoles(podsCountMap, exp.HostRoleCores, exp.MaxPodsPerHostRole));
                    if (hostRoleCount > 0)
                    {
                        foreach (var (percentile, distribution) in percentileToPoolSizeDistributionMap)
                        {
                            distribution.AddValueFrequency(percentile * hostRoleCount, frequency);
                        }
                    }
                }
                endIdx++;
            }
        }

        public static void DropsPerRequestCoreDemandAnalysis(Experiment exp,
                                                List<(PoolLabel, double)> requestsArrivalTime,
                                                List<(PoolLabel, double)> coreRecyclingArrivalTime,
                                                IDistribution creationTimeDistribution,
                                                SamplingApproach samplingApproach,
                                                Dictionary<double, IDistribution>? percentileToPoolSizeDistributionMap)
        {

            List<(RequestType, (PoolLabel, double))> eventsList = new List<(RequestType, (PoolLabel, double))>();

            int allocationIdx = 0;
            int recyclingIdx = 0;
            while (allocationIdx < requestsArrivalTime.Count || recyclingIdx < coreRecyclingArrivalTime.Count)
            {
                if (allocationIdx < requestsArrivalTime.Count && recyclingIdx < coreRecyclingArrivalTime.Count)
                {
                    var nextAllocationTS = requestsArrivalTime[allocationIdx].Item2;
                    var nextRecyclingTS = coreRecyclingArrivalTime[recyclingIdx].Item2;
                    if (nextAllocationTS <= nextRecyclingTS)
                    {
                        eventsList.Add((RequestType.Allocation, requestsArrivalTime[allocationIdx]));
                        allocationIdx++;
                    }
                    else
                    {
                        eventsList.Add((RequestType.Deallocation, coreRecyclingArrivalTime[recyclingIdx]));
                        recyclingIdx++;
                    }
                }

                else if (allocationIdx < requestsArrivalTime.Count)
                {
                    eventsList.Add((RequestType.Allocation, requestsArrivalTime[allocationIdx]));
                    allocationIdx++;
                }

                else if (recyclingIdx < coreRecyclingArrivalTime.Count)
                {
                    eventsList.Add((RequestType.Deallocation, coreRecyclingArrivalTime[recyclingIdx]));
                    recyclingIdx++;
                }
            }

            int startIdx = 0;
            int endIdx = 0;
            Dictionary<double, int> podsCountMap = new Dictionary<double, int>();
            while (endIdx < eventsList.Count)
            {

                if (eventsList[endIdx].Item1 == RequestType.Deallocation)
                {
                    endIdx++;
                    continue;
                }
                var eventItem = eventsList[endIdx];
                double cores = eventItem.Item2.Item1.Cores;
                if (!podsCountMap.ContainsKey(cores))
                {
                    podsCountMap.Add(cores, 0);
                }


                foreach (var (size, count) in podsCountMap)
                {
                    podsCountMap[size] = 0;
                }

                var creationTimeSample = creationTimeDistribution.GetSample();
                switch (samplingApproach)
                {
                    case SamplingApproach.Average:
                        creationTimeSample = creationTimeDistribution.GetMean();
                        break;
                }

                startIdx = endIdx;

                while (startIdx >= 0 && eventsList[endIdx].Item2.Item2 - eventsList[startIdx].Item2.Item2 < creationTimeSample)
                {
                    eventItem = eventsList[startIdx];
                    cores = eventItem.Item2.Item1.Cores;
                    if (eventItem.Item1 == RequestType.Allocation)
                    {
                        podsCountMap[cores]++;
                    }
                    else
                    {
                        if (exp.HostRoleOptimizationMethod == HostRoleOptimizationMethod.DROPS)
                        {
                            podsCountMap[cores]--;
                        }
                    }
                    startIdx--;
                }

                double hostRoleCount = Math.Ceiling(Utilities.ComputeNeededHostRoles(podsCountMap, exp.HostRoleCores, exp.MaxPodsPerHostRole));
                if (hostRoleCount <= 0)
                {
                    hostRoleCount = 1;
                }
                if (hostRoleCount > 0)
                {
                    foreach (var (percentile, distribution) in percentileToPoolSizeDistributionMap)
                    {
                        distribution.AddValueFrequency(percentile * hostRoleCount, 1);
                    }
                }
                endIdx++;
                if (endIdx % 100000 == 0)
                {
                    Console.WriteLine("DROPS Per-core analysis: completed analysis for {0} events", endIdx);
                }
            }
        }

        public static void PerRequestPodDemandAnalysis(List<double> requestsArrivalTime,
                                                            IDistribution creationTimeDistribution,
                                                            SamplingApproach samplingApproach,
                                                            Dictionary<double, IDistribution>? percentileToPoolSizeDistributionMap
                                                        )
        {
            int endIdx = 0;
            int startIdx;
            int poolSize;
            var macCreationTimeSample = creationTimeDistribution.GetTail(1.0);
            while (endIdx < requestsArrivalTime.Count)
            {
                startIdx = endIdx - 1;
                poolSize = 1;
                while (startIdx >= 0 && requestsArrivalTime[endIdx] - requestsArrivalTime[startIdx] < macCreationTimeSample)
                {
                    var requestTS = requestsArrivalTime[startIdx];
                    var creationDuration = creationTimeDistribution.GetSample();
                    switch (samplingApproach)
                    {
                        case SamplingApproach.Average:
                            creationDuration = creationTimeDistribution.GetMean();
                            break;
                    }
                    var replenishmentTS = requestTS + creationDuration;
                    if (replenishmentTS > requestsArrivalTime[endIdx])
                    {
                        poolSize++;
                    }
                    startIdx--;
                }

                if (percentileToPoolSizeDistributionMap != null)
                {
                    foreach (var (percentile, distribution) in percentileToPoolSizeDistributionMap)
                    {
                        distribution.AddValue(percentile * poolSize);
                    }
                }
                endIdx++;
            }
        }

        public static void ParseSmoothedTraces(Experiment exp)
        {

            string line = "";
            try
            {

                foreach (var (poolLabel, poolDistributions) in exp.TestTrace.PoolLabelToDistributions)
                {
                    poolDistributions.SmoothedTrace = new SmoothedTrace
                    {
                        WindowSize = exp.PredictionWindowSize,
                        Trace = new List<(int, double)>()
                    };
                }

                StreamReader reader = new StreamReader(exp.PredictedTraceFile);
                line = reader.ReadLine();
                line = reader.ReadLine();
                while (line != null)
                {
                    string runtimeString = "";
                    string runtimeVersionString = "";
                    double cores;

                    var tmp = line.Split(",");
                    runtimeString = tmp[0].Split("_")[0].Split("=")[1];
                    runtimeVersionString = tmp[0].Split("_")[1].Split("=")[1];
                    var coreStr = (tmp[0].Split("_")[2]).Replace("m", "");
                    cores = Double.Parse(coreStr);
                    if (cores > 2)
                    {
                        cores = 0.25;
                    }

                    PoolLabel poolLabel = new PoolLabel(new AllocationLabel(runtimeString, runtimeVersionString), cores);
                    if (!exp.TestTrace.PoolLabelToDistributions.ContainsKey(poolLabel))
                    {
                        line = reader.ReadLine();
                        continue;
                    }
                    var poolDistributions = exp.TestTrace.PoolLabelToDistributions[poolLabel];
                    var trace = poolDistributions.SmoothedTrace.Trace;
                    var avgLoad = Double.Parse(tmp[2]) / exp.PredictionWindowSize;
                    trace.Add((0, avgLoad));
                    line = reader.ReadLine();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception while reading {0}", exp.PredictedTraceFile);
                Console.WriteLine(e.ToString());
                Console.WriteLine(line);
            }
        }

        public static void DumpFrequencyDistribution(IDistribution distribution, string filePath)
        {
            try
            {
                StreamWriter outputFile = new StreamWriter(filePath);
                string line;
                for (int i = 0; i < distribution.PairsCount(); i++)
                {
                    var keyValuePair = distribution.GetValueFreqPairByIndex(i);
                    line = String.Format("{0},{1},", keyValuePair.Key, keyValuePair.Value.Item1);
                    outputFile.WriteLine(line);
                }
                outputFile.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        internal static int FindBufferSizeToMatchPercentile(Dictionary<int, double> bufferSizeToSuccessRateMap, double targetPercentile)
        {
            List<int> SortedSizes = new List<int>(bufferSizeToSuccessRateMap.Keys);
            SortedSizes.Sort();
            // foreach (var (size, percentile) in bufferSizeToSuccessRateMap)
            foreach (var size in SortedSizes)
            {
                if (bufferSizeToSuccessRateMap[size] >= targetPercentile)
                {
                    return size;
                }
            }
            return bufferSizeToSuccessRateMap.Keys.ToList().Max();
        }
    }
}