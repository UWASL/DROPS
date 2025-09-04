using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public class Analyzer
    {
        public static void RunExperiments(List<Experiment> experiments)
        {
            RandomSource.Init();
            for (int i = 0; i < experiments.Count; i++)
            {
                var exp = experiments[i];
                exp.Id = i;
                RunOneExperiment(exp);
            }
        }

        public static void RunOneExperiment(Experiment exp)
        {
            string path;
            int simulationMaxRequests = int.MaxValue;
            // int simulationMaxRequests = 15000;
            Console.WriteLine("#################################");
            Console.WriteLine("Running Experiment[{0}]: {1}", exp.Id, exp.ExpName);
            var inputTargetPercentiles = new List<double>(exp.TargetPercentiles);
            if (!exp.TargetPercentiles.Contains(1.0))
            {
                exp.TargetPercentiles.Add(1.0);
            }

            exp.Trace = new Trace(TraceType.AllocationTrace, exp.AllocationTracePath, exp.PodLifeCycleTracePath);
            exp.Trace.Parse(exp);
            exp.TestTrace = new Trace(TraceType.AllocationTrace, exp.TestAllocationTracePath, exp.PodLifeCycleTracePath);
            exp.TestTrace.Parse(exp);

            // Console.WriteLine("###############################");
            // Console.WriteLine("Before Filtering");

            // Console.WriteLine("Training Trace:");
            // Console.WriteLine(exp.Trace.ToString());
            // Console.WriteLine("Testing Trace:");
            // Console.WriteLine(exp.TestTrace.ToString());

            var poolsToKeepArray = new List<PoolLabel>()
            {
                new PoolLabel(new AllocationLabel("dotnet-isolated", "8.0"), 1),
                new PoolLabel(new AllocationLabel("python", "3.11"), 1),
                new PoolLabel(new AllocationLabel("python", "3.10"), 1),
                new PoolLabel(new AllocationLabel("node", "20"), 1),
                new PoolLabel(new AllocationLabel("powershell", "7.4"), 1),

                new PoolLabel(new AllocationLabel("dotnet-isolated", "8.0"), 0.25),
                new PoolLabel(new AllocationLabel("dotnet-isolated", "8.0"), 2),

                new PoolLabel(new AllocationLabel("python", "3.11"), 2),
                new PoolLabel(new AllocationLabel("python", "3.11"), 0.25),

                new PoolLabel(new AllocationLabel("python", "3.10"), 2),

                new PoolLabel(new AllocationLabel("node", "20"), 2),

                new PoolLabel(new AllocationLabel("dotnet-isolated", "9.0"), 1),

                new PoolLabel(new AllocationLabel("java", "11"), 1),
                new PoolLabel(new AllocationLabel("java", "17"), 1),
            };

            exp.Trace.RemoveAllPoolsExcept(poolsToKeepArray);
            exp.TestTrace.RemoveAllPoolsExcept(poolsToKeepArray);

            Trace.RemoveUncommonPools(exp);

            // Console.WriteLine("###############################");
            // Console.WriteLine("After Filtering");
            // Console.WriteLine("Training Trace:");
            // Console.WriteLine(exp.Trace.ToString());
            // Console.WriteLine("Testing Trace:");
            // Console.WriteLine(exp.TestTrace.ToString());


            exp.InitResultsObject(new List<PoolLabel>(exp.Trace.PoolLabelToDistributions.Keys));

            // create the smoothed trace
            AnalysisHelper.CreateSmoothedTraces(exp, exp.PredictionWindowSize);

            // perform pod demand analysis
            if (exp.StaticPoolSizesMap == null)
            {
                AnalysisHelper.PodDemandAnalysis(exp, exp.PoolDemandAnalysisSamplesCount);
            }

            if (exp.OptimizerAggressivePodCreation)
            {
                AnalysisHelper.HostRoleAnalysisPerPool(exp, exp.Trace,
                                                        exp.HostRoleInitializationDemandDistribution,
                                                        1);
            }

            var hostRoleDemandCountDistDict = new Dictionary<double, IDistribution>
            {
                { 1.0, new DistributionEmpiricalFrequencyArray() }
            };

            AnalysisHelper.CoreDemandAnalysis(exp, hostRoleDemandCountDistDict, exp.HostRoleDemandAnalysisSamplesCount);

            var successRateMap = new Dictionary<int, double>();
            AnalysisHelper.GenerateSuccessRateMap(hostRoleDemandCountDistDict[1.0], successRateMap);

            foreach (var targetPercentile in inputTargetPercentiles)
            {
                int hostRolesInitialCount = 0;
                switch (exp.HostRoleOptimizationMethod)
                {
                    case HostRoleOptimizationMethod.DROPS:
                    case HostRoleOptimizationMethod.Production:
                        hostRolesInitialCount = AnalysisHelper.FindBufferSizeToMatchPercentile(successRateMap, targetPercentile);
                        break;
                    case HostRoleOptimizationMethod.PredictiveReactive:
                        hostRolesInitialCount = (int)Math.Ceiling(hostRoleDemandCountDistDict[1.0].GetTail(1.0));
                        break;
                }
                RunOnePercentile(exp, targetPercentile, simulationMaxRequests, hostRolesInitialCount);
            }

            exp.TraceSummaryStr = exp.Trace.GetTraceSummaryText(exp, null);
            exp.TestTraceSummaryStr = exp.TestTrace.GetTraceSummaryText(exp, null);
            exp.Trace = null;
            exp.TestTrace = null;
        }

        public static void RunOnePercentile(Experiment exp,
                                            double targetPercentile,
                                            int simulationMaxRequests,
                                            int hostRolesCount
                                            )
        {

            PoolGroupParameters poolGroupParameters = new PoolGroupParameters(exp.Trace.PoolGroupParametersList[0]);
            PoolGroupParameters testPoolGroupParameters = new PoolGroupParameters(exp.TestTrace.PoolGroupParametersList[0]);

            Console.WriteLine("**********************************");
            var percentileResults = exp.Results.PercentileToResultsMap[targetPercentile];

            Console.WriteLine("Simulation Starts - Percentile = {0}",
                                                        targetPercentile);

            percentileResults.Reset();
            string traceReplayStatsFilePath = exp.ResultPath + Experiment.GetTraceReplayStatsFileName(exp.Id, exp.ExpName, targetPercentile);

            percentileResults.HostRolesPoolSize = hostRolesCount;
            SetMinPoolGroupHostRolesCount(poolGroupParameters, testPoolGroupParameters, hostRolesCount, targetPercentile);

            InitPoolsParameters(exp, poolGroupParameters, testPoolGroupParameters, targetPercentile, hostRolesCount);

            var simTime = new SimulationTime();
            var simulator = new Simulator(simTime);
            var openLoopLoader = new OpenLoopLoad(simulator, null, null, exp, exp.TestTrace, simTime);
            var serverlessService = new ServerlessService(simTime, simulator, exp, targetPercentile, hostRolesCount,
                                                            exp.HostRoleInitializationDemandDistribution,
                                                            testPoolGroupParameters,
                                                            traceReplayStatsFilePath);
            var poolOptimizer = new PoolOptimizer(simTime, serverlessService, simulator, exp);
            var mySystem = new ServerlessSystem(openLoopLoader, serverlessService, poolOptimizer, simTime, simulator, exp);
            mySystem.RunSimulation(int.MaxValue, simulationMaxRequests, true);
            mySystem.UnSubscribeHandlers();

            string latencyFile = exp.ResultPath + Experiment.GetRequestLatencyFileName(exp.Id, exp.ExpName, targetPercentile);
            AnalysisHelper.DumpFrequencyDistribution(percentileResults.RequestLatencyDistribution, latencyFile);
        }

        public static void InitPoolsParametersFromInputSizes(
                                                    Experiment exp,
                                                    PoolGroupParameters poolGroupParameters,
                                                    PoolGroupParameters testPoolGroupParameters,
                                                    double targetPercentile,
                                                    int hostRolesInitialCount)
        {
            Console.WriteLine("Setting container pools sizes");
            Trace trace = exp.Trace;
            Trace dstTrace = exp.TestTrace;
            double totalCores = 0;
            var inputPoolSizesMap = exp.StaticPoolSizesMap;
            List<PoolLabel> poolsToRemove = new List<PoolLabel>();
            var dstTracePoolGroupParameters = testPoolGroupParameters;
            foreach (var (allocationLabel, poolsParameters) in poolGroupParameters.RuntimeToPoolParameters)
            {
                var runtimeToPoolsParametersMap = poolGroupParameters.RuntimeToPoolParameters;
                var dstRuntimeToPoolsParametersMap = dstTracePoolGroupParameters.RuntimeToPoolParameters;
                foreach (double core in Parameter.PossibleCoreAllocations)
                {
                    var poolLabel = TraceLineFields.ConvertToPoolLabel(allocationLabel.Runtime, allocationLabel.RuntimeVersion, core);
                    if (!inputPoolSizesMap.ContainsKey(poolLabel))
                    {
                        if (trace.PoolLabelToDistributions.ContainsKey(poolLabel))
                        {
                            poolsToRemove.Add(poolLabel);
                        }
                        continue;
                    }
                    int neededPodsCount = inputPoolSizesMap[poolLabel];
                    totalCores += neededPodsCount * core;
                    PoolEmpiricalDistributions? poolDistributions = null;
                    if (!trace.PoolLabelToDistributions.ContainsKey(poolLabel))
                    {
                        var cores = runtimeToPoolsParametersMap[allocationLabel].Keys.ToList();
                        PoolLabel tmpPoolLabel = new PoolLabel(poolLabel.AllocationLabel, cores[0]);
                        trace.PoolLabelToDistributions.Add(poolLabel, trace.PoolLabelToDistributions[tmpPoolLabel]);
                    }

                    poolDistributions = trace.PoolLabelToDistributions[poolLabel];

                    runtimeToPoolsParametersMap[allocationLabel].Remove(poolLabel.Cores);
                    runtimeToPoolsParametersMap[allocationLabel].Add(poolLabel.Cores,
                        new PoolParameters(poolGroupParameters.PoolGroupId, allocationLabel, core,
                                            poolDistributions.PodLifeCycleDistributions,
                                            neededPodsCount, (int)(neededPodsCount * 1.5), 0, 0, 0, 0, null));


                    dstRuntimeToPoolsParametersMap[allocationLabel].Remove(poolLabel.Cores);
                    dstRuntimeToPoolsParametersMap[allocationLabel].Add(poolLabel.Cores,
                    new PoolParameters(poolGroupParameters.PoolGroupId, allocationLabel, core,
                                        dstTrace.PoolLabelToDistributions[poolLabel].PodLifeCycleDistributions,
                                        neededPodsCount, (int)(neededPodsCount * 1.5), 0, 0, 0, 0, null));


                    // only needed for writing results
                    var percentileResults = exp.Results.PercentileToResultsMap[targetPercentile];
                    percentileResults.PoolSizeMap[poolLabel] = neededPodsCount;
                }
            }

            if (poolsToRemove.Count != 0)
            {
                trace.RemovePools(poolsToRemove);
            }
        }

        public static void InitPoolsParameters(
                                    Experiment exp,
                                    PoolGroupParameters poolGroupParameters,
                                    PoolGroupParameters testPoolGroupParameters,
                                    double targetPercentile,
                                    int hostRolesInitialCount
                                    )
        {

            Trace trace = exp.Trace;
            Trace dstTrace = exp.TestTrace;
            if (exp.StaticPoolSizesMap != null)
            {
                InitPoolsParametersFromInputSizes(exp, poolGroupParameters, testPoolGroupParameters, targetPercentile, hostRolesInitialCount);
                return;
            }

            Console.WriteLine("Setting Pods Pools Sizes");
            double totalCores = 0;
            double totalHostRoles = 0.0;
            int poolsCount = 0;
            double totalCoresInSystem = hostRolesInitialCount * exp.HostRoleCores;

            var dstTracePoolGroupParameters = testPoolGroupParameters;

            var adjustedPercentile = targetPercentile;
            if (adjustedPercentile != 1.0)
            {
                adjustedPercentile = 1 - 0.5959 * (1 - adjustedPercentile);
            }

            foreach (var (allocationLabel, poolsParameters) in poolGroupParameters.RuntimeToPoolParameters)
            {
                var runtimeToPoolsParametersMap = poolGroupParameters.RuntimeToPoolParameters;
                var dstRuntimeToPoolsParametersMap = dstTracePoolGroupParameters.RuntimeToPoolParameters;

                foreach (double core in Parameter.PossibleCoreAllocations)
                {
                    var poolLabel = TraceLineFields.ConvertToPoolLabel(allocationLabel.Runtime, allocationLabel.RuntimeVersion, core);
                    if (!trace.PoolLabelToDistributions.ContainsKey(poolLabel))
                    {
                        continue;
                    }
                    if (!dstTrace.PoolLabelToDistributions.ContainsKey(poolLabel))
                    {
                        continue;
                    }
                    poolsCount++;
                    var poolDistributions = trace.PoolLabelToDistributions[poolLabel];
                    var dstPoolDistributions = dstTrace.PoolLabelToDistributions[poolLabel];
                    int neededPodsCount = 0;
                    int predictionReferencePoolSize = 0;
                    double predictionReferenceLoad = 0.0;


                    switch (exp.PoolOptimizationMethod)
                    {
                        case PoolOptimizationMethod.PredictionPoissonLoad:
                        case PoolOptimizationMethod.PredictionConstantLoad:
                        case PoolOptimizationMethod.PredictiveReactive:
                            neededPodsCount = (int)poolDistributions.PercentileToPoolSizeDistributionMap[targetPercentile].GetTail(1.0);
                            break;

                        case PoolOptimizationMethod.DROPS:
                        default:
                            var poolSizeToSuccessRateMap = trace.PoolLabelToDistributions[poolLabel].PoolSizeToSuccessRateMap;
                            neededPodsCount = AnalysisHelper.FindBufferSizeToMatchPercentile(poolSizeToSuccessRateMap, adjustedPercentile);
                            break;
                    }

                    var testPoolDistributions = dstTrace.PoolLabelToDistributions[poolLabel];

                    totalCores += neededPodsCount * core;
                    // var referencePredictionLoad = poolDistributions.SmoothedTrace.Trace[poolDistributions.SmoothedTrace.Trace.Count - 1].Item2;

                    runtimeToPoolsParametersMap[allocationLabel].Remove(poolLabel.Cores);
                    runtimeToPoolsParametersMap[allocationLabel].Add(poolLabel.Cores,
                        new PoolParameters(poolGroupParameters.PoolGroupId, allocationLabel, core,
                                            trace.PoolLabelToDistributions[poolLabel].PodLifeCycleDistributions,
                                            neededPodsCount, (int)(neededPodsCount * 1.5), 0, 0,
                                            predictionReferencePoolSize, predictionReferenceLoad, testPoolDistributions.SmoothedTrace));

                    dstRuntimeToPoolsParametersMap[allocationLabel].Remove(poolLabel.Cores);
                    dstRuntimeToPoolsParametersMap[allocationLabel].Add(poolLabel.Cores,
                        new PoolParameters(poolGroupParameters.PoolGroupId, allocationLabel, core,
                                            dstTrace.PoolLabelToDistributions[poolLabel].PodLifeCycleDistributions,
                                            neededPodsCount, (int)(neededPodsCount * 1.5), 0, 0,
                                            predictionReferencePoolSize, predictionReferenceLoad, testPoolDistributions.SmoothedTrace));

                    // only needed for writing results
                    var percentileResults = exp.Results.PercentileToResultsMap[targetPercentile];
                    percentileResults.PoolSizeMap[poolLabel] = neededPodsCount;
                }

            }
            if (exp.PoolOptimizationMethod != PoolOptimizationMethod.DROPS
                || exp.OptimizerAggressivePodCreation != true)
            {
                return;
            }

            var extraCores = totalCoresInSystem - totalCores;
            var poolExtraCores = 0.0;
            double poolExtraRatio = 0.0;


            totalCores = 0;
            var aggressivePercentile = 1.0;
            int targetPercentilePoolSize;
            // var aggressivePercentile = targetPercentile;
            foreach (var (allocationLabel, poolsParameters) in poolGroupParameters.RuntimeToPoolParameters)
            {
                foreach (var (cores, poolParameters) in poolsParameters)
                {
                    var poolLabel = new PoolLabel(poolParameters.AllocationLabel, cores);
                    var poolSizeToSuccessRateMap = trace.PoolLabelToDistributions[poolLabel].PoolSizeToSuccessRateMap;
                    var podsCount = AnalysisHelper.FindBufferSizeToMatchPercentile(poolSizeToSuccessRateMap, aggressivePercentile);
                    targetPercentilePoolSize = AnalysisHelper.FindBufferSizeToMatchPercentile(poolSizeToSuccessRateMap, adjustedPercentile);
                    var newPods = Math.Max(0, podsCount - targetPercentilePoolSize);
                    var poolCores = newPods * cores;
                    totalCores += poolCores;
                }
            }


            Console.WriteLine("The platform has {0} extra cores", extraCores);
            exp.MaxExtraCores = extraCores;
            
            if (Parameter.AggressiveOptimizationDistributeCoresEvenly)
            {
                extraCores = totalCoresInSystem - totalCores;
                poolExtraCores = extraCores / poolsCount;
                poolExtraRatio = 1.0 / poolsCount;
            }

            targetPercentilePoolSize = -1;
            foreach (var (allocationLabel, poolsParameters) in poolGroupParameters.RuntimeToPoolParameters)
            {
                var dstRuntimeToPoolsParametersMap = dstTracePoolGroupParameters.RuntimeToPoolParameters;
                foreach (var (cores, poolParameters) in poolsParameters)
                {
                    var poolLabel = new PoolLabel(poolParameters.AllocationLabel, cores);
                    if (Parameter.AggressiveOptimizationDistributeCoresEvenly)
                    {
                        poolParameters.MaxExtraPods = (int)(poolExtraCores / cores);
                        poolParameters.ExtraCoresRatio = poolExtraRatio;
                        dstRuntimeToPoolsParametersMap[allocationLabel][cores].MaxExtraPods = (int)(poolExtraCores / cores);
                        dstRuntimeToPoolsParametersMap[allocationLabel][cores].ExtraCoresRatio = poolExtraRatio;
                    }
                    else if (extraCores < totalCores)
                    {
                        var poolSizeToSuccessRateMap = trace.PoolLabelToDistributions[poolLabel].PoolSizeToSuccessRateMap;
                        var podsCount = AnalysisHelper.FindBufferSizeToMatchPercentile(poolSizeToSuccessRateMap, aggressivePercentile);
                        targetPercentilePoolSize = AnalysisHelper.FindBufferSizeToMatchPercentile(poolSizeToSuccessRateMap, adjustedPercentile);
                        var newPods = Math.Max(0, podsCount - targetPercentilePoolSize);
                        if (newPods == 0)
                        {
                            continue;
                        }
                        // var podsCount = poolParameters._minPodsCount;
                        var poolCores = newPods * cores;
                        poolExtraRatio = poolCores / totalCores;
                        poolExtraCores = poolExtraRatio * extraCores;
                        poolParameters.MaxExtraPods = (int)(poolExtraCores / cores);
                        poolParameters.ExtraCoresRatio = poolExtraRatio;
                        dstRuntimeToPoolsParametersMap[allocationLabel][cores].MaxExtraPods = (int)(poolExtraCores / cores);
                        dstRuntimeToPoolsParametersMap[allocationLabel][cores].ExtraCoresRatio = poolExtraRatio;
                    }
                    else
                    {
                        var poolSizeToSuccessRateMap = trace.PoolLabelToDistributions[poolLabel].PoolSizeToSuccessRateMap;
                        var podsCount = AnalysisHelper.FindBufferSizeToMatchPercentile(poolSizeToSuccessRateMap, aggressivePercentile);
                        targetPercentilePoolSize = AnalysisHelper.FindBufferSizeToMatchPercentile(poolSizeToSuccessRateMap, adjustedPercentile);
                        var newPods = Math.Max(0, podsCount - targetPercentilePoolSize);
                        if (newPods == 0)
                        {
                            continue;
                        }
                        // var podsCount = poolParameters._minPodsCount;
                        var poolCores = newPods * cores;
                        poolExtraRatio = poolCores / extraCores;
                        poolExtraCores = poolCores;
                        poolParameters.MaxExtraPods = newPods;
                        poolParameters.ExtraCoresRatio = poolExtraRatio;
                        dstRuntimeToPoolsParametersMap[allocationLabel][cores].MaxExtraPods = newPods;
                        dstRuntimeToPoolsParametersMap[allocationLabel][cores].ExtraCoresRatio = poolExtraRatio;
                    }
                    Console.WriteLine("Pool = {0}, pool size = {1}, extra cores (%) = {2}, extra pods = {3}",
                                    poolLabel, targetPercentilePoolSize, poolExtraRatio, (int)(poolExtraCores / cores));
                }
                // var poolCores = poolParameters._minPodsCount * cores;
            }
        }

        public static void SetMinPoolGroupHostRolesCount(PoolGroupParameters poolGroupParameters,
                                                        PoolGroupParameters testPoolGroupParameters,
                                                        int hostRolesCount,
                                                        double targetPercentile)
        {
            poolGroupParameters.MinAssignedHostRolesCount = hostRolesCount;
            poolGroupParameters.MaxAssignedHostRolesCount = (int)(1.5 * hostRolesCount);
            poolGroupParameters.MinIdleCoresCount = hostRolesCount;

            testPoolGroupParameters.MinAssignedHostRolesCount = hostRolesCount;
            testPoolGroupParameters.MaxAssignedHostRolesCount = (int)(1.5 * hostRolesCount);
            testPoolGroupParameters.MinIdleCoresCount = hostRolesCount;
        }

    }
}