
namespace ServerlessPoolOptimizer
{

    public enum PoolOptimizationMethod
    {
        Production,
        DROPS,
        Reactive,
        PredictiveReactive,
        PredictionConstantLoad,
        PredictionPoissonLoad,
        PredictionConcentratedLoad,
    }

    public enum HostRoleOptimizationMethod
    {
        Production,
        DROPS,
        PredictiveReactive,
    }

    public class PercentileResults
    {
        public Dictionary<PoolLabel, PoolStatistics> PoolLabelToPoolStatsMap;
        public DistributionEmpiricalFrequencyArray HostRoleUtilizationStats;
        public DistributionEmpiricalFrequencyArray PodCoreUtilizationStats;
        public DistributionEmpiricalFrequencyArray HostRolesCountDistribution;
        public DistributionEmpiricalFrequencyArray NonFullHostRolesCountDistribution;
        public DistributionEmpiricalFrequencyArray HostRoleDemandDistribution;
        public DistributionEmpiricalFrequencyArray RequestLatencyDistribution;
        public List<(double, int)> HostRoleAllocationTrace;
        public Dictionary<PoolLabel, int> PoolSizeMap;
        public int HostRolesPoolSize;
        public double TotalCoreHour, PodsTotalCoreHour, PoolTotalCoreHour;
        internal double ExpectedFailureRate;
        internal double MeasuredFailureRate;
        internal double TotalFailedRequests;
        internal double TotalRequests;
        internal double percentile;
        internal List<HostRole> HostRolesList;

        public PercentileResults(List<PoolLabel> poolLabelsList)
        {
            PoolLabelToPoolStatsMap = new Dictionary<PoolLabel, PoolStatistics>();
            PoolSizeMap = new Dictionary<PoolLabel, int>();
            foreach (var poolLabel in poolLabelsList)
            {
                PoolLabelToPoolStatsMap.Add(poolLabel, new PoolStatistics());
                PoolSizeMap.Add(poolLabel, 0);
            }
            HostRoleUtilizationStats = new DistributionEmpiricalFrequencyArray();
            PodCoreUtilizationStats = new DistributionEmpiricalFrequencyArray();
            HostRolesCountDistribution = new DistributionEmpiricalFrequencyArray();
            NonFullHostRolesCountDistribution = new DistributionEmpiricalFrequencyArray();
            HostRoleDemandDistribution = new DistributionEmpiricalFrequencyArray();
            RequestLatencyDistribution = new DistributionEmpiricalFrequencyArray();
            HostRoleAllocationTrace = new List<(double, int)>();
        }

        public void Reset()
        {
            var poolLabelsList = new List<PoolLabel>(PoolLabelToPoolStatsMap.Keys);
            PoolLabelToPoolStatsMap.Clear();
            PoolSizeMap.Clear();
            foreach (var poolLabel in poolLabelsList)
            {
                PoolLabelToPoolStatsMap.Add(poolLabel, new PoolStatistics());
                PoolSizeMap.Add(poolLabel, 0);
            }
            HostRoleUtilizationStats.Clear();
            PodCoreUtilizationStats.Clear();
            HostRolesCountDistribution.Clear();
            NonFullHostRolesCountDistribution.Clear();
            RequestLatencyDistribution.Clear();
            HostRoleAllocationTrace.Clear();
        }

        public double GetPoolFailureRate(PoolLabel poolLabel)
        {
            var poolStats = PoolLabelToPoolStatsMap[poolLabel];
            double allocationFailurePercentage = (double)poolStats._totalFailedRequestsCount / poolStats._totalRequestsCount * 100.0;
            return Math.Round(allocationFailurePercentage, 2);
        }

        public string SummaryStr(double maxCOGS)
        {
            string resultsStr = "";

            resultsStr += String.Format("{0},{1},{2},{3},{4},{5}",
                                            TotalCoreHour,
                                            PodsTotalCoreHour,
                                            PoolTotalCoreHour,
                                            TotalRequests,
                                            TotalFailedRequests,
                                            Math.Round(MeasuredFailureRate, 3)
                                        );
            return resultsStr;
        }

        public string GetPoolsResultsStr(string expName = "")
        {
            double totalRequests = 0;
            double failedRequests = 0;
            string resultsStr = "";

            foreach (var (poolLabel, poolStats) in PoolLabelToPoolStatsMap)
            {
                if (poolStats.PoolUtilizationDistribution.Count() == 0)
                {
                    continue;
                }
                totalRequests += poolStats._totalRequestsCount;
                failedRequests += poolStats._totalFailedRequestsCount;
                double allocationFailurePercentage = (double)poolStats._totalFailedRequestsCount / poolStats._totalRequestsCount * 100.0;
                allocationFailurePercentage = Math.Round(allocationFailurePercentage, 3);
                resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}\n",
                                            Utilities.PercentileToString(percentile),
                                            expName,
                                            poolLabel,
                                            poolStats._totalRequestsCount,
                                            poolStats._totalFailedRequestsCount,
                                            allocationFailurePercentage,
                                            poolStats.PoolUtilizationDistribution.GetTail(0.0),
                                            Math.Round(poolStats.PoolUtilizationDistribution.GetMean(), 2),
                                            poolStats.PoolUtilizationDistribution.GetTail(0.50),
                                            poolStats.PoolUtilizationDistribution.GetTail(0.90),
                                            poolStats.PoolUtilizationDistribution.GetTail(0.95),
                                            poolStats.PoolUtilizationDistribution.GetTail(0.99),
                                            poolStats.PoolUtilizationDistribution.GetTail(0.999),
                                            poolStats.PoolUtilizationDistribution.GetTail(1.0)
                                            );
            }
            MeasuredFailureRate = failedRequests / totalRequests * 100.0;
            TotalFailedRequests = failedRequests;
            TotalRequests = totalRequests;
            return resultsStr;
        }

        public string GetHostRolesResultsStr(string expName = "")
        {
            string resultsStr = "";
            if (PodCoreUtilizationStats.Count() == 0)
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{4},{4},{4},{4},{4},{4},{4}\n",
                                                Utilities.PercentileToString(percentile),
                                                expName,
                                                "Pod-Core Utilization",
                                                "N/A",
                                                "N/A"
                                            );
            }
            else
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}\n",
                                                Utilities.PercentileToString(percentile),
                                                expName,
                                                "Pod-Core Utilization",
                                                "N/A",
                                                PodCoreUtilizationStats.GetTail(0.0),
                                                Math.Round(PodCoreUtilizationStats.GetMean(), 2),
                                                PodCoreUtilizationStats.GetTail(0.50),
                                                PodCoreUtilizationStats.GetTail(0.90),
                                                PodCoreUtilizationStats.GetTail(0.95),
                                                PodCoreUtilizationStats.GetTail(0.99),
                                                PodCoreUtilizationStats.GetTail(0.999),
                                                PodCoreUtilizationStats.GetTail(1.0)
                                            );
            }

            if (HostRoleUtilizationStats.Count() == 0)
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{4},{4},{4},{4},{4},{4},{4}\n",
                                                Utilities.PercentileToString(percentile),
                                                expName,
                                                "Core Utilization",
                                                "N/A",
                                                "N/A"
                                            );
            }
            else
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}\n",
                                                            Utilities.PercentileToString(percentile),
                                                            expName,
                                                            "Core Utilization",
                                                            "N/A",
                                                            HostRoleUtilizationStats.GetTail(0.0),
                                                            Math.Round(HostRoleUtilizationStats.GetMean(), 2),
                                                            HostRoleUtilizationStats.GetTail(0.50),
                                                            HostRoleUtilizationStats.GetTail(0.90),
                                                            HostRoleUtilizationStats.GetTail(0.95),
                                                            HostRoleUtilizationStats.GetTail(0.99),
                                                            HostRoleUtilizationStats.GetTail(0.999),
                                                            HostRoleUtilizationStats.GetTail(1.0)
                                                        );
            }

            if (HostRolesCountDistribution.Count() == 0)
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{4},{4},{4},{4},{4},{4},{4}\n",
                                                                Utilities.PercentileToString(percentile),
                                                                expName,
                                                                "Host Roles Count",
                                                                "N/A",
                                                                "N/A"
                                                            );
            }
            else
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}\n",
                                                Utilities.PercentileToString(percentile),
                                                expName,
                                                "Host Roles Count",
                                                HostRolesPoolSize,
                                                HostRolesCountDistribution.GetTail(0.0),
                                                Math.Round(HostRolesCountDistribution.GetMean(), 2),
                                                HostRolesCountDistribution.GetTail(0.50),
                                                HostRolesCountDistribution.GetTail(0.90),
                                                HostRolesCountDistribution.GetTail(0.95),
                                                HostRolesCountDistribution.GetTail(0.99),
                                                HostRolesCountDistribution.GetTail(0.999),
                                                HostRolesCountDistribution.GetTail(1.0)
                                            );
            }

            if (NonFullHostRolesCountDistribution.Count() == 0)
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{4},{4},{4},{4},{4},{4},{4}\n",
                                                                                Utilities.PercentileToString(percentile),
                                                                                expName,
                                                                                "Idle Host Roles Count",
                                                                                "N/A",
                                                                                "N/A"
                                                                            );
            }
            else
            {
                resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}\n",
                                                Utilities.PercentileToString(percentile),
                                                expName,
                                                "Idle Host Roles Count",
                                                "N/A",
                                                NonFullHostRolesCountDistribution.GetTail(0.0),
                                                Math.Round(NonFullHostRolesCountDistribution.GetMean(), 2),
                                                NonFullHostRolesCountDistribution.GetTail(0.50),
                                                NonFullHostRolesCountDistribution.GetTail(0.90),
                                                NonFullHostRolesCountDistribution.GetTail(0.95),
                                                NonFullHostRolesCountDistribution.GetTail(0.99),
                                                NonFullHostRolesCountDistribution.GetTail(0.999),
                                                NonFullHostRolesCountDistribution.GetTail(1.0)
                                            );
            }

            return resultsStr;
        }
    }

    public class ExperimentResults
    {
        public Dictionary<double, PercentileResults> PercentileToResultsMap;
        public ExperimentResults(List<PoolLabel> poolLabelsList, List<double> targetPercentilesList)
        {
            PercentileToResultsMap = new Dictionary<double, PercentileResults>();
            foreach (var percentile in targetPercentilesList)
            {
                PercentileToResultsMap.Add(percentile, new PercentileResults(poolLabelsList));
            }
        }

        public string GetPoolsResultsHeader()
        {
            string resultsStr = "";
            resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}\n",
                                                "Percentile",
                                                "Exp",
                                                "Label",
                                                "Requests Count",
                                                "Failed Requests Count",
                                                "Failed Requests (%)",
                                                "Min",
                                                "Avg",
                                                "P50",
                                                "P90",
                                                "P95",
                                                "P99",
                                                "P999",
                                                "Max"
                                            );
            return resultsStr;
        }

        public string GetHostRolesResultsHeader()
        {
            string resultsStr = "";
            resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}\n",
                                                "Percentile",
                                                "Exp",
                                                "Label",
                                                "Pool Size",
                                                "Min",
                                                "Avg",
                                                "P50",
                                                "P90",
                                                "P95",
                                                "P99",
                                                "P999",
                                                "Max"
                                            );
            return resultsStr;
        }

        public override string ToString()
        {
            string resultsStr = "";

            resultsStr += GetPoolsResultsHeader();

            foreach (var percentile in PercentileToResultsMap.Keys)
            {
                // resultsStr += String.Format("Target Percentile = {0}, Utilization (%)\n", percentile);
                PercentileToResultsMap[percentile].percentile = percentile;
                resultsStr += PercentileToResultsMap[percentile].GetPoolsResultsStr();
            }
            resultsStr += "\n";

            resultsStr += GetHostRolesResultsHeader();

            foreach (var percentile in PercentileToResultsMap.Keys)
            {
                // resultsStr += String.Format("Target Percentile = {0}, Utilization (%)\n", percentile);
                PercentileToResultsMap[percentile].percentile = percentile;
                resultsStr += PercentileToResultsMap[percentile].GetHostRolesResultsStr();
            }
            resultsStr += "\n";

            resultsStr += String.Format("\n{0},{1},{2},{3},{4},{5}\n",
                    "Percentile",
                    "Host Roles Pool Size",
                    "COGS",
                    "COGS(%)",
                    "Expected Failure Rate",
                    "Measure Failure Rate");
            var maxCOGS = PercentileToResultsMap[PercentileToResultsMap.Keys.Max()].TotalCoreHour;
            foreach (var percentile in PercentileToResultsMap.Keys)
            {
                resultsStr += String.Format("{0}, {1}\n", Utilities.PercentileToString(percentile), PercentileToResultsMap[percentile].SummaryStr(maxCOGS));
            }
            resultsStr += "\n\n";
            return resultsStr;
        }
    }

    public class Experiment
    {
        /*
         *  Experiment parameters
         */

        // an experiment name
        public string ExpName;
        // path to the container allocation trace file
        public string AllocationTracePath;
        // path to the test container allocation trace file
        public string TestAllocationTracePath;
        // path to the pod life cycle trace file
        public string PodLifeCycleTracePath;
        public string PredictedTraceFile;
        // csv outputs file path
        public string ResultPath;
        public string InputPath;
        // list of all target SLOs/percentiles  
        public List<double> TargetPercentiles;
        public Dictionary<PoolLabel, int> StaticPoolSizesMap;
        public ExperimentResults Results;
        public int Id;
        public Trace Trace { get; set; }
        public Trace TestTrace { get; set; }
        public string TraceSummaryStr;
        public string TestTraceSummaryStr;

        /*
         *  Statistical Analysis parameters
         */

        // number of samples for the container pool sliding window analysis
        public int PoolDemandAnalysisSamplesCount;
        // number of samples for the host role sliding window analysis
        public int HostRoleDemandAnalysisSamplesCount;
        // defines how we take samples from distribution, either randomly or the average 
        public SamplingApproach SamplingApproach;
        // ignored in the current algorithm 


        /*
         *  Simulator parameters & vars
         */
        // host role creation delay distribution
        public IDistribution HostRoleInitializationDemandDistribution;
        // number of cores on a host role that can be used to run containers
        public double HostRoleCores;
        // number of host roles cores reserved for management
        public double HostRoleReservedCores;
        // maximum number of pods can be hosted on a host role
        public int MaxPodsPerHostRole;
        // the frequency at statistics/metrics are collected 
        public double CollectStatsFrequency;
        // ignore - not used
        public double SlidingWindowSize;
        // ignore - not used
        public double DistributionPercentile;
        // for future - not needed with a single poolgroup
        public bool AssignHostRolesToPoolGroup;
        // if true, combines all allocation requests in one pool (i.e., the trace is assumed for one pool)
        public bool UseCombinedPool;
        // should not change - the simulator use it
        // public double CurrentTargetPercentile;
        // if true, request queue is used (i.e., no load shedding) 
        public bool UseRequestsQueue;
        // if true, all pod transition delays are zero except creation delay 
        public bool IgnorePodTransitionsExceptCreation;
        // if true, pods recycling is enabled 
        public bool RecyclePodsSimulatorFlag;


        /*
         *  Optimizer parameters
        */
        // flag to enable/disable the optimizer (reconciliation algorithm)
        public bool OptimizerIsEnabled;
        // the frequency at which reconciliation algorithm run
        public double OptimizerFrequency;
        // if true, optimizer will aggressively create pods (i.e., utilize all cores in the system)  
        public bool OptimizerAggressivePodCreation;
        // flag to enable/disable the reconciliation algorithm
        public bool OptimizerScalingHostRolesIsEnabled;
        // flag to enable/disable host role deletion
        public bool OptimizerEnableHostRoleDeletion;

        // parameters for Host role reconciliation algorithm (obsolete)
        public double ExpandHostRolesUtilizationThreshold;
        public double TargetHostRolesUtilizationThreshold;
        public double ShrinkHostRolesUtilizationThreshold;
        internal double PredictionWindowSize;
        internal int PredictionForcePoolSize;
        internal PoolOptimizationMethod PoolOptimizationMethod;
        internal HostRoleOptimizationMethod HostRoleOptimizationMethod;
        internal RecyclingTraceSamplingApproach RecyclingTraceSamplingApproach;
        internal double ReactiveScalingUpFactor;
        internal double ReactiveScalingDownFactor;
        internal double MaxExtraCores;

        public Experiment(string pExpName = "",
                            string pTrainingTracePath = null,
                            string pTestingTracePath = null,
                            string pLifeCycleTracePath = null,
                            string pResultPath = null,
                            string pInputPath = null,
                            string pPredictedTraceFile = null,
                            DistributionEmpiricalFrequencyArray pVmCreationCdf = null,
                            double pHostRoleCores = 12,
                            double pHostRoleReservedCores = 0,
                            int pMaxPodsPerHostRole = 22,
                            double pVmExpandThreshold = 0.4,
                            double pVmTargetThreshold = 0.2,
                            double pVmShrinkThreshold = 0.1,
                            double pOptimizerFrequency = 15,
                            double pCollectStatsFrequency = 300,
                            double pSlidingWindowSize = 1,
                            double pDistributionPercentile = 1.0,
                            bool pOptimizerEnabled = true,
                            bool pScalingHostRolesIsEnabled = true,
                            bool pUseCombinedPool = false,
                            int pPoolDemandAnalysisSamplesCount = 1,
                            int pHostRoleDemandAnalysisSamplesCount = 1,
                            List<double> pTargetPercentiles = null,
                            bool pUseRequestsQueue = true,
                            bool pOptimizerAggressiveContainerCreation = false,
                            SamplingApproach pSamplingApproach = SamplingApproach.Random,
                            bool pIgnorePodTransitionsExceptCreation = false,
                            bool pRecyclePodsSimulatorFlag = true,
                            bool pOptimizerEnableHostRoleDeletion = true,
                            Dictionary<PoolLabel, int> pStaticPoolSizesMap = null,
                            double pPredictionInterval = 3600,
                            int pPredictionForcePoolSize = -1,
                            PoolOptimizationMethod pPoolOptimizationMethod = PoolOptimizationMethod.DROPS,
                            HostRoleOptimizationMethod pVmOptimizationMethod = HostRoleOptimizationMethod.DROPS,
                            RecyclingTraceSamplingApproach pRecyclingTraceSamplingApproach = RecyclingTraceSamplingApproach.Random,
                            double pReactiveScalingUpFactor = 1.35,
                            double pReactiveScalingDownFactor = 1
                        )
        {
            ExpName = pExpName;
            AllocationTracePath = pTrainingTracePath;
            TestAllocationTracePath = pTestingTracePath;
            PodLifeCycleTracePath = pLifeCycleTracePath;
            ResultPath = pResultPath;
            InputPath = pInputPath;
            PredictedTraceFile = pPredictedTraceFile;
            HostRoleCores = pHostRoleCores;
            HostRoleReservedCores = pHostRoleReservedCores;
            MaxPodsPerHostRole = pMaxPodsPerHostRole;
            ExpandHostRolesUtilizationThreshold = pVmExpandThreshold;
            TargetHostRolesUtilizationThreshold = pVmTargetThreshold;
            ShrinkHostRolesUtilizationThreshold = pVmShrinkThreshold;
            OptimizerFrequency = pOptimizerFrequency;
            CollectStatsFrequency = pCollectStatsFrequency;
            SlidingWindowSize = pSlidingWindowSize;
            DistributionPercentile = pDistributionPercentile;
            OptimizerScalingHostRolesIsEnabled = pScalingHostRolesIsEnabled;
            OptimizerIsEnabled = pOptimizerEnabled;
            UseCombinedPool = pUseCombinedPool;
            UseRequestsQueue = pUseRequestsQueue;
            SamplingApproach = pSamplingApproach;
            IgnorePodTransitionsExceptCreation = pIgnorePodTransitionsExceptCreation;
            RecyclePodsSimulatorFlag = pRecyclePodsSimulatorFlag;
            OptimizerAggressivePodCreation = pOptimizerAggressiveContainerCreation;
            OptimizerEnableHostRoleDeletion = pOptimizerEnableHostRoleDeletion;
            PoolDemandAnalysisSamplesCount = pPoolDemandAnalysisSamplesCount;
            HostRoleDemandAnalysisSamplesCount = pHostRoleDemandAnalysisSamplesCount;
            PredictionWindowSize = pPredictionInterval;
            PredictionForcePoolSize = pPredictionForcePoolSize;
            PoolOptimizationMethod = pPoolOptimizationMethod;
            HostRoleOptimizationMethod = pVmOptimizationMethod;
            RecyclingTraceSamplingApproach = pRecyclingTraceSamplingApproach;
            if (pTargetPercentiles == null)
            {
                pTargetPercentiles = new List<double> { 1.0 };
            }
            TargetPercentiles = pTargetPercentiles;

            HostRoleInitializationDemandDistribution = pVmCreationCdf;
            if (HostRoleInitializationDemandDistribution == null)
            {
                HostRoleInitializationDemandDistribution = new DistributionUniform(8 * 60, 10 * 60);
            }

            StaticPoolSizesMap = pStaticPoolSizesMap;
            ReactiveScalingUpFactor = pReactiveScalingUpFactor;
            ReactiveScalingDownFactor = pReactiveScalingDownFactor;
            MaxExtraCores = 0;
        }

        internal void InitResultsObject(List<PoolLabel> poolLabelsList)
        {
            Results = new ExperimentResults(poolLabelsList, TargetPercentiles);
        }

        internal static string GetHostRoleDemandCountDistFileName(int expNum, string traceName, double targetPercentile)
        {
            return String.Format("{0}-{1}-{2}-{3}", expNum, traceName, targetPercentile, "hostrole_demand_count_distribution.csv");
        }

        internal static string GetTraceFileName(int expNum, string traceName)
        {
            return String.Format("{0}-{1}-{2}", expNum, traceName, "trace.csv");
        }

        internal static string GetLifeCycleDistributionsFileName(int expNum, string traceName)
        {
            return String.Format("{0}-{1}-{2}", expNum, traceName, "life_cycle_distributions.csv");
        }

        internal static string GetSmoothedTracesFileName(int expNum, string traceName)
        {
            return String.Format("{0}-{1}-{2}", expNum, traceName, "smoothed_traces.csv");
        }

        internal static string GetOptimalPoolSizeFileName(int expNum, string traceName)
        {
            return String.Format("{0}-{1}-{2}", expNum, traceName, "optimal_pool_size.csv");
        }

        internal static string GetPoolSizeDistFileName(int expNum, string traceName)
        {
            return String.Format("{0}-{1}-{2}", expNum, traceName, "pool_size_distribution.csv");
        }

        internal static string GetPodsPoolSizeToSuccessRateFileName(int expNum, string traceName)
        {
            return String.Format("{0}-{1}-{2}", expNum, traceName, "pool_size_to_success_rate.csv");
        }

        internal static string GetTraceReplayStatsFileName(int expNum, string traceName, double targetPercentile)
        {
            return String.Format("{0}-{1}-{2}-{3}", expNum, traceName, targetPercentile, "replay_stats.csv");
        }

        internal static string GetRequestLatencyFileName(int expNum, string traceName, double targetPercentile)
        {
            return String.Format("{0}-{1}-{2}-{3}", expNum, traceName, targetPercentile, "request_latency.csv");
        }

        internal static string GetHostRoleAllocationTraceFileName(int expNum, string traceName, double targetPercentile)
        {
            return String.Format("{0}-{1}-{2}-{3}", expNum, traceName, targetPercentile, "hostrole_allocation.csv");
        }

        internal static string GetResultsSummaryFileName()
        {
            return String.Format("results.csv");
        }

        internal static string GetCoreHourFileName(int expNum, string traceName, double targetPercentile)
        {
            return String.Format("{0}-{1}-{2}-{3}", expNum, traceName, targetPercentile, "average_core_time.csv");
        }

        internal static string GetPerPoolHostRoleDemandFileName(int expNum, string traceName)
        {
            return String.Format("{0}-{1}-{2}", expNum, traceName, "pools_hostrole_demand_dist.csv");
        }
    }
}
