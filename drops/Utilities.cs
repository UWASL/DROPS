using System.Text.Json;

namespace ServerlessPoolOptimizer
{
    public class Utilities
    {

        public static List<Experiment> ParseExperiments(string configFile)
        {

            string json = File.ReadAllText(configFile);
            using JsonDocument doc = JsonDocument.Parse(json);
            JsonElement rootElement = doc.RootElement;

            string rootDirectory = rootElement.GetProperty("rootPath").GetString();
            string resultsDirectory = rootDirectory + rootElement.GetProperty("resultsFolder").GetString() + "/";
            string inputDirectory = rootDirectory + rootElement.GetProperty("inputFolder").GetString() + "/";
            string tracesDirectory = rootDirectory + rootElement.GetProperty("tracesFolder").GetString() + "/";

            if (!Directory.Exists(resultsDirectory))
            {
                // Create folder
                Directory.CreateDirectory(resultsDirectory);
                Console.WriteLine("Created results folder:" + resultsDirectory);
            }

            if (!Directory.Exists(inputDirectory))
            {
                Console.WriteLine("Directory does not exist:" + inputDirectory);
                Environment.Exit(1);
            }

            if (!Directory.Exists(tracesDirectory))
            {
                Console.WriteLine("Directory does not exist:" + tracesDirectory);
                Environment.Exit(1);
            }

            List<Experiment> experiments = new List<Experiment>();

            foreach (JsonElement exp in rootElement.GetProperty("experiments").EnumerateArray())
            {
                string containersOptimizationMethodStr = exp.GetProperty("containerOptimizationMethod").GetString();
                PoolOptimizationMethod containersOptimizationMethod = (PoolOptimizationMethod)Enum.Parse(typeof(PoolOptimizationMethod), containersOptimizationMethodStr);
                var vmOptimizationMethod = Utilities.MapContainerOptMethodToVmOptMethod(containersOptimizationMethod);
                List<double> percentiles = new();
                foreach (JsonElement p in exp.GetProperty("percentiles").EnumerateArray())
                {
                    percentiles.Add(p.GetDouble());
                }

                string vmCreationCdfFile = exp.GetProperty("vmCreationCdfPath").GetString();
                vmCreationCdfFile = tracesDirectory + vmCreationCdfFile;
                var vmCreationCdf = Utilities.GetVmCreationDistribution(vmCreationCdfFile);

                int collectStatsFrequency = 600;
                if (exp.TryGetProperty("collectStatsFrequency", out JsonElement collectStatsFrequencyElem))
                {
                    collectStatsFrequency = collectStatsFrequencyElem.GetInt32();
                }

                string lifeCycleTraceName = exp.GetProperty("lifeCycleTraceName").GetString();

                string trainingTraceName;
                string testingTraceName;
                string expName;

                switch (containersOptimizationMethod)
                {
                    case PoolOptimizationMethod.DROPS:
                        bool isAggressiveContainerCreation = exp.GetProperty("isAggressiveContainerCreation").GetBoolean();
                        if (isAggressiveContainerCreation)
                        {
                            expName = "sub-Aggressive";
                        }
                        else
                        {
                            expName = "sub-DROPS";
                        }

                        List<string> trainingTraces = new();
                        List<string> testingTraces = new();

                        foreach (JsonElement traceStr in exp.GetProperty("trainingTraceName").EnumerateArray())
                        {
                            trainingTraces.Add(traceStr.GetString());
                        }

                        foreach (JsonElement traceStr in exp.GetProperty("testingTraceName").EnumerateArray())
                        {
                            testingTraces.Add(traceStr.GetString());
                        }

                        for (int i = 0; i < trainingTraces.Count(); i++)
                        {
                            trainingTraceName = trainingTraces[i];
                            testingTraceName = testingTraces[i];
                            experiments.Add(new Experiment(
                                            pExpName: expName,
                                            pTrainingTracePath: tracesDirectory + trainingTraceName,
                                            pTestingTracePath: tracesDirectory + testingTraceName,
                                            pLifeCycleTracePath: tracesDirectory + lifeCycleTraceName,
                                            pResultPath: resultsDirectory,
                                            pTargetPercentiles: percentiles,
                                            pVmCreationCdf: vmCreationCdf,
                                            pOptimizerAggressiveContainerCreation: isAggressiveContainerCreation,
                                            pPoolOptimizationMethod: containersOptimizationMethod,
                                            pVmOptimizationMethod: vmOptimizationMethod,
                                            pCollectStatsFrequency: collectStatsFrequency
                                        ));
                            // counter++;
                        }
                        break;

                    case PoolOptimizationMethod.Production:
                        double vmExpandThreshold = exp.GetProperty("vmExpandThreshold").GetDouble();
                        double vmShrinkThreshold = exp.GetProperty("vmShrinkThreshold").GetDouble();
                        double vmTargetThreshold = exp.GetProperty("vmTargetThreshold").GetDouble();
                        trainingTraceName = exp.GetProperty("trainingTraceName").GetString();
                        testingTraceName = exp.GetProperty("testingTraceName").GetString();
                        Dictionary<PoolLabel, int> productionPoolSizes = Parameter.GetProductionPoolSizes();
                        experiments.Add(new Experiment(
                            pExpName: containersOptimizationMethodStr,
                            pTrainingTracePath: tracesDirectory + trainingTraceName,
                            pTestingTracePath: tracesDirectory + testingTraceName,
                            pLifeCycleTracePath: tracesDirectory + lifeCycleTraceName,
                            pResultPath: resultsDirectory,
                            pTargetPercentiles: percentiles,
                            pStaticPoolSizesMap: productionPoolSizes,
                            pVmCreationCdf: vmCreationCdf,
                            pPoolOptimizationMethod: containersOptimizationMethod,
                            pVmOptimizationMethod: vmOptimizationMethod,
                            pVmExpandThreshold: vmExpandThreshold,
                            pVmShrinkThreshold: vmShrinkThreshold,
                            pVmTargetThreshold: vmTargetThreshold,
                            pCollectStatsFrequency: collectStatsFrequency
                        ));
                        break;

                    case PoolOptimizationMethod.Reactive:

                        double scaleUpFactor = exp.GetProperty("scaleUpFactor").GetDouble();
                        double scaleDownFactor = exp.GetProperty("scaleDownFactor").GetDouble();
                        trainingTraceName = exp.GetProperty("trainingTraceName").GetString();
                        testingTraceName = exp.GetProperty("testingTraceName").GetString();

                        experiments.Add(new Experiment(
                                        pExpName: containersOptimizationMethodStr + "-" + scaleUpFactor + "-" + scaleDownFactor,
                                        pTrainingTracePath: tracesDirectory + trainingTraceName,
                                        pTestingTracePath: tracesDirectory + testingTraceName,
                                        pLifeCycleTracePath: tracesDirectory + lifeCycleTraceName,
                                        pResultPath: resultsDirectory,
                                        pTargetPercentiles: percentiles,
                                        pVmCreationCdf: vmCreationCdf,
                                        pPoolOptimizationMethod: containersOptimizationMethod,
                                        pVmOptimizationMethod: vmOptimizationMethod,
                                        pReactiveScalingUpFactor: scaleUpFactor,
                                        pReactiveScalingDownFactor: scaleDownFactor,
                                        pCollectStatsFrequency: collectStatsFrequency
                                    ));
                        break;

                    case PoolOptimizationMethod.PredictiveReactive:
                    case PoolOptimizationMethod.PredictionPoissonLoad:
                    case PoolOptimizationMethod.PredictionConstantLoad:
                    case PoolOptimizationMethod.PredictionConcentratedLoad:

                        trainingTraceName = exp.GetProperty("trainingTraceName").GetString();
                        testingTraceName = exp.GetProperty("testingTraceName").GetString();

                        double predictionInterval = exp.GetProperty("predictionInterval").GetDouble();
                        string predictionFile = null;
                        expName = "perfect-";
                        if (exp.TryGetProperty("predictionFile", out JsonElement predictionFileElem))
                        {
                            predictionFile = tracesDirectory + "/" + predictionFileElem.GetString();
                            expName = "model-";
                        }
                        expName += containersOptimizationMethodStr + "-";
                        expName += predictionInterval + "-sec";

                        experiments.Add(new Experiment(
                                pExpName: expName,
                                pTrainingTracePath: tracesDirectory + trainingTraceName,
                                pTestingTracePath: tracesDirectory + testingTraceName,
                                pLifeCycleTracePath: tracesDirectory + lifeCycleTraceName,
                                pResultPath: resultsDirectory,
                                pTargetPercentiles: percentiles,
                                pVmCreationCdf: vmCreationCdf,
                                pPoolOptimizationMethod: containersOptimizationMethod,
                                pVmOptimizationMethod: vmOptimizationMethod,
                                pPredictionInterval: predictionInterval,
                                pPredictedTraceFile: predictionFile,
                                pCollectStatsFrequency: collectStatsFrequency
                        ));
                        break;
                }
            }
            return experiments;
        }


        public static string GetValuePercentageStr(double value, double total)
        {
            string str = String.Format("{0:0.00}({1:0.00}%)", value, value / total * 100);
            return str;
        }

        public static double ComputeNeededHostRoles(int podsCount, double podCores, double hostRoleCores, int hostRoleMaxPods)
        {
            double totalCores = podsCount * podCores;
            double hostRoles;
            if (podCores <= 0.5)
            {
                hostRoles = (double)podsCount / hostRoleMaxPods;
            }
            else
            {
                hostRoles = totalCores / hostRoleCores;
            }
            return hostRoles;
        }

        public static double ComputeNeededHostRoles(Dictionary<double, int> podsCountMap, double hostRoleCores, int hostRoleMaxPods)
        {
            double hostRoles = 0;
            foreach (var (podSize, count) in podsCountMap)
            {
                hostRoles += ComputeNeededHostRoles(count, podSize, hostRoleCores, hostRoleMaxPods);
            }
            return hostRoles;
        }

        internal static string PercentileToString(double percentile)
        {
            if (percentile == 1)
            {
                return "P100";
            }
            else if (percentile == 0.99995)
            {
                return "P99.995";
            }
            else if (percentile == 0.9999)
            {
                return "P99.99";
            }
            else if (percentile == 0.999)
            {
                return "P99.9";
            }
            else if (percentile == 0.99)
            {
                return "P99";
            }
            else if (percentile == 0.95)
            {
                return "P95";
            }
            else
            {
                return "P" + (int)(percentile * 100);
            }
        }

        internal static void AddDistribution(DistributionEmpiricalFrequencyArray src,
                                                DistributionEmpiricalFrequencyArray dst)
        {
            var count = src.PairsCount();
            for (int i = 0; i < count; i++)
            {
                var element = src.GetValueFreqPairByIndex(i);
                var value = element.Key;
                var freq = element.Value.Item1;
                dst.AddValueFrequency(value, freq);
            }
        }

        public static DistributionEmpiricalFrequencyArray GetVmCreationDistribution(string path)
        {
            var hostRoleCreationDelayDistribution = new DistributionEmpiricalFrequencyArray();
            try
            {
                foreach (string line in File.ReadLines(path))
                {
                    hostRoleCreationDelayDistribution.AddValueFrequency(double.Parse(line), 1);
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Error while parsing VM creation CDF file: {0}", path);
                Environment.Exit(0);
            }
            return hostRoleCreationDelayDistribution;
        }

        public static HostRoleOptimizationMethod MapContainerOptMethodToVmOptMethod(PoolOptimizationMethod containersMethod)
        {
            HostRoleOptimizationMethod vmOptimizationMethod;
            switch (containersMethod)
            {
                case PoolOptimizationMethod.DROPS:
                    vmOptimizationMethod = HostRoleOptimizationMethod.DROPS;
                    break;
                case PoolOptimizationMethod.Production:
                case PoolOptimizationMethod.PredictionConcentratedLoad:
                case PoolOptimizationMethod.PredictionPoissonLoad:
                case PoolOptimizationMethod.PredictionConstantLoad:
                    vmOptimizationMethod = HostRoleOptimizationMethod.Production;
                    break;
                case PoolOptimizationMethod.PredictiveReactive:
                case PoolOptimizationMethod.Reactive:
                    vmOptimizationMethod = HostRoleOptimizationMethod.PredictiveReactive;
                    break;
                default:
                    throw new IndexOutOfRangeException();
            }
            return vmOptimizationMethod;
        }

        public static void WriteCostResults(List<Experiment> experiments, string filePath,
                                            Experiment combinedExp, Experiment combinedAggressiveExp)
        {
            string resultsStr = "";
            resultsStr += String.Format("{0},{1},{2},{3},{4},{5},{6},{7}\n",
                                            "Percentile",
                                            "Exp",
                                            "Total Core Hours",
                                            "Total Containers Core Hours",
                                            "Pool Core Hours",
                                            "Total Requests",
                                            "Total Failed Requests",
                                            "Failure Rate"
                                        );

            foreach (var percentile in experiments[0].TargetPercentiles)
            {
                foreach (var exp in experiments)
                {
                    if (!exp.Results.PercentileToResultsMap.ContainsKey(percentile))
                    {
                        continue;
                    }
                    if (exp.ExpName.Contains("sub"))
                    {
                        continue;
                    }
                    var maxCOGS = exp.Results.PercentileToResultsMap[exp.Results.PercentileToResultsMap.Keys.Max()].TotalCoreHour;
                    var percentileResults = exp.Results.PercentileToResultsMap[percentile];
                    resultsStr += String.Format("{0},{1},{2}\n", Utilities.PercentileToString(percentile), exp.ExpName, percentileResults.SummaryStr(maxCOGS));
                }
                var combinedPercentileResults = combinedExp.Results.PercentileToResultsMap[percentile];
                var combinedAggressivePercentileResults = combinedAggressiveExp.Results.PercentileToResultsMap[percentile];
                HostRoleStateTimeTracker combinedCostBreaker = null;
                if (combinedPercentileResults != null)
                {
                    string coreHoursFilePath = experiments[0].ResultPath + Experiment.GetCoreHourFileName(0,
                                                                                                combinedExp.ExpName,
                                                                                                percentile);
                    combinedCostBreaker = ServerlessService.MeasureHostRoleStateTimeBreaker(coreHoursFilePath, combinedPercentileResults, null);
                    var percentileResults = combinedExp.Results.PercentileToResultsMap[combinedExp.Results.PercentileToResultsMap.Keys.Max()];
                    var maxCOGS2 = 1.0;
                    if (percentileResults != null)
                        maxCOGS2 = percentileResults.TotalCoreHour;
                    resultsStr += String.Format("{0},{1},{2}\n", Utilities.PercentileToString(percentile), combinedExp.ExpName, combinedPercentileResults.SummaryStr(maxCOGS2));

                    string path2 = experiments[0].ResultPath + Experiment.GetRequestLatencyFileName(0, combinedExp.ExpName, percentile);
                    AnalysisHelper.DumpFrequencyDistribution(combinedPercentileResults.RequestLatencyDistribution, path2);
                }

                if (combinedAggressivePercentileResults != null)
                {
                    string coreHoursFilePath = experiments[0].ResultPath + Experiment.GetCoreHourFileName(0,
                                                                                                combinedAggressiveExp.ExpName,
                                                                                                percentile);
                    ServerlessService.MeasureHostRoleStateTimeBreaker(coreHoursFilePath, combinedAggressivePercentileResults, combinedCostBreaker);
                    var percentileResults = combinedAggressiveExp.Results.PercentileToResultsMap[combinedAggressiveExp.Results.PercentileToResultsMap.Keys.Max()];
                    var maxCOGS2 = 1.0;
                    if (percentileResults != null)
                        maxCOGS2 = percentileResults.TotalCoreHour;
                    resultsStr += String.Format("{0},{1},{2}\n", Utilities.PercentileToString(percentile), combinedAggressiveExp.ExpName, combinedAggressivePercentileResults.SummaryStr(maxCOGS2));

                    string latencyFile = experiments[0].ResultPath + Experiment.GetRequestLatencyFileName(0, combinedAggressiveExp.ExpName, percentile);
                    AnalysisHelper.DumpFrequencyDistribution(combinedAggressivePercentileResults.RequestLatencyDistribution, latencyFile);
                }
            }

            try
            {
                StreamWriter outputFile = new StreamWriter(filePath);
                outputFile.WriteLine(resultsStr);
                outputFile.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void WriteResults(List<Experiment> experiments)
        {
            Experiment combinedExp = new Experiment("DROPS", pTargetPercentiles: experiments[0].TargetPercentiles);
            Experiment combinedAggressiveExp = new Experiment("DROPS-Aggressive", pTargetPercentiles: experiments[0].TargetPercentiles);
            combinedExp.InitResultsObject(experiments[0].Results.PercentileToResultsMap[1.0].PoolLabelToPoolStatsMap.Keys.ToList());
            combinedAggressiveExp.InitResultsObject(experiments[0].Results.PercentileToResultsMap[1.0].PoolLabelToPoolStatsMap.Keys.ToList());

            foreach (var percentile in experiments[0].TargetPercentiles)
            {
                combinedExp.Results.PercentileToResultsMap[percentile] = CombineSubExperiments(experiments, percentile, "DROPS");
                combinedAggressiveExp.Results.PercentileToResultsMap[percentile] = CombineSubExperiments(experiments, percentile, "Aggressive");
            }

            foreach (var percentile in experiments[0].TargetPercentiles)
            {
                foreach (var exp in experiments)
                {
                    if (!exp.Results.PercentileToResultsMap.ContainsKey(percentile))
                    {
                        continue;
                    }
                    if (exp.ExpName.Contains("sub"))
                    {
                        continue;
                    }
                    var percentileResults = exp.Results.PercentileToResultsMap[percentile];
                    percentileResults.percentile = percentile;
                    percentileResults.GetPoolsResultsStr(exp.ExpName);
                }

                if (combinedExp.Results.PercentileToResultsMap[percentile] != null)
                {
                    combinedExp.Results.PercentileToResultsMap[percentile].GetPoolsResultsStr(combinedExp.ExpName);
                }
                if (combinedAggressiveExp.Results.PercentileToResultsMap[percentile] != null)
                {
                    combinedAggressiveExp.Results.PercentileToResultsMap[percentile].GetPoolsResultsStr(combinedAggressiveExp.ExpName);
                }
            }

            string costFilePath = experiments[0].ResultPath + "cost.csv";
            WriteCostResults(experiments, costFilePath, combinedExp, combinedAggressiveExp);

            experiments.Add(combinedExp);
            experiments.Add(combinedAggressiveExp);
        }

        public static PercentileResults CombineSubExperiments(List<Experiment> experiments, double percentile, string keyword)
        {
            PercentileResults combinedResults = null;
            int count = 0;
            foreach (var exp in experiments)
            {
                if (!exp.ExpName.Contains(keyword))
                {
                    continue;
                }
                count++;
                var percentileResults = exp.Results.PercentileToResultsMap[percentile];
                if (percentileResults.HostRolesList == null)
                {
                    return combinedResults;
                }
                if (combinedResults == null)
                {
                    combinedResults = new PercentileResults(percentileResults.PoolLabelToPoolStatsMap.Keys.ToList());
                    combinedResults.HostRolesList = new List<HostRole>();
                }
                combinedResults.percentile = percentile;
                combinedResults.TotalRequests += percentileResults.TotalRequests;
                combinedResults.TotalFailedRequests += percentileResults.TotalFailedRequests;
                combinedResults.HostRolesPoolSize += percentileResults.HostRolesPoolSize;
                combinedResults.HostRolesList.AddRange(percentileResults.HostRolesList);
                Utilities.AddDistribution(percentileResults.RequestLatencyDistribution, combinedResults.RequestLatencyDistribution);
                foreach (var (poolLabel, poolStats) in percentileResults.PoolLabelToPoolStatsMap)
                {
                    var combinedPoolStats = combinedResults.PoolLabelToPoolStatsMap[poolLabel];
                    combinedPoolStats._totalFailedRequestsCount += poolStats._totalFailedRequestsCount;
                    combinedPoolStats._totalRequestsCount += poolStats._totalRequestsCount;
                    combinedPoolStats._failedPodCreationCount += poolStats._failedPodCreationCount;
                    combinedPoolStats._totalPodCreationCount += poolStats._totalPodCreationCount;
                    combinedPoolStats._totalSucceededRequestsCount += poolStats._totalSucceededRequestsCount;
                    Utilities.AddDistribution(poolStats.PoolUtilizationDistribution, combinedPoolStats.PoolUtilizationDistribution);
                    combinedResults.PoolSizeMap[poolLabel] += percentileResults.PoolSizeMap[poolLabel];
                }
            }
            if (combinedResults == null)
            {
                return combinedResults;
            }
            combinedResults.MeasuredFailureRate = combinedResults.TotalFailedRequests / combinedResults.TotalRequests * 100.0;
            foreach (var (poolLabel, poolStats) in combinedResults.PoolLabelToPoolStatsMap)
            {
                combinedResults.PoolSizeMap[poolLabel] /= count;
                combinedResults.HostRolesPoolSize /= count;
            }

            return combinedResults;
        }
    }
}