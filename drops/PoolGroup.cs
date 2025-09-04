using System.Data.Common;

namespace ServerlessPoolOptimizer
{
    public struct PoolGroupId(int pId) : IEquatable<PoolGroupId>
    {
        public int Id = pId;
        public readonly bool Equals(PoolGroupId other)
        {
            return Id == other.Id;
        }
        public override string ToString()
        {
            return String.Format("{0}", Id);
        }
    }

    public struct AllocationLabel(string Runtime, string RuntimeVersion) : IEquatable<AllocationLabel>
    {
        public string Runtime = Runtime;
        public string RuntimeVersion = RuntimeVersion;

        public readonly bool Equals(AllocationLabel other)
        {
            return Runtime == other.Runtime && RuntimeVersion == other.RuntimeVersion;
        }

        public override string ToString()
        {
            return String.Format("{0}|{1}", Runtime, RuntimeVersion);
        }
    }

    public class PoolGroupParameters
    {
        public PoolGroupId PoolGroupId;
        public int MinAssignedHostRolesCount;
        public int MaxAssignedHostRolesCount;
        public double MinIdleCoresCount;
        public IDictionary<AllocationLabel, SortedList<double, PoolParameters>> RuntimeToPoolParameters;
        public PoolGroupParameters(PoolGroupId pPoolGroupId)
        {
            PoolGroupId = pPoolGroupId;
            RuntimeToPoolParameters = new Dictionary<AllocationLabel, SortedList<double, PoolParameters>>();
        }

        public PoolGroupParameters(PoolGroupParameters srcPoolGroupParameters)
        {
            PoolGroupId = srcPoolGroupParameters.PoolGroupId;
            MinAssignedHostRolesCount = srcPoolGroupParameters.MinAssignedHostRolesCount;
            MaxAssignedHostRolesCount = srcPoolGroupParameters.MaxAssignedHostRolesCount;
            MinIdleCoresCount = srcPoolGroupParameters.MinIdleCoresCount;
            RuntimeToPoolParameters = new Dictionary<AllocationLabel, SortedList<double, PoolParameters>>();
            foreach (var (allocationLabel, pools) in srcPoolGroupParameters.RuntimeToPoolParameters)
            {
                RuntimeToPoolParameters.Add(allocationLabel, new SortedList<double, PoolParameters>());
                foreach (var (cores, poolParameters) in pools)
                {
                    RuntimeToPoolParameters[allocationLabel].Add(cores, new PoolParameters(poolParameters.PoolGroupId,
                                                                                            poolParameters.AllocationLabel,
                                                                                            poolParameters.Cores,
                                                                                            poolParameters._lifeCycleDistributions,
                                                                                            poolParameters._minPodsCount,
                                                                                            poolParameters._maxPodsCount,
                                                                                            poolParameters.MaxExtraPods,
                                                                                            poolParameters.ExtraCoresRatio,
                                                                                            poolParameters.PredictionReferencePoolSize,
                                                                                            poolParameters.PredictionReferencePoolSize,
                                                                                            poolParameters.SmoothedTrace
                                                                                            ));
                }
            }
        }

    }

    public class PoolGroup
    {
        public readonly PoolGroupParameters PoolGroupParameters;
        public IDictionary<AllocationLabel, SortedList<double, Pool>> RuntimeToPools;
        public PoolGroup(PoolGroupParameters pPoolGroupParameters,
                        ISimulationTimeReader pSimulationTimeReaderdouble,
                        Simulator pSimulator,
                        Experiment pExp,
                        PercentileResults pPercentileResults)
        {
            PoolGroupParameters = pPoolGroupParameters;
            RuntimeToPools = new Dictionary<AllocationLabel, SortedList<double, Pool>>();
            foreach (var (runtime, runtimePoolsParameters) in PoolGroupParameters.RuntimeToPoolParameters)
            {
                RuntimeToPools[runtime] = new SortedList<double, Pool>();
                foreach (var (poolCores, poolParameters) in runtimePoolsParameters)
                {
                    RuntimeToPools[runtime][poolCores] = new Pool(pSimulationTimeReaderdouble, pSimulator, poolParameters, pExp, pPercentileResults);
                }
            }
        }
    }

}