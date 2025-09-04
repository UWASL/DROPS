
namespace ServerlessPoolOptimizer
{
    public enum SamplingApproach
    {
        Random, Average, Frequency
    }

    public enum RecyclingTraceSamplingApproach
    {
        Random, Average
    }

    public class Parameter
    {
        public static readonly double[] PossibleCoreAllocations = [2.00, 1.00, 0.25];
        public static readonly AllocationLabel CombinedPoolAllocationLabel = new AllocationLabel("Combined", "1.0");
        public static PoolLabel CombinedPoolLabel = new PoolLabel(CombinedPoolAllocationLabel, 1.0);
        public static readonly int TraceSkipLinesCount = 2;
        public static readonly int MinHostRolesCount = 10;
        public static readonly bool AggressiveOptimizationDistributeCoresEvenly = false;
        public static readonly int ReactiveMinPoolSize = 15;
        public static readonly int ReactiveMinVmPoolSize = 5;
        public static readonly int ReactiveExtraVmPoolSize = 5;
        public static readonly int ReactiveMaxPoolSize = 4500;

        public static Dictionary<PoolLabel, int> GetProductionPoolSizes()
        {
            Dictionary<PoolLabel, int> EastusPoolSizesMap = new Dictionary<PoolLabel, int>
            {
                { new PoolLabel(new AllocationLabel("python", "3.10"), 1), 600 },
                { new PoolLabel(new AllocationLabel("python", "3.11"), 1), 600 },
                { new PoolLabel(new AllocationLabel("dotnet-isolated", "8.0"), 1), 272 },
                { new PoolLabel(new AllocationLabel("node", "20"), 1), 200 },
                { new PoolLabel(new AllocationLabel("powershell", "7.4"), 1), 40 },
                { new PoolLabel(new AllocationLabel("python", "3.10"), 2), 64 },
                { new PoolLabel(new AllocationLabel("python", "3.11"), 0.25), 5 },
                { new PoolLabel(new AllocationLabel("python", "3.11"), 2), 64 },
                { new PoolLabel(new AllocationLabel("dotnet-isolated", "8.0"), 0.25), 5 },
                { new PoolLabel(new AllocationLabel("dotnet-isolated", "8.0"), 2), 64 },
                { new PoolLabel(new AllocationLabel("java", "17"), 1), 256 },
                { new PoolLabel(new AllocationLabel("java", "11"), 1), 256 },
                { new PoolLabel(new AllocationLabel("node", "20"), 2), 40 },
                { new PoolLabel(new AllocationLabel("dotnet-isolated", "9.0"), 1), 272 },
            };
            return EastusPoolSizesMap;
        }
    }
}
