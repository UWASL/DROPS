using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    //two time concepts: TimePoint and TimeInterval
    public interface ISimulationTimeReader
    {
        double Now { get; }
    }

    public class SimulationTime : ISimulationTimeReader
    {
        public SimulationTime()
        {
            Now = 0.0;
        }

        public double Now { get; private set; }

        public void SetSimTimePoint(double pTimePoint)
        {
            Debug.Assert(Now <= pTimePoint);
            Now = pTimePoint;
        }

        public static double Round(double timePoint)
        {
            return Math.Round(timePoint, 4);
        }
    }
}
