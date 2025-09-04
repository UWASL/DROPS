using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public class OpenLoopLoad(Simulator pSimulator,
                                IDistribution? pInterArrivalDistribution,
                                IDistribution? pRequestedPodsDistribution,
                                Experiment exp,
                                Trace? pTrace,
                                ISimulationTimeReader pSimulationTime)
    {
        private readonly Simulator _simulator = pSimulator;
        private readonly IDistribution? _interArrivalDistribution = pInterArrivalDistribution;
        private readonly IDistribution? _requestedPodsDistribution = pRequestedPodsDistribution;
        private readonly IDistribution? _requestedCoresDistribution = pRequestedPodsDistribution;
        private Trace? _trace = pTrace;
        private readonly ISimulationTimeReader _clock = pSimulationTime;
        private double _traceLastRequestArrivalTime = 0.0;
        private double _sumInterArrival = 0.0;
        private double _sumRequestedPods = 0.0;
        private int _requestArrivedCounter = 0;
        private int _requestBatchCounter = 0;
        private double _traceReferenceTimePoint = 0;
        private bool _endOfTraceEventAlreadtFired = false;

        private double GetArrivalRate()
        {
            return 1.0 / _interArrivalDistribution.GetMean();
        }

        private double GetServiceRate()
        {
            return 1.0 / _requestedPodsDistribution.GetMean();
        }

        public override string ToString()
        {
            return String.Format("Loader [Util:{0:00}% iA:{1:00.00}, S:{2:00.00}, iAA:{3:00.00}, SS:{4:00.00}, ReqArrived:{5:000}, ARate:{6:000.000}, SRate:{7:000.000}, A:{8}, B:{9}]",
                GetArrivalRate() / GetServiceRate() * 100.0,
                _interArrivalDistribution.GetMean(),
                _requestedPodsDistribution.GetMean(),
                _requestArrivedCounter > 0 ? _sumInterArrival / _requestArrivedCounter : _sumInterArrival,
                _requestArrivedCounter > 0 ? _sumRequestedPods / _requestArrivedCounter : _sumRequestedPods,
                _requestArrivedCounter,
                GetArrivalRate(), GetServiceRate(),
                _interArrivalDistribution, _requestedPodsDistribution
            );
        }

        public event EventHandler<SimEvent> FireRequestWillArrive;
        public event EventHandler<String> FireGetMyStatus;
        public event EventHandler<SimEvent> FireEndOfTrace;

        public void HandleGetStatusNotification(object sender, string s)
        {
            FireGetMyStatus(this, ToString());
        }

        private void GenerateRequestsFromTrace(int requestBatchIndex)
        {
            if (_endOfTraceEventAlreadtFired)
            {
                return;
            }
            List<AllocationRequest> requestsList = _trace.GetNextRequestsBatch(requestBatchIndex, _traceReferenceTimePoint);
            if (requestsList.Count == 0)
            {
                if (!_endOfTraceEventAlreadtFired)
                {
                    _trace.Close();
                    _endOfTraceEventAlreadtFired = true;
                    SimEvent newEvent = _simulator.CreateEvent(EventType.EndOfTrace, _clock.Now, _traceLastRequestArrivalTime, null, null, null);
                    FireEndOfTrace(this, newEvent);
                }
                return;
            }
            _traceLastRequestArrivalTime = requestsList[requestsList.Count - 1].ArrivalTimePoint;
            for (int i = 0; i < requestsList.Count; i++)
            {
                var nextEvent = _simulator.CreateEvent(EventType.RequestArrive, _clock.Now, requestsList[i].ArrivalTimePoint, requestsList[i], null, null);
                _requestArrivedCounter++;
                _sumInterArrival += requestsList[i].ArrivalTimePoint;
                FireRequestWillArrive(this, nextEvent);
            }
        }

        private void GenerateRequestsFromDistributions()
        {
            var timeInterval = _interArrivalDistribution.GetSample();
            var arrivalTimePoint = _clock.Now + timeInterval;
            var requestedPods = _requestedPodsDistribution.GetSample();
            var requestedCores = _requestedCoresDistribution.GetSample();
            _sumRequestedPods += requestedPods;
            for (int i = 0; i < (int)requestedPods; i++)
            {
                var allocationLabel = Parameter.CombinedPoolAllocationLabel;
                var newRequest = new AllocationRequest(arrivalTimePoint, allocationLabel, 1, requestedCores, RequestType.Allocation);
                var nextEvent = _simulator.CreateEvent(EventType.RequestArrive, _clock.Now, newRequest.ArrivalTimePoint, newRequest, null, null);
                _requestArrivedCounter++;
                _sumInterArrival += timeInterval;
                FireRequestWillArrive(this, nextEvent);
            }
        }

        public void HandleRequestNowArrivesNotification(object sender, AllocationRequest r)
        {
            if (_requestBatchCounter == 0)
            {
                _traceReferenceTimePoint = _clock.Now;
            }
            if (_trace != null)
            {
                GenerateRequestsFromTrace(_requestBatchCounter);
            }
            else
            {
                GenerateRequestsFromDistributions();
            }
            _requestBatchCounter++;
        }
    }
}
