using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public enum RequestState
    {
        WillArrive, Successful, Failed
    }
    public enum RequestType { Allocation, Deallocation }
    public class AllocationRequest
    {
        private static int _requestIdCounter;
        public static event EventHandler<AllocationRequest> FireRequestComplete;
        public static void Init()
        {
            _requestIdCounter = 0;
            FireRequestComplete = null;
        }

        public readonly int Id;
        public readonly double ArrivalTimePoint;
        public readonly AllocationLabel AllocationPoolGroupLabel;
        public readonly double Cores;
        public readonly int RequestedPods;
        public double CompleteTimePoint = 0.0;
        public RequestType RequestType;
        public RequestState State;
        public int PodId;

        public AllocationRequest(double pArrivalPoint, AllocationLabel pAllocationPoolGroupLabel, int pRequestedPods, double pCores, RequestType pRequestType)
        {
            Id = _requestIdCounter++;
            ArrivalTimePoint = pArrivalPoint;
            AllocationPoolGroupLabel = pAllocationPoolGroupLabel;
            Cores = pCores;
            RequestedPods = pRequestedPods;
            State = RequestState.WillArrive;
            PodId = -1;
            RequestType = pRequestType;
        }

        public void ProcessingComplete(double pCompletionTimePoint, bool isSuccessful)
        {
            CompleteTimePoint = pCompletionTimePoint;
            if (isSuccessful)
            {
                Debug.Assert(PodId != -1);
                State = RequestState.Successful;
            }
            else
            {
                // Debug.Assert(PodId == -1);
                State = RequestState.Failed;
            }

            if (FireRequestComplete != null)
            {
                FireRequestComplete(this, this);
            }

        }

        public override string ToString()
        {
            return String.Format("request id {0:00} {1} ", Id, State);
        }
    }
}
