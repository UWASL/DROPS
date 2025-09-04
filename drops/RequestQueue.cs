using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public class RequestQueue(ISimulationTimeReader pClock)
    {
        public readonly List<AllocationRequest> WaitingRequestList = new List<AllocationRequest>();
        private readonly ISimulationTimeReader _clock = pClock;
        private int _count = 0;
        private int _maxCount = 0;
        private double _countTimeProduct = 0.0;
        private double _lastChangeTimePoint = 0.0;

        public int Count()
        {
            return _count;
        }

        public void AddRequest(AllocationRequest pAllocationRequest, int index = -1)
        {
            Debug.Assert(pAllocationRequest != null);

            UpdateAverageLen();
            _count++;

            if (_count > _maxCount)
            {
                _maxCount = _count;
            }
            if (index == -1)
            {
                WaitingRequestList.Add(pAllocationRequest);
            }
            else
            {
                WaitingRequestList.Insert(index, pAllocationRequest);
            }
        }

        public AllocationRequest? RemoveRequest()
        {
            if (WaitingRequestList.Count < 1) return null;

            Debug.Assert(_count >= 1);
            UpdateAverageLen();
            _count--;

            var myRequest = WaitingRequestList.ElementAt(0);
            WaitingRequestList.RemoveAt(0);

            return myRequest;
        }

        public AllocationRequest? GetFirstRequest()
        {
            if (WaitingRequestList.Count < 1) return null;
            Debug.Assert(_count >= 1);
            var myRequest = WaitingRequestList.ElementAt(0);
            return myRequest;
        }


        public override string ToString()
        {
            return String.Format("Queue [count:{0}, max:{1}, ave:{2:00.00}]", _count, _maxCount, GetAverageLen());
        }

        private void UpdateAverageLen()
        {
            _countTimeProduct += (_clock.Now - _lastChangeTimePoint) * _count;
            _lastChangeTimePoint = _clock.Now;
        }

        private double GetAverageLen()
        {
            return _countTimeProduct / _clock.Now;
        }

        public event EventHandler<String> FireGetMyStatus;
        public void HandleGetStatusNotification(object sender, string s)
        {
            FireGetMyStatus(this, ToString());
        }

    }
}
