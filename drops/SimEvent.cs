using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public enum EventType
    {
        HostRoleBecomesReady, HostRoleBecomesDeleted,
        RequestArrive, RequestDepart,
        PodBecomesPending, PodBecomesReady, PodRecyclingStarts, PodBecomesRecycled, PodBecomesAllocated, PodBecomesSpecialized, PodRunningUserWorkload, PodBecomesDeleted,
        ReactiveUpdatePoolSize, RunOptimizer, UpdatePoolSizes,
        ServiceInitializationComplete, CollectStats, EndOfTrace
    }

    public class SimEvent : EventArgs, IComparable
    {
        private readonly int _id;
        private readonly double _createTimePoint;
        private readonly double _triggerTimePoint;
        private readonly EventType _eventType;
        public readonly AllocationRequest? Request;
        public readonly HostRole? HostRole;
        public readonly Pod? Pod;

        public readonly List<PoolLabel>? PoolLabels;

        public SimEvent(int _eventId, EventType pEventType, double pCreateTimePoint, double pTriggerTimePoint,
                            AllocationRequest? pRequest, HostRole? pHostRole, Pod? pPod, List<PoolLabel>? pPoolLabels)
        {
            if (pPod != null)
            {
                Debug.Assert(pCreateTimePoint <= pTriggerTimePoint, String.Format("event trigger time cannot be before creation time: pEventType: {0}, creation time: {1}, trigger time: {2}, Pod lifecycle: {3}",
                                                                                        pEventType, pCreateTimePoint, pTriggerTimePoint, pPod.LifeCycleTimestamps));
            }
            else
            {
                Debug.Assert(pCreateTimePoint <= pTriggerTimePoint, String.Format("event trigger time cannot be before creation time: creation time: {0}, trigger time:{1}",
                                                                                        pCreateTimePoint, pTriggerTimePoint));
            }
            _id = _eventId;
            _createTimePoint = pCreateTimePoint;
            _triggerTimePoint = pTriggerTimePoint;
            _eventType = pEventType;
            switch (_eventType)
            {
                case EventType.RequestArrive:
                case EventType.RequestDepart:
                    Debug.Assert(pRequest != null);
                    Request = pRequest;
                    break;

                case EventType.HostRoleBecomesReady:
                case EventType.HostRoleBecomesDeleted:
                    Debug.Assert(pHostRole != null);
                    HostRole = pHostRole;
                    break;

                case EventType.PodBecomesPending:
                case EventType.PodBecomesReady:
                case EventType.PodBecomesAllocated:
                case EventType.PodBecomesSpecialized:
                case EventType.PodRunningUserWorkload:
                case EventType.PodBecomesDeleted:
                case EventType.PodRecyclingStarts:
                case EventType.PodBecomesRecycled:
                    Debug.Assert(pPod != null);
                    Pod = pPod;
                    break;

                case EventType.UpdatePoolSizes:
                    PoolLabels = pPoolLabels;
                    break;

                case EventType.RunOptimizer:
                case EventType.ServiceInitializationComplete:
                case EventType.CollectStats:
                case EventType.EndOfTrace:
                case EventType.ReactiveUpdatePoolSize:
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
            //Console.WriteLine("  ctime {0:000.00}, {1}", _createTimePoint, this);
        }

        public double GetTriggerTimePoint()
        {
            return _triggerTimePoint;
        }

        public EventType GetEventType()
        {
            return _eventType;
        }

        public AllocationRequest? GetRequest()
        {
            Debug.Assert(_eventType == EventType.RequestArrive || _eventType == EventType.RequestDepart);
            return Request;
        }

        public HostRole? GetHostRole()
        {
            Debug.Assert(_eventType == EventType.HostRoleBecomesReady || _eventType == EventType.HostRoleBecomesDeleted);
            return HostRole;
        }

        public Pod? GetPod()
        {
            Debug.Assert(_eventType == EventType.PodBecomesPending
                            || _eventType == EventType.PodBecomesReady
                            || _eventType == EventType.PodBecomesAllocated
                            || _eventType == EventType.PodBecomesSpecialized
                            || _eventType == EventType.PodRunningUserWorkload
                            || _eventType == EventType.PodBecomesDeleted
                            || _eventType == EventType.PodRecyclingStarts
                            || _eventType == EventType.PodBecomesRecycled);
            return Pod;
        }

        public override string ToString()
        {
            return String.Format("event {0}, type {1}, create_time {2:000.00}, trigger_time {3:000.00}, {4}", _id, _eventType, _createTimePoint, _triggerTimePoint, Request);
        }

        public int ComapreEventTypes(EventType type1, EventType type2)
        {
            if ((int)type1 == (int)type2)
            {
                // same types
                return 0;
            }
            else if ((int)type1 < (int)type2)
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }

        public int CompareTo(object obj)
        {
            if (obj is SimEvent)
            {
                SimEvent other = (SimEvent)obj;

                // are they the same?
                if (_id == other._id)
                {
                    return 0;
                }
                else
                {
                    int timeCompare = _triggerTimePoint.CompareTo(other._triggerTimePoint);
                    if (timeCompare == 0)
                    {
                        int eventTypeCompare = ComapreEventTypes(_eventType, other._eventType);
                        if (eventTypeCompare == 0)
                            return _id.CompareTo(other._id);
                        else
                            return eventTypeCompare;
                    }
                    else
                        return timeCompare;
                }
            }
            throw new NotImplementedException();
        }
    }

    //we use a special comparer for sorting to allow duplicates 
    public sealed class ComparerAllowDuplicate<T> : IComparer<T> where T : IComparable
    {
        int IComparer<T>.Compare(T x, T y)
        {
            var comp = x.CompareTo(y);
            //return comp != 0 ? comp : 1;
            return comp;
        }
    }
}
