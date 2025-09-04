using System.Diagnostics;

namespace ServerlessPoolOptimizer
{

    public class Simulator(SimulationTime pSimulationTime)
    {
        private readonly SortedSet<SimEvent> _futureEvents = new SortedSet<SimEvent>(new ComparerAllowDuplicate<SimEvent>());
        private readonly SimulationTime _simulationTime = pSimulationTime;
        private int _eventCounter = 0;

        private void ScheduleEvent(SimEvent pEvent)
        {
            Debug.Assert(pEvent != null);
            _futureEvents.Add(pEvent);
        }

        public SimEvent CreateEvent(
                                    EventType pEventType,
                                    double pCreateTimePoint,
                                    double pTriggerTimePoint,
                                    AllocationRequest? pRequest,
                                    HostRole? pHostRole,
                                    Pod? pPod,
                                    List<PoolLabel>? pPoolLabels = null)
        {
            Debug.Assert(_simulationTime.Now >= pCreateTimePoint && pCreateTimePoint <= pTriggerTimePoint);
            return new SimEvent(_eventCounter++, pEventType, pCreateTimePoint, pTriggerTimePoint, pRequest, pHostRole, pPod, pPoolLabels);
        }

        public override string ToString()
        {
            return String.Format("Simulator [time {0:000.00}]", _simulationTime.Now);
        }

        public void RunSimulation(ServerlessSystem pServerlessSystem, double pStopTimePoint, int pMaxRequest)
        {
            Debug.Assert(_futureEvents.Count == 0);
            Debug.Assert(pServerlessSystem != null);
            Debug.Assert(pStopTimePoint > 0.0);

            pServerlessSystem.ServerlessService.HandleInitializeServiceNowNotification(this);
            long printStatusEvery = 100000;
            long nextStatusSteps = printStatusEvery;
            bool isEndOfTrace = false;
            while (true)
            {
                Debug.Assert(_futureEvents.Count >= 1);
                var myEvent = _futureEvents.Min;
                _futureEvents.Remove(myEvent);
                Debug.Assert(_simulationTime.Now <= myEvent.GetTriggerTimePoint());

                //only place to ** advance time **
                _simulationTime.SetSimTimePoint(myEvent.GetTriggerTimePoint());

                switch (myEvent.GetEventType())
                {
                    case EventType.RequestArrive:
                        FireRequestNowArrives(this, myEvent.GetRequest());
                        break;
                    case EventType.RequestDepart:
                        break;
                    case EventType.HostRoleBecomesReady:
                        if (myEvent.GetHostRole().IsPending())
                            FireHostRoleStateTransitionNow(this, (myEvent.GetHostRole(), HostRoleState.Ready));
                        break;
                    case EventType.HostRoleBecomesDeleted:
                        FireHostRoleStateTransitionNow(this, (myEvent.GetHostRole(), HostRoleState.Deleted));
                        break;
                    case EventType.PodBecomesPending:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.Pending));
                        break;
                    case EventType.PodBecomesReady:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.Ready));
                        break;
                    case EventType.PodBecomesAllocated:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.Allocated));
                        break;
                    case EventType.PodBecomesSpecialized:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.Specialized));
                        break;
                    case EventType.PodRunningUserWorkload:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.UserWorkload));
                        break;
                    case EventType.PodBecomesDeleted:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.Deleted));
                        break;
                    case EventType.PodRecyclingStarts:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.BeingRecycled));
                        break;
                    case EventType.PodBecomesRecycled:
                        FirePodStateTransitionNow(this, (myEvent.GetPod(), PodState.Recycled));
                        break;
                    case EventType.ServiceInitializationComplete:
                        FireServiceInitializationNowComplete(this, EventArgs.Empty);
                        break;
                    case EventType.RunOptimizer:
                        // FireRunOptimizerNow(this, true);
                        break;
                    case EventType.ReactiveUpdatePoolSize:
                        FireReactiveScaleDownPoolSizesNow(this, false);
                        break;
                    case EventType.CollectStats:
                        FireCollectStatsNow(this, EventArgs.Empty);
                        break;
                    case EventType.UpdatePoolSizes:
                        FireUpdatePoolSizesNow(this, myEvent.PoolLabels);
                        break;
                    case EventType.EndOfTrace:
                        isEndOfTrace = true;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (_simulationTime.Now >= nextStatusSteps)
                {
                    Console.WriteLine("Completed {0} simulation steps", nextStatusSteps);
                    nextStatusSteps = nextStatusSteps + printStatusEvery;
                }

                if (_simulationTime.Now >= pStopTimePoint
                    || ((myEvent.GetEventType() == EventType.RequestArrive) && (myEvent.GetRequest().Id >= pMaxRequest)))
                {
                    pServerlessSystem.ServerlessService.FinishExperiment();
                    _futureEvents.Clear();
                    return;
                }
                else if (isEndOfTrace && pServerlessSystem.ServerlessService.HasQueuedRequests() == false)
                {
                    pServerlessSystem.ServerlessService.FinishExperiment();
                    _futureEvents.Clear();
                    return;
                }
                else if (isEndOfTrace && pServerlessSystem.ServerlessService.HasQueuedRequests() == true)
                {
                    FireRequestNowArrives(this, null);
                }
            }
        }


        public event EventHandler<AllocationRequest> FireRequestNowArrives;
        public event EventHandler<(HostRole, HostRoleState)> FireHostRoleStateTransitionNow;
        public event EventHandler<(Pod, PodState)> FirePodStateTransitionNow;
        public event EventHandler FireServiceInitializationNowComplete;
        public event EventHandler<bool> FireRunOptimizerNow;
        public event EventHandler<bool> FireReactiveScaleDownPoolSizesNow;
        public event EventHandler FireCollectStatsNow;
        public event EventHandler<List<PoolLabel>> FireUpdatePoolSizesNow;
        public event EventHandler<String> FireGetMyStatus;

        public void HandleGetStatusNotification(object sender, string s)
        {
            FireGetMyStatus(this, ToString());
        }

        public void HandleServiceInitializationCompleteAtNotification(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(ServerlessService));
            ScheduleEvent(e);
        }

        public void HandleRequestWillArriveNotification(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(OpenLoopLoad));
            ScheduleEvent(e);
        }

        public void HandleRequestWillDepartAtNotification(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(ServerlessService));
            ScheduleEvent(e);
        }

        public void HandleScheduleHostRoleTransitionAt(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(ServerlessService) || sender.GetType() == typeof(HostRole));
            ScheduleEvent(e);
        }
        public void HandleSchedulePodStateTransitionAt(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(Pool));
            Debug.Assert(e.GetPod() != null);
            ScheduleEvent(e);
        }
        public void HandleOptimizerRunAtNotification(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(PoolOptimizer));
            ScheduleEvent(e);
        }
        public void HandleCollectStatsAtNotification(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(ServerlessService));
            ScheduleEvent(e);
        }
        public void HandleUpdatePoolSizesAtNotification(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(ServerlessService));
            ScheduleEvent(e);
        }
        public void HandleEndOfTraceNotification(object sender, SimEvent e)
        {
            Debug.Assert(sender.GetType() == typeof(OpenLoopLoad));
            ScheduleEvent(e);
        }
    }
}
