using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public struct PoolLabel : IEquatable<PoolLabel>
    {
        public AllocationLabel AllocationLabel;
        public double Cores;

        public PoolLabel(AllocationLabel pAllocationLabel, double pCores)
        {
            AllocationLabel = pAllocationLabel;
            Cores = pCores;
        }

        public readonly bool Equals(PoolLabel other)
        {
            return AllocationLabel.Equals(other.AllocationLabel) && Cores == other.Cores;
        }

        public override string ToString()
        {
            return String.Format("{0}|{1}|{2}", AllocationLabel.Runtime, AllocationLabel.RuntimeVersion, Cores);
        }
    }

    public class PoolParameters
    {
        internal readonly PoolGroupId PoolGroupId;
        internal readonly AllocationLabel AllocationLabel;
        internal readonly double Cores;
        internal int _minPodsCount;
        internal int _maxPodsCount;
        internal readonly int PredictionReferencePoolSize;
        internal readonly double PredictionReferenceRequestsCount;
        internal readonly SmoothedTrace? SmoothedTrace;
        internal int MaxExtraPods;
        internal PodLifeCycleDistributions? _lifeCycleDistributions;
        internal double ExtraCoresRatio;

        public PoolParameters(PoolGroupId pPoolGroupId, AllocationLabel pAllocationLabel, double pCore,
                                PodLifeCycleDistributions? pLifeCycleDistributions,
                                int pMinPodsCount, int pMaxPodsCount, int pMaxExtraPods, double pExtraCoresRatio,
                                int pPredictionReferencePoolSize, double pPredictionReferenceRequestsCount,
                                SmoothedTrace? pSmoothedTrace)
        {
            PoolGroupId = pPoolGroupId;
            AllocationLabel = pAllocationLabel;
            Cores = pCore;
            _minPodsCount = pMinPodsCount;
            _maxPodsCount = pMaxPodsCount;
            MaxExtraPods = pMaxExtraPods;
            ExtraCoresRatio = pExtraCoresRatio;
            _lifeCycleDistributions = pLifeCycleDistributions;
            PredictionReferencePoolSize = pPredictionReferencePoolSize;
            PredictionReferenceRequestsCount = pPredictionReferenceRequestsCount;
            SmoothedTrace = pSmoothedTrace;
        }
    }

    public class PoolStatistics
    {
        // requests counters
        internal int _totalRequestsCount;
        internal int _totalSucceededRequestsCount;
        internal int _totalFailedRequestsCount;
        internal int _totalPodCreationCount;
        internal int _failedPodCreationCount;
        internal int _windowRequestsCount;
        internal int _windowSucceededRequestsCount;
        internal int _windowFailedRequestsCount;

        internal DistributionEmpiricalFrequencyArray PoolUtilizationDistribution = new DistributionEmpiricalFrequencyArray();

        public void AssertRequestsCounters()
        {
            double total = _totalFailedRequestsCount + _totalSucceededRequestsCount;
            double totalWindow = _windowFailedRequestsCount + _windowSucceededRequestsCount;
            Debug.Assert(_totalFailedRequestsCount >= 0);
            Debug.Assert(_totalSucceededRequestsCount >= 0);
            Debug.Assert(_windowFailedRequestsCount >= 0);
            Debug.Assert(_windowSucceededRequestsCount >= 0);
            Debug.Assert(_totalRequestsCount >= 0);
            Debug.Assert(_windowRequestsCount >= 0);
            Debug.Assert(total == _totalRequestsCount);
            Debug.Assert(totalWindow == _windowRequestsCount);
        }
    }

    public class Pool
    {
        private readonly Simulator _simulator;
        private int _podIdCounter;
        public readonly PoolParameters _poolParameters;
        private readonly List<Pod> _createdPodList;
        private readonly List<Pod> _pendingPodList;
        private readonly List<Pod> _readyPodList;
        private readonly List<Pod> _allocatedPodList;
        private readonly List<Pod> _specializedPodList;
        private readonly List<Pod> _userWorkloadPodList;
        private readonly List<Pod> _deletedPodList;
        private readonly List<Pod> _beingRecycledPodList;
        private readonly List<Pod> _recycledPodList;
        private readonly ISimulationTimeReader _clock;
        private readonly Experiment _experiment;
        public PoolStatistics PoolStatistics;
        public RequestQueue RequestQueue;
        public PoolLabel PoolLabel;
        public int PredictionWindowCurrentIdx;
        public int OptimalPoolSizeWindowCurrentIdx;
        public event EventHandler<SimEvent> FireSchedulePodStateTransitionAt;

        public Pool(ISimulationTimeReader pSimulationTimeReaderdouble,
                    Simulator pSimulator,
                    PoolParameters pPoolParameters,
                    Experiment pExp,
                    PercentileResults pPercentileResults)
        {
            _simulator = pSimulator;
            _experiment = pExp;
            _podIdCounter = 0;
            PoolLabel = TraceLineFields.ConvertToPoolLabel(pPoolParameters.AllocationLabel.Runtime,
                                                                pPoolParameters.AllocationLabel.RuntimeVersion,
                                                                pPoolParameters.Cores);

            PoolStatistics = pPercentileResults.PoolLabelToPoolStatsMap[PoolLabel];
            RequestQueue = new RequestQueue(pSimulationTimeReaderdouble);
            _clock = pSimulationTimeReaderdouble;
            _poolParameters = pPoolParameters;
            _createdPodList = new List<Pod>();
            _pendingPodList = new List<Pod>();
            _readyPodList = new List<Pod>();
            _allocatedPodList = new List<Pod>();
            _specializedPodList = new List<Pod>();
            _userWorkloadPodList = new List<Pod>();
            _deletedPodList = new List<Pod>();
            _beingRecycledPodList = new List<Pod>();
            _recycledPodList = new List<Pod>();
            PredictionWindowCurrentIdx = 0;
            OptimalPoolSizeWindowCurrentIdx = 0;
        }

        public PodLifeCycleTimestamps GeneratePodLifeCycleFromDistributionsUsingMean(double timePointCreation)
        {
            Debug.Assert(_poolParameters._lifeCycleDistributions != null);

            PodLifeCycleTimestamps podLifeCycleTimestamps = new PodLifeCycleTimestamps(timePointCreation);
            podLifeCycleTimestamps._durationReady = 0;

            podLifeCycleTimestamps._durationCreation = _poolParameters._lifeCycleDistributions._supplyDelayDistribution.GetMean();
            podLifeCycleTimestamps._durationPending = 0;

            podLifeCycleTimestamps._durationAllocated = _poolParameters._lifeCycleDistributions._allocatedDemandDistribution.GetMean();
            podLifeCycleTimestamps._durationSpecialized = 0;

            podLifeCycleTimestamps._durationUserWorkload = _poolParameters._lifeCycleDistributions._userWorkloadDemandDistribution.GetMean();

            podLifeCycleTimestamps._durationDeleted = _poolParameters._lifeCycleDistributions._deleteRecycleDelayDistribution.GetMean();
            podLifeCycleTimestamps._durationRecyclingVm = 0;


            if (_experiment.IgnorePodTransitionsExceptCreation)
            {
                podLifeCycleTimestamps._durationPending = 0;
                podLifeCycleTimestamps._durationAllocated = 0;
                podLifeCycleTimestamps._durationSpecialized = 0;
                podLifeCycleTimestamps._durationUserWorkload = 0;
                podLifeCycleTimestamps._durationDeleted = 0;
                podLifeCycleTimestamps._durationRecyclingVm = 0;
            }

            podLifeCycleTimestamps.Assert();
            return podLifeCycleTimestamps;
        }

        public PodLifeCycleTimestamps GeneratePodLifeCycleFromDistributions(double timePointCreation)
        {
            Debug.Assert(_poolParameters._lifeCycleDistributions != null);
            var lifeCycle = _poolParameters._lifeCycleDistributions.SampleLifeCycle();
            lifeCycle._durationReady = 0;

            if (_experiment.IgnorePodTransitionsExceptCreation)
            {
                lifeCycle._durationAllocated = 0;
                lifeCycle._durationSpecialized = 0;
                lifeCycle._durationUserWorkload = 0;
                lifeCycle._durationDeleted = 0;
                lifeCycle._durationRecyclingVm = 0;
            }

            lifeCycle.Assert();

            PodLifeCycleTimestamps lifecycle2 = new PodLifeCycleTimestamps(
                timePointCreation,
                lifeCycle._durationCreation,
                lifeCycle._durationPending,
                lifeCycle._durationReady,
                lifeCycle._durationAllocated,
                lifeCycle._durationSpecialized,
                lifeCycle._durationUserWorkload,
                lifeCycle._durationDeleted,
                lifeCycle._durationRecyclingVm
            );

            return lifecycle2;
        }

        public Pod CreatePod(int pHostRoleId, PodLifeCycleTimestamps? podLifeCycleTimestamps)
        {
            Debug.Assert(podLifeCycleTimestamps != null || _poolParameters._lifeCycleDistributions != null, "no lifecycle distributions for " + PoolLabel);
            if (podLifeCycleTimestamps == null)
            {
                switch (_experiment.SamplingApproach)
                {
                    case SamplingApproach.Random:
                        podLifeCycleTimestamps = GeneratePodLifeCycleFromDistributions(_clock.Now);
                        break;
                    case SamplingApproach.Average:
                        podLifeCycleTimestamps = GeneratePodLifeCycleFromDistributionsUsingMean(_clock.Now);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            var newPod = new Pod(_clock, _podIdCounter, podLifeCycleTimestamps, _poolParameters.AllocationLabel, _poolParameters.Cores, pHostRoleId);
            _podIdCounter++;
            return newPod;
        }

        public Pod? AllocateOnePod(AllocationRequest request)
        {
            foreach (var pod in _readyPodList)
            {
                if (pod._myState == PodState.Ready)
                {
                    // pod.AdjustCores(request.Cores);
                    double pendingEndTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Pending);
                    Debug.Assert(_clock.Now >= pendingEndTimePoint, String.Format("Event in the future: now = {0}, event time = {1}, PID = {2}, lifecycle: {3}",
                                                                                        _clock.Now, pendingEndTimePoint, pod._id, pod.LifeCycleTimestamps));
                    double readyDuration = _clock.Now - pendingEndTimePoint;
                    Debug.Assert(readyDuration >= 0);
                    pod.LifeCycleTimestamps._durationReady = readyDuration;
                    HandlePodStateTransition(pod, PodState.Allocated);
                    request.PodId = pod._id;
                    PoolStatistics._totalRequestsCount++;
                    PoolStatistics._windowRequestsCount++;

                    if (_experiment.UseRequestsQueue && request.ArrivalTimePoint < _clock.Now)
                    {
                        PoolStatistics._windowFailedRequestsCount++;
                        PoolStatistics._totalFailedRequestsCount++;
                    }
                    else
                    {
                        PoolStatistics._windowSucceededRequestsCount++;
                        PoolStatistics._totalSucceededRequestsCount++;
                    }

                    return pod;
                }
            }
            if (_experiment.UseRequestsQueue == false)
            {
                PoolStatistics._totalRequestsCount++;
                PoolStatistics._windowRequestsCount++;
                PoolStatistics._windowFailedRequestsCount++;
                PoolStatistics._totalFailedRequestsCount++;
            }
            return null;
        }

        public void SchedulePodDeletion(Pod pod)
        {
            if (pod._myState == PodState.Ready || pod._myState == PodState.Pending || pod._myState == PodState.Created)
            {
                HandlePodStateTransition(pod, PodState.Deleted);
            }
        }

        public void DeletePods(int deletePodsCount)
        {
            int count = 0;

            for (int i = _createdPodList.Count - 1; i >= 0; i--)
            {
                Pod pod = _createdPodList[i];
                if (count >= deletePodsCount)
                {
                    break;
                }
                SchedulePodDeletion(pod);
                count++;
            }

            // delete pending pods first            
            for (int i = _pendingPodList.Count - 1; i >= 0; i--)
            {
                Pod pod = _pendingPodList[i];
                if (count >= deletePodsCount)
                {
                    break;
                }
                SchedulePodDeletion(pod);
                count++;
            }
            // delete ready pods 
            for (int i = _readyPodList.Count - 1; i >= 0; i--)
            {
                Pod pod = _readyPodList[i];
                if (count >= deletePodsCount)
                {
                    break;
                }
                SchedulePodDeletion(pod);
                count++;
            }
        }

        private void AdjustPodLifeCycle(Pod pod, double realDeleteTimePoint)
        {
            Debug.Assert(pod._lastStateBeforeDeleted == PodState.UserWorkload
                            || pod._lastStateBeforeDeleted == PodState.Pending
                            || pod._lastStateBeforeDeleted == PodState.Created
                            || pod._lastStateBeforeDeleted == PodState.Ready);

            switch (pod._lastStateBeforeDeleted)
            {
                case PodState.UserWorkload:
                    // no changes needed
                    break;
                case PodState.Created:
                    double creationStartTimePoint = pod.LifeCycleTimestamps._timePointCreation;
                    Debug.Assert(SimulationTime.Round(creationStartTimePoint) <= SimulationTime.Round(realDeleteTimePoint));
                    double spentInCreation = realDeleteTimePoint - creationStartTimePoint;
                    pod.LifeCycleTimestamps._durationCreation = spentInCreation;
                    pod.LifeCycleTimestamps._durationPending = 0;
                    pod.LifeCycleTimestamps._durationReady = 0;
                    pod.LifeCycleTimestamps._durationAllocated = 0;
                    pod.LifeCycleTimestamps._durationSpecialized = 0;
                    pod.LifeCycleTimestamps._durationUserWorkload = 0;
                    break;
                case PodState.Pending:
                    double pendingStartTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Created);
                    double pendingEndTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Pending);
                    Debug.Assert(SimulationTime.Round(pendingEndTimePoint) >= SimulationTime.Round(realDeleteTimePoint));
                    Debug.Assert(SimulationTime.Round(pendingStartTimePoint) <= SimulationTime.Round(realDeleteTimePoint));
                    double spentInPending = realDeleteTimePoint - pendingStartTimePoint;
                    pod.LifeCycleTimestamps._durationPending = spentInPending;
                    pod.LifeCycleTimestamps._durationReady = 0;
                    pod.LifeCycleTimestamps._durationAllocated = 0;
                    pod.LifeCycleTimestamps._durationSpecialized = 0;
                    pod.LifeCycleTimestamps._durationUserWorkload = 0;
                    break;
                case PodState.Ready:
                    double readyStartTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Pending);
                    pod.LifeCycleTimestamps._durationReady = realDeleteTimePoint - readyStartTimePoint;
                    double spentInReady = realDeleteTimePoint - readyStartTimePoint;
                    pod.LifeCycleTimestamps._durationReady = spentInReady;
                    pod.LifeCycleTimestamps._durationAllocated = 0;
                    pod.LifeCycleTimestamps._durationSpecialized = 0;
                    pod.LifeCycleTimestamps._durationUserWorkload = 0;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void HandlePodStateTransition(Pod pod, PodState nextState)
        {
            SimEvent? nextEvent;
            switch (nextState)
            {
                case PodState.Created:
                    Debug.Assert(pod._myState == PodState.Created);
                    _createdPodList.Add(pod);
                    double pendingTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(nextState);
                    nextEvent = _simulator.CreateEvent(EventType.PodBecomesPending, _clock.Now, pendingTimePoint, null, null, pod);
                    FireSchedulePodStateTransitionAt(this, nextEvent);
                    break;

                case PodState.Pending:
                    Debug.Assert(pod._myState == PodState.Deleted
                                    || pod._myState == PodState.BeingRecycled
                                    || pod._myState == PodState.Recycled
                                    || pod._myState == PodState.Created);

                    if (pod._myState != PodState.Created)
                    {
                        return;
                    }

                    _createdPodList.Remove(pod);
                    _pendingPodList.Add(pod);
                    double readyTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(nextState);
                    nextEvent = _simulator.CreateEvent(EventType.PodBecomesReady, _clock.Now, readyTimePoint, null, null, pod);
                    FireSchedulePodStateTransitionAt(this, nextEvent);
                    break;

                case PodState.Ready:
                    // a pod can be deleted while in Pending state and Ready transition already scheduled
                    Debug.Assert(pod._myState == PodState.Pending
                                    || pod._myState == PodState.Deleted
                                    || pod._myState == PodState.BeingRecycled
                                    || pod._myState == PodState.Recycled);

                    if (pod._myState != PodState.Pending) { return; }
                    var readyStartTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Pending);
                    Debug.Assert(_clock.Now >= readyStartTimePoint);
                    _pendingPodList.Remove(pod);
                    _readyPodList.Add(pod);
                    break;

                case PodState.Allocated:
                    // a pod can be deleted while in Ready state and Allocated transition already scehduled
                    Debug.Assert(pod._myState == PodState.Ready
                                    || pod._myState == PodState.Deleted
                                    || pod._myState == PodState.BeingRecycled
                                    || pod._myState == PodState.Recycled);

                    if (pod._myState != PodState.Ready) { return; }
                    _readyPodList.Remove(pod);
                    _allocatedPodList.Add(pod);
                    double specializedTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(nextState);
                    nextEvent = _simulator.CreateEvent(EventType.PodBecomesSpecialized, _clock.Now, specializedTimePoint, null, null, pod);
                    FireSchedulePodStateTransitionAt(this, nextEvent);
                    break;

                case PodState.Specialized:
                    Debug.Assert(pod._myState == PodState.Allocated);
                    _allocatedPodList.Remove(pod);
                    _specializedPodList.Add(pod);
                    double userWorkloadStartTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(nextState);
                    nextEvent = _simulator.CreateEvent(EventType.PodRunningUserWorkload, _clock.Now, userWorkloadStartTimePoint, null, null, pod);
                    FireSchedulePodStateTransitionAt(this, nextEvent);
                    break;

                case PodState.UserWorkload:
                    Debug.Assert(pod._myState == PodState.Specialized);
                    _specializedPodList.Remove(pod);
                    _userWorkloadPodList.Add(pod);
                    double deletedTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(nextState);
                    nextEvent = _simulator.CreateEvent(EventType.PodBecomesDeleted, _clock.Now, deletedTimePoint, null, null, pod);
                    if (_experiment.RecyclePodsSimulatorFlag)
                    {
                        FireSchedulePodStateTransitionAt(this, nextEvent);
                    }
                    break;

                case PodState.Deleted:
                    Debug.Assert(pod._myState == PodState.UserWorkload
                                || pod._myState == PodState.Pending
                                || pod._myState == PodState.Created
                                || pod._myState == PodState.Ready);
                    switch (pod._myState)
                    {
                        case PodState.UserWorkload:
                            _userWorkloadPodList.Remove(pod);
                            pod._lastStateBeforeDeleted = PodState.UserWorkload;
                            break;
                        case PodState.Pending:
                            _pendingPodList.Remove(pod);
                            pod._lastStateBeforeDeleted = PodState.Pending;
                            AdjustPodLifeCycle(pod, _clock.Now);
                            break;
                        case PodState.Created:
                            _createdPodList.Remove(pod);
                            pod._lastStateBeforeDeleted = PodState.Created;
                            AdjustPodLifeCycle(pod, _clock.Now);
                            break;
                        case PodState.Ready:
                            _readyPodList.Remove(pod);
                            pod._lastStateBeforeDeleted = PodState.Ready;
                            AdjustPodLifeCycle(pod, _clock.Now);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                    _deletedPodList.Add(pod);
                    double recyclingStartTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(nextState);
                    nextEvent = _simulator.CreateEvent(EventType.PodRecyclingStarts, _clock.Now, recyclingStartTimePoint, null, null, pod);
                    FireSchedulePodStateTransitionAt(this, nextEvent);
                    break;

                case PodState.BeingRecycled:
                    Debug.Assert(pod._myState == PodState.Deleted);
                    _deletedPodList.Remove(pod);
                    _beingRecycledPodList.Add(pod);
                    double recyclingCompletesTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(nextState);
                    nextEvent = _simulator.CreateEvent(EventType.PodBecomesRecycled, _clock.Now, recyclingCompletesTimePoint, null, null, pod);
                    FireSchedulePodStateTransitionAt(this, nextEvent);
                    break;

                case PodState.Recycled:
                    Debug.Assert(pod._myState == PodState.BeingRecycled);
                    _beingRecycledPodList.Remove(pod);
                    _recycledPodList.Add(pod);
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
            pod._myState = nextState;
            pod.LifeCycleTimestamps.Assert();
            pod.LifeCycleTimestamps.AssertLifeCycle();
            AssertPodCounters();
        }


        public int GetExtraPodsCount()
        {
            var count = _createdPodList.Count + _pendingPodList.Count + _readyPodList.Count - _poolParameters._minPodsCount;
            count = Math.Max(count, 0);
            return count;
        }

        public double GetExtraCoresCount()
        {
            double count = _createdPodList.Count + _pendingPodList.Count + _readyPodList.Count - _poolParameters._minPodsCount;
            count = Math.Max(count, 0);
            count = count * _poolParameters.Cores;
            return count;
        }

        public int GetReadyPodsCount()
        {
            return _readyPodList.Count();
        }

        public int GetAlivePodsCount()
        {
            return _podIdCounter - _deletedPodList.Count() - _beingRecycledPodList.Count() - _recycledPodList.Count() - _createdPodList.Count() - _pendingPodList.Count();
        }

        public double GetTotalCores()
        {
            int podsCount = _podIdCounter - _recycledPodList.Count();
            return podsCount * _poolParameters.Cores;
        }

        public double GetUserAllocatedCores()
        {
            int podsCount = _allocatedPodList.Count() + _specializedPodList.Count() + _userWorkloadPodList.Count();
            return podsCount * _poolParameters.Cores;
        }

        public int GetUserAllocatedPodsCount()
        {
            return _allocatedPodList.Count() + _specializedPodList.Count() + _userWorkloadPodList.Count();
        }

        public int GetFreePodsCount()
        {
            return _createdPodList.Count() + _pendingPodList.Count() + _readyPodList.Count();
        }
        public int GetPoolSize()
        {
            return _createdPodList.Count() + _pendingPodList.Count() + _readyPodList.Count();
        }

        public int GetTotalCreatedPodsCount()
        {
            return _podIdCounter;
        }

        public string GetStatsHeader()
        {
            return String.Format("{0}#{1},{0}#{2},{0}#{3},{0}#{4},{0}#{5},{0}#{6},{0}#{7},{0}#{8},{0}#{9},{0}#{10},{0}#{11},{0}#{12},{0}#{13},{0}#{14}",
                        TraceLineFields.ConvertToPoolLabel(_poolParameters.AllocationLabel.Runtime, _poolParameters.AllocationLabel.RuntimeVersion, _poolParameters.Cores),
                        "p-utilization", "win-requests", "win-succeeded", "win-failed", "p-total", "p-created",
                        "p-pending", "p-ready", "p-allocated", "p-specialized", "p-user", "p-deleted", "p-being-recycled", "p-recycled");
        }

        public string GetStats(bool collectWindowStats = true)
        {
            PoolStatistics.AssertRequestsCounters();
            var utilization = 100.0;
            if (_poolParameters._minPodsCount > 0)
            {
                utilization = 100 - Math.Min((double)_readyPodList.Count / _poolParameters._minPodsCount, 1.0) * 100.0;
            }
            PoolStatistics.PoolUtilizationDistribution.AddValue(utilization);
            var windowRequestCount = 0;
            var windowSucceededRequestsCount = 0;
            var windowFailedRequestsCount = 0;
            if (collectWindowStats)
            {
                windowRequestCount = PoolStatistics._windowRequestsCount;
                windowSucceededRequestsCount = PoolStatistics._windowSucceededRequestsCount;
                windowFailedRequestsCount = PoolStatistics._windowFailedRequestsCount;

                // reset window counters
                PoolStatistics._windowRequestsCount = 0;
                PoolStatistics._windowFailedRequestsCount = 0;
                PoolStatistics._windowSucceededRequestsCount = 0;
            }
            var str = String.Format("{0:0.00},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}",
                            utilization, windowRequestCount, windowSucceededRequestsCount,
                            windowFailedRequestsCount, _poolParameters._minPodsCount, _createdPodList.Count(),
                            _pendingPodList.Count(), _readyPodList.Count(), _allocatedPodList.Count(),
                            _specializedPodList.Count(), _userWorkloadPodList.Count(), _deletedPodList.Count(),
                            _beingRecycledPodList.Count(), _recycledPodList.Count());

            return str;
        }

        public void AssertPodCounters()
        {
            double total = _createdPodList.Count() + _pendingPodList.Count() + _readyPodList.Count()
                            + _allocatedPodList.Count() + _specializedPodList.Count()
                            + _userWorkloadPodList.Count() + _deletedPodList.Count()
                            + _beingRecycledPodList.Count() + _recycledPodList.Count();

            Debug.Assert(_createdPodList.Count() >= 0);
            Debug.Assert(_pendingPodList.Count() >= 0);
            Debug.Assert(_readyPodList.Count() >= 0);
            Debug.Assert(_allocatedPodList.Count() >= 0);
            Debug.Assert(_specializedPodList.Count() >= 0);
            Debug.Assert(_userWorkloadPodList.Count() >= 0);
            Debug.Assert(_deletedPodList.Count() >= 0);
            Debug.Assert(_beingRecycledPodList.Count() >= 0);
            Debug.Assert(_recycledPodList.Count() >= 0);
            Debug.Assert(total == _podIdCounter);
        }

    }
}