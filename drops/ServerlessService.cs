using System.Diagnostics;
using System.Net.Quic;
using System.Text.RegularExpressions;

namespace ServerlessPoolOptimizer
{
    public enum ServerlessState { Initializing, Stable, Expanding, Shrinking }

    public class ServerlessService
    {
        private StreamWriter? _statsWriter;
        private int _hostRoleIdCount;
        private readonly ISimulationTimeReader _clock;
        private readonly Simulator _simulator;
        private ServerlessState _myState;
        public readonly PoolGroup PoolGroup;
        public readonly IDictionary<AllocationLabel, SortedList<double, Pool>> _allocationLabelToPoolsMap;
        private readonly int _hostRoleInitialCount;
        private readonly List<HostRole> _pendingHostRoleList;
        private readonly List<HostRole> _readyHostRoleList;
        private readonly List<HostRole> _fullHostRoleList;
        private readonly List<HostRole> _beingDeletedHostRoleList;
        private readonly List<HostRole> _deletedHostRoleList;
        private readonly List<HostRole> _assignedHostRoleList;
        private readonly Experiment _experiment;
        private readonly double _targetPercentile;
        private readonly PercentileResults _percentileResults;
        public IDistribution? _hostRoleBootDemandDistribution;

        public IDictionary<PoolLabel, List<AllocationRequest>> _poolLabelToAllocationRequest;

        public ServerlessService(ISimulationTimeReader pSimulationTimeReader,
                                Simulator pSimulator,
                                Experiment pExp,
                                double pTargetPercentile,
                                int pHostRoleInitialCount,
                                IDistribution? pHostRoleBootDemandDistribution,
                                PoolGroupParameters pPoolGroupParameters,
                                String pStatsFilePath)
        {
            _myState = ServerlessState.Initializing;
            _simulator = pSimulator;
            _experiment = pExp;
            _targetPercentile = pTargetPercentile;
            _percentileResults = _experiment.Results.PercentileToResultsMap[_targetPercentile];
            _statsWriter = null;
            if (pStatsFilePath != null)
                _statsWriter = new StreamWriter(pStatsFilePath);
            _hostRoleBootDemandDistribution = pHostRoleBootDemandDistribution;
            _hostRoleIdCount = 0;
            _clock = pSimulationTimeReader;
            _hostRoleInitialCount = pHostRoleInitialCount;
            _pendingHostRoleList = new List<HostRole>();
            _readyHostRoleList = new List<HostRole>();
            _fullHostRoleList = new List<HostRole>();
            _beingDeletedHostRoleList = new List<HostRole>();
            _deletedHostRoleList = new List<HostRole>();
            _assignedHostRoleList = new List<HostRole>();
            _poolLabelToAllocationRequest = new Dictionary<PoolLabel, List<AllocationRequest>>();
            PoolGroup = new PoolGroup(pPoolGroupParameters, _clock, _simulator, _experiment, _percentileResults);
            _allocationLabelToPoolsMap = PoolGroup.RuntimeToPools;
            foreach (var (allocationLabel, pools) in _allocationLabelToPoolsMap)
            {
                foreach (var (size, pool) in pools)
                {
                    _poolLabelToAllocationRequest.Add(pool.PoolLabel, new List<AllocationRequest>());
                }
            }
        }

        public override string ToString()
        {
            var s = "";
            return s;
        }

        public event EventHandler<SimEvent> FireScheduleHostRoleStateTransitionAt;
        public event EventHandler<SimEvent> FireServiceInitializationCompleteAt;
        public event EventHandler<SimEvent> FireRequestWillDepartAt;
        public event EventHandler<SimEvent> FireCollectStatsAt;
        public event EventHandler<SimEvent> FireReactiveScaleDownPoolSizesAt;
        public event EventHandler<SimEvent> FireUpdatePoolSizesAt;
        public event EventHandler<String> FireGetMyStatus;
        public event EventHandler<bool> FireRunOptimizerNow;

        public List<HostRole> GetAssignedHostRolesToPoolGroup(PoolGroupId poolGroupId)
        {
            if (_experiment.AssignHostRolesToPoolGroup)
            {
                return _assignedHostRoleList;
            }
            else
            {
                return _readyHostRoleList;
            }
        }

        public List<HostRole> GetNonFullHostRoles()
        {
            if (_experiment.AssignHostRolesToPoolGroup)
            {
                return _assignedHostRoleList.FindAll(h => h.GetIdleCores() > 0);
            }
            else
            {
                return _readyHostRoleList.FindAll(h => h.GetIdleCores() > 0);
            }
        }

        public int GetReadyHostRolesCount()
        {
            return _readyHostRoleList.Count() + _fullHostRoleList.Count();
        }


        public void HandleRequestNowArrivesNotification(object sender, AllocationRequest? request)
        {
            Debug.Assert(sender is Simulator);
            if (request == null)
            {
                ProcessRequests();
                return;
            }

            Debug.Assert(request.PodId == -1);
            if (request.RequestType == RequestType.Deallocation)
            {
                return;
            }
            var runtimePools = _allocationLabelToPoolsMap[request.AllocationPoolGroupLabel];
            Debug.Assert(runtimePools != null);
            foreach (var (poolCores, pool) in runtimePools)
            {
                if (request.Cores > poolCores)
                {
                    continue;
                }
                pool.RequestQueue.AddRequest(request);
                break;
            }
            ProcessRequests();
        }

        public void ProcessRequests()
        {

            foreach (var (runtime, runtimePools) in PoolGroup.RuntimeToPools)
            {
                foreach (var (cores, pool) in runtimePools)
                {
                    ProcessPoolRequests(pool);
                }
            }
            if (_myState != ServerlessState.Initializing)
            {
                // WriteStats();
            }
            FireRunOptimizerNow(this, false);
        }

        public void ProcessPoolRequests(Pool pool)
        {
            var requestQueue = pool.RequestQueue;
            var poolLabel = pool.PoolLabel;
            while (true)
            {
                var request = requestQueue.GetFirstRequest();
                if (request == null)
                {
                    break;
                }

                bool isSuccess = ProcessOneRequest(request);
                if (isSuccess)
                {
                    requestQueue.RemoveRequest();
                    bool requestSucceeded = true;
                    if (_experiment.UseRequestsQueue)
                    {
                        requestSucceeded = _clock.Now == request.ArrivalTimePoint;
                    }
                    else
                    {
                        Debug.Assert(_clock.Now == request.ArrivalTimePoint);
                    }

                    if (!requestSucceeded
                        && (_experiment.PoolOptimizationMethod == PoolOptimizationMethod.Reactive
                            || _experiment.PoolOptimizationMethod == PoolOptimizationMethod.PredictiveReactive)
                        )
                    {
                        pool._poolParameters._minPodsCount = (int)Math.Ceiling(pool._poolParameters._minPodsCount * _experiment.ReactiveScalingUpFactor);
                        pool._poolParameters._minPodsCount = Math.Min(pool._poolParameters._minPodsCount, Parameter.ReactiveMaxPoolSize);
                        // Console.WriteLine("Pool {0} expanded to size {1}", pool._poolParameters.AllocationLabel, pool._poolParameters._minPodsCount);
                    }

                    var latency = _clock.Now - request.ArrivalTimePoint;
                    Debug.Assert(latency >= 0);
                    _percentileResults.RequestLatencyDistribution.AddValue(latency);

                    request.ProcessingComplete(_clock.Now, requestSucceeded);
                    SimEvent newEvent = _simulator.CreateEvent(EventType.RequestDepart, _clock.Now, _clock.Now, request, null, null);
                    FireRequestWillDepartAt(this, newEvent);
                }
                else if (_experiment.UseRequestsQueue == false)
                {
                    requestQueue.RemoveRequest();
                    var latency = _clock.Now - request.ArrivalTimePoint;
                    Debug.Assert(latency >= 0);
                    _percentileResults.RequestLatencyDistribution.AddValue(latency);
                    request.ProcessingComplete(_clock.Now, false);
                    SimEvent newEvent = _simulator.CreateEvent(EventType.RequestDepart, _clock.Now, _clock.Now, request, null, null);
                    FireRequestWillDepartAt(this, newEvent);
                }
                else
                {
                    break;
                }
            }
        }

        public bool ProcessOneRequest(AllocationRequest request)
        {
            Pod? allocatedPod = null;
            var runtimePools = _allocationLabelToPoolsMap[request.AllocationPoolGroupLabel];
            Debug.Assert(runtimePools != null);
            // try to allocate pods from the smallest pool to the largest
            Pool? allocatedPool = null;
            foreach (var (poolCores, pool) in runtimePools)
            {
                if (request.Cores > poolCores)
                {
                    continue;
                }
                else if (_experiment.StaticPoolSizesMap == null && request.Cores != poolCores)
                {
                    continue;
                }
                allocatedPod = pool.AllocateOnePod(request);
                if (allocatedPod != null)
                {
                    allocatedPool = pool;
                    break;
                }
            }
            if (allocatedPod != null)
            {
                // allocation was successful
                return true;
            }
            return false;
        }

        public bool HasQueuedRequests()
        {
            foreach (var (runtime, runtimePools) in PoolGroup.RuntimeToPools)
            {
                foreach (var (cores, pool) in runtimePools)
                {
                    if (pool.RequestQueue.Count() > 0)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public int QueuedRequestsCount()
        {
            var count = 0;
            foreach (var (runtime, runtimePools) in PoolGroup.RuntimeToPools)
            {
                foreach (var (cores, pool) in runtimePools)
                {
                    count += pool.RequestQueue.Count(); 
                }
            }
            return count;
        }

        public void ScheduleHostedPodsDeletion(HostRole hostrole)
        {
            foreach (var pod in hostrole.HostedPods)
            {
                AllocationLabel allocationLabel = pod._allocationLabel;
                Pool pool = _allocationLabelToPoolsMap[allocationLabel][pod._poolCores];
                pool.SchedulePodDeletion(pod);
            }
        }

        public void HandleHostRoleStateTransitionNotification(object sender, (HostRole, HostRoleState) hostRoleTransitionTuple)
        {
            HostRole hostRole = hostRoleTransitionTuple.Item1;
            HostRoleState nextState = hostRoleTransitionTuple.Item2;
            switch (nextState)
            {
                case HostRoleState.Pending:
                    _pendingHostRoleList.Add(hostRole);
                    hostRole.SetState(nextState);
                    break;
                case HostRoleState.Ready:
                    Debug.Assert(hostRole.MyState == HostRoleState.Pending);
                    _pendingHostRoleList.Remove(hostRole);
                    _readyHostRoleList.Add(hostRole);
                    ServiceTransitionToStableState();
                    hostRole.SetState(nextState);
                    if (_myState != ServerlessState.Initializing)
                    {
                        FireRunOptimizerNow(this, false);
                    }
                    break;
                case HostRoleState.BeingDeleted:
                    _readyHostRoleList.Remove(hostRole);
                    _beingDeletedHostRoleList.Add(hostRole);
                    hostRole.SetState(nextState);
                    break;
                case HostRoleState.Deleted:
                    _beingDeletedHostRoleList.Remove(hostRole);
                    _deletedHostRoleList.Add(hostRole);
                    ServiceTransitionToStableState();
                    hostRole.SetState(nextState);
                    break;
            }
        }

        public Pod? CreatePod(Pool pool, bool collectStat)
        {
            var podCores = pool._poolParameters.Cores;
            var poolGroupId = pool._poolParameters.PoolGroupId;
            // find a host role to host the pod
            // var hostRolesList = GetAssignedHostRolesToPoolGroup(poolGroupId);
            var hostRolesList = GetNonFullHostRoles();
            var hostRolesSortedBasedOnUtilization = hostRolesList.OrderBy(o => o.GetIdleCores()).ToList();
            if (hostRolesList.Count == 0)
            {
                if (collectStat)
                {
                    pool.PoolStatistics._totalPodCreationCount++;
                    pool.PoolStatistics._failedPodCreationCount++;
                }
                return null;
            }
            Pod? pod = null;
            for (int i = 0; i < hostRolesSortedBasedOnUtilization.Count; i++)
            {
                var hostRole = hostRolesSortedBasedOnUtilization[i];
                if (hostRole.IsReady() && hostRole.HasCapacity(podCores))
                {
                    pod = pool.CreatePod(hostRole._id, null);
                    if (_myState == ServerlessState.Initializing)
                    {
                        pod.LifeCycleTimestamps._durationCreation = 0;
                        pod.LifeCycleTimestamps._durationPending = 0;
                    }
                    pool.HandlePodStateTransition(pod, PodState.Created);
                    hostRole.PlacePod(pod);
                    if (hostRole.GetIdleCores() == 0)
                    {
                        _fullHostRoleList.Add(hostRole);
                        _readyHostRoleList.Remove(hostRole);
                    }
                    break;
                }
            }
            if (collectStat)
            {
                pool.PoolStatistics._totalPodCreationCount++;
            }
            if (pod == null)
            {
                if (collectStat)
                {
                    pool.PoolStatistics._failedPodCreationCount++;
                }
            }

            return pod;
        }

        public int CreatePods(Pool pool, int newPodsCount, bool collectStat)
        {
            for (int i = 0; i < newPodsCount; i++)
            {
                Pod? pod = CreatePod(pool, collectStat);
                if (pod == null)
                {
                    return i;
                }
            }
            return newPodsCount;
        }

        public void DeletePods(Pool pool, int deletePodsCount)
        {
            pool.DeletePods(deletePodsCount);
        }

        public void HandlePodStateTransitionNotification(object sender, (Pod, PodState) PodNextStateTuple)
        {
            Debug.Assert(sender.GetType() == typeof(Simulator));
            Pod pod = PodNextStateTuple.Item1;
            PodState nextState = PodNextStateTuple.Item2;
            Pool pool = _allocationLabelToPoolsMap[pod._allocationLabel][pod._poolCores];
            Debug.Assert(pool != null);
            pool.HandlePodStateTransition(pod, nextState);
            if (nextState == PodState.Recycled)
            {
                var hostRole = _readyHostRoleList.Find(h => h._id == pod._hostRoleId);
                var isHostRoleFull = false;
                if (hostRole == null)
                {
                    hostRole = _fullHostRoleList.Find(h => h._id == pod._hostRoleId);
                    isHostRoleFull = true;
                }
                if (hostRole == null)
                {
                    hostRole = _beingDeletedHostRoleList.Find(h => h._id == pod._hostRoleId);
                    isHostRoleFull = false;
                }
                Debug.Assert(hostRole != null);
                hostRole.DeallocatePod(pod);
                if (isHostRoleFull)
                {
                    _fullHostRoleList.Remove(hostRole);
                    _readyHostRoleList.Add(hostRole);
                }
                if (_experiment.PoolOptimizationMethod != PoolOptimizationMethod.Reactive
                    && _experiment.PoolOptimizationMethod != PoolOptimizationMethod.PredictiveReactive
                    )
                {
                    FireRunOptimizerNow(this, false);
                }
            }
            if (_myState != ServerlessState.Initializing)
            {
                // WriteStats();
            }
            if (nextState == PodState.Ready)
            {
                ProcessRequests();
            }
        }

        public void AssignHostRolesToPoolGroup(PoolGroupId poolGroupId, int hostRolesCount)
        {
            int count = 0;
            foreach (var hostrole in _readyHostRoleList)
            {
                if (hostrole.AssignToPoolGroup(PoolGroup.PoolGroupParameters.PoolGroupId))
                {
                    _assignedHostRoleList.Add(hostrole);
                    count++;
                    if (count >= hostRolesCount)
                    {
                        break;
                    }
                }
            }
        }

        public void LegionDeleteHostRoles(int deleteHostRolesCount)
        {
            // if (_myState != ServerlessState.Stable)
            // {
            //     return;
            // }
            int count = 0;
            for (int i = _pendingHostRoleList.Count - 1; i >= 0; i--)
            {
                if (count >= deleteHostRolesCount)
                {
                    break;
                }
                count++;
                var hostRole = _pendingHostRoleList[i];
                SetServiceState(ServerlessState.Shrinking);
                _pendingHostRoleList.Remove(hostRole);
                _deletedHostRoleList.Add(hostRole);
                hostRole.SetState(HostRoleState.Ready);
                hostRole.SetState(HostRoleState.BeingDeleted);
                hostRole.SetState(HostRoleState.Deleted);
            }

            List<HostRole> hostRolesSortedBasedOnUtilization = _readyHostRoleList.OrderBy(o => o.GetIdleCores()).ToList();
            for (int i = hostRolesSortedBasedOnUtilization.Count - 1; i >= 0; i--)
            {
                if (count >= deleteHostRolesCount)
                {
                    break;
                }
                count++;
                HostRole hostrole = hostRolesSortedBasedOnUtilization[i];
                Debug.Assert(hostrole.IsReady());
                SetServiceState(ServerlessState.Shrinking);
                HandleHostRoleStateTransitionNotification(this, (hostrole, HostRoleState.BeingDeleted));
                hostrole.Delete();
                if (!hostrole.IsDeleted())
                {
                    ScheduleHostedPodsDeletion(hostrole);
                }

            }
        }

        public void CreateHostRoles(int newHostRolesCount)
        {

            for (int i = 0; i < newHostRolesCount; i++)
            {
                _percentileResults.HostRoleAllocationTrace.Add((_clock.Now, 1));

                double hostRoleBootDemand = 0.0;
                if (_experiment.SamplingApproach == SamplingApproach.Average)
                {
                    hostRoleBootDemand = _hostRoleBootDemandDistribution.GetMean();
                }
                else if (_experiment.SamplingApproach == SamplingApproach.Random)
                {
                    hostRoleBootDemand = _hostRoleBootDemandDistribution.GetSample();

                }
                HostRole newHostRole = new HostRole(_hostRoleIdCount++, _clock,
                                                    _experiment.HostRoleCores,
                                                    _experiment.MaxPodsPerHostRole,
                                                    hostRoleBootDemand,
                                                    _experiment.HostRoleReservedCores,
                                                    HandleHostRoleStateTransitionNotification);

                HandleHostRoleStateTransitionNotification(this, (newHostRole, HostRoleState.Pending));
                SetServiceState(ServerlessState.Expanding);
                var nextEvent = _simulator.CreateEvent(EventType.HostRoleBecomesReady, _clock.Now, _clock.Now + hostRoleBootDemand, null, newHostRole, null);
                FireScheduleHostRoleStateTransitionAt(this, nextEvent);
            }
        }

        public void HandleInitializeServiceNowNotification(object sender)
        {
            Debug.Assert(sender is Simulator);
            PrintStatsHeader();
            for (var i = 0; i < _hostRoleInitialCount; i++)
            {
                // assume these host roles are ready (created and initialized)
                HostRole newHostRole = new HostRole(_hostRoleIdCount++, _clock, _experiment.HostRoleCores,
                                                        _experiment.MaxPodsPerHostRole, 0.0, _experiment.HostRoleReservedCores,
                                                        HandleHostRoleStateTransitionNotification);

                HandleHostRoleStateTransitionNotification(this, (newHostRole, HostRoleState.Pending));
                HandleHostRoleStateTransitionNotification(this, (newHostRole, HostRoleState.Ready));
            }

            AssignHostRolesToPoolGroup(PoolGroup.PoolGroupParameters.PoolGroupId, PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount);

            var pointTimeServiceInitializationComplete = _clock.Now;
            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    for (int i = 0; i < pool._poolParameters._minPodsCount; i++)
                    {
                        Pod? pod = CreatePod(pool, false);
                        if (pod == null)
                        {
                            break;
                            // throw new Exception("No enough host roles to initialize the service!!!");
                        }
                        double podReadyStartTimePoint = pod.LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Pending);
                        if (pointTimeServiceInitializationComplete < podReadyStartTimePoint)
                        {
                            pointTimeServiceInitializationComplete = podReadyStartTimePoint;
                        }
                    }
                }
            }
            FireRunOptimizerNow(this, false);
            SimEvent newEvent = _simulator.CreateEvent(EventType.ServiceInitializationComplete, _clock.Now, pointTimeServiceInitializationComplete, null, null, null);
            FireServiceInitializationCompleteAt(this, newEvent);
        }

        public void HandleServiceInitializationNowComplete(object sender, EventArgs e)
        {
            _myState = ServerlessState.Stable;
            if (_experiment.PoolOptimizationMethod == PoolOptimizationMethod.Reactive
                || _experiment.PoolOptimizationMethod == PoolOptimizationMethod.PredictiveReactive)
            {
                HandleReactiveScaleDownPoolSizes(null, false);
            }
        }

        private void SetServiceState(ServerlessState newState)
        {
            if (_myState == ServerlessState.Initializing)
            {
                return;
            }
            _myState = newState;
        }

        private void ServiceTransitionToStableState()
        {
            if (_myState == ServerlessState.Stable || _myState == ServerlessState.Initializing)
            {
                return;
            }
            if (_pendingHostRoleList.Count() != 0 || _beingDeletedHostRoleList.Count() != 0)
            {
                return;
            }
            SetServiceState(ServerlessState.Stable);
        }

        public void FinishExperiment()
        {
            WriteStats();
            if (_statsWriter != null)
            {
                _statsWriter.Close();
            }
            SetHostRolesForCostComputation();
            string filePath = _experiment.ResultPath +
                                        Experiment.GetCoreHourFileName(_experiment.Id,
                                                                            _experiment.ExpName,
                                                                            _targetPercentile);

            MeasureHostRoleStateTimeBreaker(filePath, _percentileResults, null);
            PrintResultsSummary();
        }


        public void SetHostRolesForCostComputation()
        {
            _percentileResults.HostRolesList =
                [
                    .. _pendingHostRoleList,
                    .. _fullHostRoleList,
                    .. _readyHostRoleList,
                    .. _beingDeletedHostRoleList,
                    .. _deletedHostRoleList,
                ];

            Debug.Assert(_percentileResults.HostRolesList.Count == _hostRoleIdCount);
        }


        public static HostRoleStateTimeTracker MeasureHostRoleStateTimeBreaker(string filePath,
                                                            PercentileResults percentileResults,
                                                            HostRoleStateTimeTracker? referenceHostRoleStateTimeTracker
                                                            )
        {
            try
            {
                StreamWriter outputFile = new StreamWriter(filePath);
                string str = HostRoleStateTimeTracker.GetPrintHeader(true);
                str = HostRoleStateTimeTracker.GetPrintHeader(false);
                outputFile.WriteLine(str);
                HostRoleStateTimeTracker aggregateHostRoleStateTimeTracker = new HostRoleStateTimeTracker(-1);

                foreach (var hostrole in percentileResults.HostRolesList)
                {
                    hostrole.PopulateHostRoleStateTimeTracker();
                    aggregateHostRoleStateTimeTracker.Add(hostrole.HostRoleStateTimeTracker);
                    str = hostrole.HostRoleStateTimeTracker.ToString(isForTerminal: true);
                    str = hostrole.HostRoleStateTimeTracker.ToString(isForTerminal: false);
                }

                if (referenceHostRoleStateTimeTracker != null)
                {
                    aggregateHostRoleStateTimeTracker.HostRoleUnallocatedCores -= aggregateHostRoleStateTimeTracker.HostRoleTotalTime - referenceHostRoleStateTimeTracker.HostRoleTotalTime;
                }

                str = aggregateHostRoleStateTimeTracker.ToString(isForTerminal: true);
                str = aggregateHostRoleStateTimeTracker.ToString(isForTerminal: false);
                outputFile.WriteLine(str);
                outputFile.Close();
                var hostRoleAllocatedCoreHour = aggregateHostRoleStateTimeTracker.HostRoleTotalTime;
                percentileResults.TotalCoreHour = hostRoleAllocatedCoreHour;
                

                percentileResults.PodsTotalCoreHour = aggregateHostRoleStateTimeTracker.PodCreation
                                                        + aggregateHostRoleStateTimeTracker.PodPending
                                                        + aggregateHostRoleStateTimeTracker.PodReady
                                                        + aggregateHostRoleStateTimeTracker.PodAllocated
                                                        + aggregateHostRoleStateTimeTracker.PodSpecialization
                                                        + aggregateHostRoleStateTimeTracker.PodUserWorkload
                                                        + aggregateHostRoleStateTimeTracker.PodDeletion
                                                        + aggregateHostRoleStateTimeTracker.PodRecycling;

                percentileResults.PoolTotalCoreHour = aggregateHostRoleStateTimeTracker.PodCreation
                                                            + aggregateHostRoleStateTimeTracker.PodPending
                                                            + aggregateHostRoleStateTimeTracker.PodReady;
                return aggregateHostRoleStateTimeTracker;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            return null;

        }

        public double GetTotalAssignedCores()
        {
            var totalPodsCores = 0.0;
            foreach (var (allocationLabel, poolGroup) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in poolGroup)
                {
                    totalPodsCores += pool.GetTotalCores();
                }
            }
            return totalPodsCores;
        }

        public double GetTotalPodsPoolsUtilization()
        {
            var maximumPodsCount = 0.0;
            var currentPodsCount = 0.0;
            foreach (var (allocationLabel, poolGroup) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in poolGroup)
                {
                    maximumPodsCount += pool._poolParameters._minPodsCount;
                    currentPodsCount += pool.GetReadyPodsCount();
                }
            }

            var utilization = 1 - currentPodsCount / maximumPodsCount;
            return utilization;
        }

        public double GetTotalIdleCores()
        {
            var idlesCores = GetIdleHostRoleCores();
            foreach (var (allocationLabel, poolGroup) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in poolGroup)
                {
                    idlesCores += pool.GetPoolSize() * pool._poolParameters.Cores;
                }
            }

            return idlesCores;
        }

        public double GetMaximumIdleCores()
        {
            double maximumIdleCoresCount = 0.0;
            foreach (var (allocationLabel, poolGroup) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in poolGroup)
                {
                    maximumIdleCoresCount += pool._poolParameters._minPodsCount * pool._poolParameters.Cores;
                }
            }

            switch (_experiment.HostRoleOptimizationMethod)
            {
                case HostRoleOptimizationMethod.Production:
                    var hostRolesNeededForQueuedRequests = GetRequiredHostRolesForQueuedRequests();
                    var assignedCoresCount = GetTotalAssignedCores();
                    assignedCoresCount = assignedCoresCount + hostRolesNeededForQueuedRequests * _experiment.HostRoleCores;
                    maximumIdleCoresCount = maximumIdleCoresCount + (assignedCoresCount * _experiment.ExpandHostRolesUtilizationThreshold);
                    break;

                default:
                    maximumIdleCoresCount = maximumIdleCoresCount + (_hostRoleInitialCount * _experiment.HostRoleCores);
                    break;
            }

            return maximumIdleCoresCount;
        }

        public double GetTotalIdleResourceUtilization()
        {
            double maxIdleCores = GetMaximumIdleCores();
            double currentIdleCores = GetTotalIdleCores();
            double utilization = 1 - (currentIdleCores / maxIdleCores);
            return utilization;
        }


        public double GetHostRolesUtilization(PoolGroupId? poolGroupId)
        {
            var totalCores = 0.0;
            var totalUserAllocatedCores = 0.0;
            var idleCores = 0.0;
            var hostrolesUtilization = 0.0;

            foreach (var hostrole in _fullHostRoleList)
            {
                if (poolGroupId == null || (poolGroupId != null && hostrole.IsAssignedToPoolGroup((PoolGroupId)poolGroupId)))
                {
                    totalCores += hostrole.GetTotalCores();
                    idleCores += hostrole.GetIdleCores();
                }
            }

            foreach (var hostrole in _readyHostRoleList)
            {
                if (poolGroupId == null || (poolGroupId != null && hostrole.IsAssignedToPoolGroup((PoolGroupId)poolGroupId)))
                {
                    totalCores += hostrole.GetTotalCores();
                    idleCores += hostrole.GetIdleCores();
                }
            }

            switch (_experiment.PoolOptimizationMethod)
            {
                case PoolOptimizationMethod.DROPS:
                    var idleHostRolesCount = idleCores / _experiment.HostRoleCores;
                    hostrolesUtilization = 1 - idleHostRolesCount / _hostRoleInitialCount;
                    hostrolesUtilization = Math.Max(hostrolesUtilization, 0);
                    hostrolesUtilization = Math.Min(hostrolesUtilization, 1);
                    break;

                default:

                    hostrolesUtilization = 1.0 - idleCores / totalCores;
                    hostrolesUtilization = Math.Max(hostrolesUtilization, 0);
                    hostrolesUtilization = Math.Min(hostrolesUtilization, 1);
                    break;
            }

            return hostrolesUtilization;
        }

        public string CollectStats(bool collectWindowStats = true)
        {
            var totalReadyHostRolesCount = _readyHostRoleList.Count + _fullHostRoleList.Count;
            var idleHostRolesCount = GetIdleHostRolesCount();
            var nonFullHostRoles = GetIdleHostRolesCount();
            var coresUtilization = GetHostRolesUtilization(null) * 100.0;
            var totalIdleResourceUtilization = GetTotalIdleResourceUtilization() * 100.0;

            var podCoreUtilization = GetTotalPodsPoolsUtilization() * 100.0;

            _percentileResults.HostRoleUtilizationStats.AddValue(coresUtilization);
            _percentileResults.PodCoreUtilizationStats.AddValue(podCoreUtilization);
            _percentileResults.HostRolesCountDistribution.AddValue(totalReadyHostRolesCount);
            _percentileResults.NonFullHostRolesCountDistribution.AddValue(idleHostRolesCount);

            string str = String.Format("{0:0.00},{1:0.00},{2:0.00},{3:0.00},{4},{5}",
                            _clock.Now, totalIdleResourceUtilization, coresUtilization, podCoreUtilization,
                            nonFullHostRoles, nonFullHostRoles + _pendingHostRoleList.Count);

            foreach (var (allocationLabel, poolGroup) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in poolGroup)
                {
                    str += "," + pool.GetStats(collectWindowStats);
                }
            }
            return str;
        }
        public void PrintStatsHeader()
        {
            string header = "clock,p_c_utilization,c-utilization,p-utilization,#hostroles,#nonfull-hostroles";
            foreach (var (allocationLabel, poolGroup) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in poolGroup)
                {
                    header += "," + pool.GetStatsHeader();
                }
            }
            if (_statsWriter != null)
            {
                _statsWriter.WriteLine(header);
            }
        }
        public void ScheduleCollectStatsEvent()
        {
            SimEvent newEvent = _simulator.CreateEvent(EventType.CollectStats, _clock.Now,
                                _clock.Now + _experiment.CollectStatsFrequency,
                                null, null, null);
            FireCollectStatsAt(this, newEvent);
        }

        public void PrintResultsSummary()
        {
            Console.WriteLine("Simulation Completes - Results Summary");
            Console.WriteLine("{0,-25} {1,-20} {2,-20}", "Pool", "Total Requests", "Failed Requests");
            Console.WriteLine("{0,-25} {0,-20} {0,-20}", "======");
            foreach (var (allocationLabel, poolGroup) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in poolGroup)
                {
                    Console.WriteLine("{0, -25} {1, -20} {2, -20}",
                        TraceLineFields.ConvertToPoolLabel(allocationLabel.Runtime, allocationLabel.RuntimeVersion, poolCores),
                        pool.PoolStatistics._totalRequestsCount,
                        Utilities.GetValuePercentageStr(pool.PoolStatistics._totalFailedRequestsCount,
                                                        pool.PoolStatistics._totalRequestsCount)
                    );
                }
            }
        }

        public bool ShouldUpdatePoolSize()
        {
            return (_experiment.PoolOptimizationMethod == PoolOptimizationMethod.PredictionPoissonLoad
                    || _experiment.PoolOptimizationMethod == PoolOptimizationMethod.PredictionConstantLoad
                    || _experiment.PoolOptimizationMethod == PoolOptimizationMethod.PredictionConcentratedLoad
                    || _experiment.PoolOptimizationMethod == PoolOptimizationMethod.PredictiveReactive
                );
        }

        public void UpdatePoolSizesPrediction(List<PoolLabel>? poolLabels)
        {

            if (_myState == ServerlessState.Initializing)
            {
                return;
            }

            if (!ShouldUpdatePoolSize())
            {
                return;
            }

            var nextWindowPodsCount = 0.0;

            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (cores, pool) in runtimePools)
                {
                    if (poolLabels != null && !poolLabels.Contains(pool.PoolLabel))
                    {
                        continue;
                    }
                    if (pool.PredictionWindowCurrentIdx >= pool._poolParameters.SmoothedTrace.Trace.Count)
                    {
                        Console.WriteLine("Short Predicted trace. Current interval is {0}, trace length: {1}", pool.PredictionWindowCurrentIdx, pool._poolParameters.SmoothedTrace.Trace.Count);
                    }
                    var predictedRequestsCount = pool._poolParameters.SmoothedTrace.Trace[pool.PredictionWindowCurrentIdx].Item2;
                    var creationDelay = pool._poolParameters._lifeCycleDistributions._supplyDelayDistribution.GetMean();
                    var currentWindowPodsCount = 1.0;
                    var minPodsCount = 1;
                    switch (_experiment.PoolOptimizationMethod)
                    {
                        case PoolOptimizationMethod.PredictionConcentratedLoad:
                            currentWindowPodsCount = predictedRequestsCount * _experiment.PredictionWindowSize;
                            currentWindowPodsCount = Math.Max(currentWindowPodsCount, minPodsCount);
                            break;

                        case PoolOptimizationMethod.PredictionConstantLoad:
                            currentWindowPodsCount = predictedRequestsCount * creationDelay * _targetPercentile;
                            currentWindowPodsCount = Math.Max(currentWindowPodsCount, minPodsCount);
                            break;

                        case PoolOptimizationMethod.PredictiveReactive:
                            double previousWindowRequests = pool.PoolStatistics._windowRequestsCount;
                            var previousPoolSize = pool._poolParameters._minPodsCount;
                            var nextWindowRequestsCount = Math.Ceiling(predictedRequestsCount * _experiment.PredictionWindowSize);
                            if (nextWindowRequestsCount == 0)
                            {
                                currentWindowPodsCount = Parameter.ReactiveMinPoolSize;
                            }
                            else if (previousWindowRequests == 0 && nextWindowRequestsCount != 0)
                            {
                                currentWindowPodsCount = nextWindowRequestsCount;
                            }
                            else
                            {
                                currentWindowPodsCount = nextWindowRequestsCount;
                            }
                            break;

                        case PoolOptimizationMethod.PredictionPoissonLoad:
                            var epsilon = 1.0 - _targetPercentile;
                            if (epsilon == 0)
                            {
                                epsilon = 0.000001;
                            }
                            if (predictedRequestsCount <= 0)
                            {
                                currentWindowPodsCount = 0;
                            }
                            else
                            {
                                currentWindowPodsCount = AnalysisHelper.ComputePoissonPoolSize(predictedRequestsCount, creationDelay, epsilon);
                            }
                            currentWindowPodsCount = Math.Max(currentWindowPodsCount, minPodsCount);
                            break;
                    }
                    pool._poolParameters._minPodsCount = (int)Math.Ceiling(currentWindowPodsCount);
                    switch (_experiment.PoolOptimizationMethod)
                    {
                        case PoolOptimizationMethod.PredictionConstantLoad:
                        case PoolOptimizationMethod.PredictionPoissonLoad:
                        case PoolOptimizationMethod.PredictionConcentratedLoad:
                        case PoolOptimizationMethod.PredictiveReactive:
                            if (pool._poolParameters.SmoothedTrace.Trace.Count - 1 > pool.PredictionWindowCurrentIdx)
                            {
                                pool.PredictionWindowCurrentIdx++;
                            }
                            break;
                    }
                }
            }

            FireRunOptimizerNow(this, false);
            SimEvent newEvent = _simulator.CreateEvent(EventType.UpdatePoolSizes, _clock.Now,
                            _clock.Now + _experiment.PredictionWindowSize,
                            null, null, null);
            FireUpdatePoolSizesAt(this, newEvent);
        }

        public void HandleReactiveScaleDownPoolSizes(object sender, bool scaleDownVmPool = false)
        {
            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (cores, pool) in runtimePools)
                {
                    pool._poolParameters._minPodsCount = pool._poolParameters._minPodsCount - 1;
                    if (pool._poolParameters._minPodsCount < Parameter.ReactiveMinPoolSize)
                    {
                        pool._poolParameters._minPodsCount = Parameter.ReactiveMinPoolSize;
                    }
                }
            }

            if (scaleDownVmPool)
            {
                if (PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount > Parameter.ReactiveMinVmPoolSize)
                {
                    PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount = PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount - 1;
                }
            }

            SimEvent newEvent = _simulator.CreateEvent(EventType.ReactiveUpdatePoolSize, _clock.Now,
                                _clock.Now + _experiment.ReactiveScalingDownFactor,
                                null, null, null);
            FireReactiveScaleDownPoolSizesAt(this, newEvent);
        }


        public void HandleUpdatePoolSizesNotification(object sender, List<PoolLabel> poolLabels)
        {
            UpdatePoolSizesPrediction(poolLabels);
        }

        public void HandleCollectStatsNotification(object sender, EventArgs e)
        {
            WriteStats();
            ScheduleCollectStatsEvent();
        }

        public void WriteStats()
        {
            string statsString = CollectStats();
            if (_statsWriter == null)
            {
                return;
            }
            _statsWriter.WriteLine(statsString);
        }

        public double GetIdleHostRoleCores()
        {
            var hostRolesList = _readyHostRoleList.FindAll(h => h.GetIdleCores() > 0);
            double totalCores = 0;
            foreach (var hostRole in hostRolesList)
            {
                totalCores += hostRole.GetIdleCores();
            }
            return totalCores;
        }

        public double GetIdlePendingHostRoleCores()
        {
            var hostRolesList = _readyHostRoleList.FindAll(h => h.GetIdleCores() >= 0);
            double totalCores = 0;
            foreach (var hostRole in hostRolesList)
            {
                totalCores += hostRole.GetIdleCores();
            }

            foreach (var hostRole in _pendingHostRoleList)
            {
                totalCores += hostRole.GetIdleCores();
            }

            return totalCores;
        }


        public int GetPendingHostRolesCount()
        {
            return _pendingHostRoleList.Count;
        }

        public double GetIdleHostRolesCount()
        {

            if (_experiment.OptimizerAggressivePodCreation == false)
            {
                var idleCoresCount = GetIdleHostRoleCores();
                var freeHostRolesCount = (int)(idleCoresCount / _experiment.HostRoleCores);
                return freeHostRolesCount;
            }
            else
            {
                var idleCoresCount = GetIdleHostRoleCores();
                var freeHostRolesCount = idleCoresCount / _experiment.HostRoleCores;

                foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
                {
                    foreach (var (cores, pool) in runtimePools)
                    {
                        var idlePodsCount = pool.GetExtraPodsCount();
                        freeHostRolesCount += Utilities.ComputeNeededHostRoles(idlePodsCount,
                                                                                cores,
                                                                                _experiment.HostRoleCores,
                                                                                _experiment.MaxPodsPerHostRole);
                    }
                }
                return (int)freeHostRolesCount;
            }
        }

        public double GetHostRolesUtilizedAggressive()
        {
            if (_experiment.OptimizerAggressivePodCreation == false)
            {
                return 0;
            }
            else
            {
                double freeHostRolesCount = 0;
                foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
                {
                    foreach (var (cores, pool) in runtimePools)
                    {
                        var idlePodsCount = pool.GetExtraPodsCount();
                        freeHostRolesCount += Utilities.ComputeNeededHostRoles(idlePodsCount,
                                                                                cores,
                                                                                _experiment.HostRoleCores,
                                                                                _experiment.MaxPodsPerHostRole);
                    }
                }
                return (int)freeHostRolesCount;
            }
        }


        public void ReconcilePoolGroups()
        {
            while (true)
            {
                var shouldBreak = true;
                foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
                {
                    foreach (var (poolCores, pool) in runtimePools)
                    {
                        var waitingRequests = pool.RequestQueue.Count();
                        var currentPodsCount = pool.GetPoolSize();
                        var targetPoolSize = waitingRequests + pool._poolParameters._minPodsCount;
                        if (currentPodsCount < targetPoolSize)
                        {
                            var requestedPodsCount = 1;
                            var createdPodsCount = CreatePods(pool, requestedPodsCount, true);
                            if (createdPodsCount != requestedPodsCount)
                            {
                                shouldBreak = true;
                                break;
                            }
                            if (pool.GetPoolSize() != targetPoolSize)
                            {
                                shouldBreak = false;
                            }
                        }
                    }
                }
                if (shouldBreak)
                {
                    break;
                }
            }

            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    var waitingRequests = pool.RequestQueue.Count();
                    var currentPodsCount = pool.GetPoolSize();
                    var targetPoolSize = waitingRequests + pool._poolParameters._minPodsCount;
                    if (_experiment.PoolOptimizationMethod == PoolOptimizationMethod.DROPS
                            && _experiment.OptimizerAggressivePodCreation == false)
                        Debug.Assert(currentPodsCount <= targetPoolSize);
                }
            }
        }


        public int GetRequiredHostRolesForReactive()
        {
            var coresToCountMap = new Dictionary<double, int>();
            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    if (!coresToCountMap.ContainsKey(poolCores))
                    {
                        coresToCountMap.Add(poolCores, 0);
                    }
                    coresToCountMap[poolCores] += pool._poolParameters._minPodsCount;
                }
            }
            return (int)Math.Ceiling(Utilities.ComputeNeededHostRoles(coresToCountMap, _experiment.HostRoleCores, _experiment.MaxPodsPerHostRole));
        }

        public int GetRequiredHostRolesForQueuedRequests()
        {
            var coresToCountMap = new Dictionary<double, int>();
            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    if (!coresToCountMap.ContainsKey(poolCores))
                    {
                        coresToCountMap.Add(poolCores, 0);
                    }
                    var waitingRequests = pool.RequestQueue.Count();
                    coresToCountMap[poolCores] += waitingRequests;
                }
            }

            return (int)Math.Ceiling(Utilities.ComputeNeededHostRoles(coresToCountMap, _experiment.HostRoleCores, _experiment.MaxPodsPerHostRole));
        }

        internal void WirePodEvents(
                    System.EventHandler<ServerlessPoolOptimizer.SimEvent> handleSchedulePodStateTransitionAt)
        {
            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    pool.FireSchedulePodStateTransitionAt += handleSchedulePodStateTransitionAt;
                }
            }
        }
        internal void UnwirePodEvents(
                            System.EventHandler<ServerlessPoolOptimizer.SimEvent> handleSchedulePodStateTransitionAt)
        {
            foreach (var (allocationLabel, runtimePools) in _allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    pool.FireSchedulePodStateTransitionAt -= handleSchedulePodStateTransitionAt;
                }
            }
        }
    }
}
