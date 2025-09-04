using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public enum PodState { Created, Pending, Ready, Allocated, Specialized, UserWorkload, Deleted, BeingRecycled, Recycled }
    // possible transitions
    /*
    *   Created --> Pending --> Ready --> Allocated --> Specialized --> UserWorkload --> Deleted --> Recycled
    *   Pending --> Deleted --> Recycled        
    *   Ready   --> Deleted --> Recycled
    *   Created --> Deleted --> Recycled     
    */
    public class PodLifeCycleDistributions
    {
        internal IDistribution _creationDemandDistribution;
        internal IDistribution _pendingDemandDistribution;
        internal IDistribution _idleDurationDistribution;
        internal IDistribution _allocatedDemandDistribution;
        internal IDistribution _specializedDemandDistribution;
        internal IDistribution _userWorkloadDemandDistribution;
        internal IDistribution _deleteDemandDistribution;
        internal IDistribution _vmRecyclingDemandDistribution;
        internal IDistribution _supplyDelayDistribution; // the combination of creation + pending
        internal IDistribution _deleteRecycleDelayDistribution; // the combination of deletion + recycling
        internal IDistribution _fullLifeCycleDistribution; // the combination of all states
        internal IDistribution _allocatedToRecycledDistribution; // from allocated --> recycled
        internal List<PodLifeCycleTimestamps> _lifeCyclesList;
        public PodLifeCycleDistributions(IDistribution pCreationDemandDistribution, IDistribution pPendingDemandDistribution,
                                        IDistribution pIdleDurationDistribution, IDistribution pAllocatedDemandDistribution,
                                        IDistribution pSpecializedDemandDistribution, IDistribution pUserWorkloadDemandDistribution,
                                        IDistribution pDeleteDemandDistribution, IDistribution pVmRecyclingDemandDistribution,
                                        IDistribution pSupplyDelayDistribution, IDistribution pDeleteRecycleDelayDistribution,
                                        IDistribution pFullLifeCycleDistribution, IDistribution pAllocatedToRecycledDistribution)
        {
            _creationDemandDistribution = pCreationDemandDistribution;
            _pendingDemandDistribution = pPendingDemandDistribution;
            _idleDurationDistribution = pIdleDurationDistribution;
            _allocatedDemandDistribution = pAllocatedDemandDistribution;
            _specializedDemandDistribution = pSpecializedDemandDistribution;
            _userWorkloadDemandDistribution = pUserWorkloadDemandDistribution;
            _deleteDemandDistribution = pDeleteDemandDistribution;
            _vmRecyclingDemandDistribution = pVmRecyclingDemandDistribution;
            _supplyDelayDistribution = pSupplyDelayDistribution;
            _deleteRecycleDelayDistribution = pDeleteRecycleDelayDistribution;
            _fullLifeCycleDistribution = pFullLifeCycleDistribution;
            _allocatedToRecycledDistribution = pAllocatedToRecycledDistribution;
            _lifeCyclesList = new List<PodLifeCycleTimestamps>();
        }

        internal int MinLength()
        {
            var min = _creationDemandDistribution.Count();
            min = Math.Min(min, _pendingDemandDistribution.Count());
            min = Math.Min(min, _idleDurationDistribution.Count());
            min = Math.Min(min, _allocatedDemandDistribution.Count());
            min = Math.Min(min, _specializedDemandDistribution.Count());
            min = Math.Min(min, _userWorkloadDemandDistribution.Count());
            min = Math.Min(min, _deleteDemandDistribution.Count());
            min = Math.Min(min, _vmRecyclingDemandDistribution.Count());
            min = Math.Min(min, _supplyDelayDistribution.Count());
            min = Math.Min(min, _deleteRecycleDelayDistribution.Count());
            min = Math.Min(min, _fullLifeCycleDistribution.Count());
            min = Math.Min(min, _allocatedToRecycledDistribution.Count());
            return (int)min;
        }

        internal PodLifeCycleTimestamps SampleLifeCycle()
        {
            int index = (int)(RandomSource.GetNext() * _lifeCyclesList.Count);
            return _lifeCyclesList[index];
        }

    }

    public class PodLifeCycleTimestamps
    {
        internal readonly double _timePointCreation;
        internal double _durationCreation; // TS = _durationCreation + _timePointCreation  
        internal double _durationPending; // TS = _timePointCreation + _durationCreation + _durationPending
        internal double _durationReady;
        internal double _durationAllocated;
        internal double _durationSpecialized;
        internal double _durationUserWorkload;
        internal double _durationDeleted;
        internal double _durationRecyclingVm;

        public PodLifeCycleTimestamps(double pTimePointCreation = -1.0, double pDurationCreation = -1.0, double pDurationPending = -1.0,
                                        double pDurationReady = -1.0, double pDurationAllocated = -1.0, double pDurationSpecialized = -1.0,
                                        double pDurationUserWorkload = -1.0, double pDurationDeleted = -1.0, double pDurationRecyclingVm = -1.0)
        {
            _timePointCreation = pTimePointCreation;
            _durationCreation = pDurationCreation;
            _durationPending = pDurationPending;
            _durationReady = pDurationReady;
            _durationAllocated = pDurationAllocated;
            _durationSpecialized = pDurationSpecialized;
            _durationUserWorkload = pDurationUserWorkload;
            _durationDeleted = pDurationDeleted;
            _durationRecyclingVm = pDurationRecyclingVm;
        }

        public void Assert()
        {
            Debug.Assert(_timePointCreation >= 0);
            Debug.Assert(_durationCreation >= 0);
            Debug.Assert(_durationPending >= 0);
            // Debug.Assert(_durationReady >= 0);
            Debug.Assert(_durationAllocated >= 0);
            Debug.Assert(_durationSpecialized >= 0);
            Debug.Assert(_durationUserWorkload >= 0);
            Debug.Assert(_durationDeleted >= 0);
            Debug.Assert(_durationRecyclingVm >= 0);
        }

        public void AssertLifeCycle()
        {
            Debug.Assert(_timePointCreation >= 0);
            Debug.Assert(GetTransitionEndTimePoint(PodState.Created) >= _timePointCreation);
            Debug.Assert(GetTransitionEndTimePoint(PodState.Pending) >= GetTransitionEndTimePoint(PodState.Created));
            Debug.Assert(GetTransitionEndTimePoint(PodState.Ready) >= GetTransitionEndTimePoint(PodState.Pending));
            Debug.Assert(GetTransitionEndTimePoint(PodState.Allocated) >= GetTransitionEndTimePoint(PodState.Ready));
            Debug.Assert(GetTransitionEndTimePoint(PodState.Specialized) >= GetTransitionEndTimePoint(PodState.Allocated));
            Debug.Assert(GetTransitionEndTimePoint(PodState.UserWorkload) >= GetTransitionEndTimePoint(PodState.Specialized));
            Debug.Assert(GetTransitionEndTimePoint(PodState.Deleted) >= GetTransitionEndTimePoint(PodState.UserWorkload));
            Debug.Assert(GetTransitionEndTimePoint(PodState.Recycled) >= GetTransitionEndTimePoint(PodState.Deleted));
        }

        public double GetTransitionEndTimePoint(PodState state)
        {
            double timePoint = _timePointCreation;
            switch (state)
            {
                case PodState.Recycled:
                case PodState.BeingRecycled:
                    timePoint += _durationRecyclingVm;
                    timePoint += _durationDeleted;
                    timePoint += _durationUserWorkload;
                    timePoint += _durationSpecialized;
                    timePoint += _durationAllocated;
                    timePoint += _durationReady;
                    timePoint += _durationPending;
                    timePoint += _durationCreation;
                    break;
                case PodState.Deleted:
                    timePoint += _durationDeleted;
                    timePoint += _durationUserWorkload;
                    timePoint += _durationSpecialized;
                    timePoint += _durationAllocated;
                    timePoint += _durationReady;
                    timePoint += _durationPending;
                    timePoint += _durationCreation;
                    break;
                case PodState.UserWorkload:
                    timePoint += _durationUserWorkload;
                    timePoint += _durationSpecialized;
                    timePoint += _durationAllocated;
                    timePoint += _durationReady;
                    timePoint += _durationPending;
                    timePoint += _durationCreation;
                    break;
                case PodState.Specialized:
                    timePoint += _durationSpecialized;
                    timePoint += _durationAllocated;
                    timePoint += _durationReady;
                    timePoint += _durationPending;
                    timePoint += _durationCreation;
                    break;
                case PodState.Allocated:
                    timePoint += _durationAllocated;
                    timePoint += _durationReady;
                    timePoint += _durationPending;
                    timePoint += _durationCreation;
                    break;
                case PodState.Ready:
                    timePoint += _durationReady;
                    timePoint += _durationPending;
                    timePoint += _durationCreation;
                    break;
                case PodState.Pending:
                    timePoint += _durationPending;
                    timePoint += _durationCreation;
                    break;
                case PodState.Created:
                    timePoint += _durationCreation;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return timePoint;
        }

        public override string ToString()
        {
            string str = "";
            str += String.Format("Creation time = {0}, ", _timePointCreation);
            str += String.Format("creation duration = {0}, ", _durationCreation);
            str += String.Format("pending duration = {0}, ", _durationPending);
            str += String.Format("idle duration = {0}, ", _durationReady);
            str += String.Format("allocation duration = {0}, ", _durationAllocated);
            str += String.Format("spec duration = {0}, ", _durationSpecialized);
            str += String.Format("user duration = {0}, ", _durationUserWorkload);
            str += String.Format("deletion duration = {0}, ", _durationDeleted);
            str += String.Format("recycling duration = {0}", _durationRecyclingVm);

            return str;
        }


    }

    public class Pod
    {
        internal readonly int _id;
        internal PodState _myState;
        internal double _podCores;
        internal PodState _lastStateBeforeDeleted;
        internal readonly PodLifeCycleTimestamps LifeCycleTimestamps;
        internal readonly AllocationLabel _allocationLabel;
        // the core size of the pool itself. Pod can have a smaller size.
        internal readonly int _hostRoleId;
        internal readonly double _poolCores;
        private readonly ISimulationTimeReader _clock;

        internal Pod(ISimulationTimeReader pSimulationTimeReaderdouble, int podId,
                    PodLifeCycleTimestamps pLifeCycleTimestamps, AllocationLabel pAllocationLabel,
                    double pPoolCores, int pHostRoleId)
        {
            _clock = pSimulationTimeReaderdouble;
            _id = podId;
            _podCores = pPoolCores;
            _myState = PodState.Created;
            _lastStateBeforeDeleted = PodState.Created;
            LifeCycleTimestamps = pLifeCycleTimestamps;
            _allocationLabel = pAllocationLabel;
            _poolCores = pPoolCores;
            _hostRoleId = pHostRoleId;
            LifeCycleTimestamps.Assert();
        }

        public void AdjustCores(double newCores)
        {
            Debug.Assert(_podCores >= newCores);
            _podCores = newCores;
        }

        public PodLifeCycleTimestamps GetRealLifeCycle()
        {
            PodLifeCycleTimestamps realLifeCycle = new PodLifeCycleTimestamps(
                LifeCycleTimestamps._timePointCreation
            );

            switch (_myState)
            {
                case PodState.Created:
                    realLifeCycle._durationCreation = _clock.Now - LifeCycleTimestamps._timePointCreation;
                    break;
                case PodState.Pending:
                    realLifeCycle._durationCreation = LifeCycleTimestamps._durationCreation;
                    double timePointPending = LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Created);
                    Debug.Assert(_clock.Now >= timePointPending);
                    realLifeCycle._durationPending = _clock.Now - timePointPending;
                    break;
                case PodState.Ready:
                    realLifeCycle._durationCreation = LifeCycleTimestamps._durationCreation;
                    realLifeCycle._durationPending = LifeCycleTimestamps._durationPending;
                    double timePointReady = LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Pending);
                    Debug.Assert(_clock.Now >= timePointReady);
                    realLifeCycle._durationReady = _clock.Now - timePointReady;
                    break;
                case PodState.Allocated:
                    realLifeCycle._durationCreation = LifeCycleTimestamps._durationCreation;
                    realLifeCycle._durationPending = LifeCycleTimestamps._durationPending;
                    realLifeCycle._durationReady = LifeCycleTimestamps._durationReady;
                    double timePointAllocated = LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Ready);
                    Debug.Assert(_clock.Now >= timePointAllocated);
                    realLifeCycle._durationAllocated = _clock.Now - timePointAllocated;
                    break;
                case PodState.Specialized:
                    realLifeCycle._durationCreation = LifeCycleTimestamps._durationCreation;
                    realLifeCycle._durationPending = LifeCycleTimestamps._durationPending;
                    realLifeCycle._durationReady = LifeCycleTimestamps._durationReady;
                    realLifeCycle._durationAllocated = LifeCycleTimestamps._durationAllocated;
                    double timePointSpecialized = LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Allocated);
                    Debug.Assert(_clock.Now >= timePointSpecialized);
                    realLifeCycle._durationSpecialized = _clock.Now - timePointSpecialized;
                    break;
                case PodState.UserWorkload:
                    realLifeCycle._durationCreation = LifeCycleTimestamps._durationCreation;
                    realLifeCycle._durationPending = LifeCycleTimestamps._durationPending;
                    realLifeCycle._durationReady = LifeCycleTimestamps._durationReady;
                    realLifeCycle._durationAllocated = LifeCycleTimestamps._durationAllocated;
                    realLifeCycle._durationSpecialized = LifeCycleTimestamps._durationSpecialized;
                    double _timePointUserWorkload = LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Specialized);
                    Debug.Assert(_clock.Now >= _timePointUserWorkload);
                    realLifeCycle._durationUserWorkload = _clock.Now - _timePointUserWorkload;
                    break;
                case PodState.Deleted:
                    realLifeCycle._durationCreation = LifeCycleTimestamps._durationCreation;
                    realLifeCycle._durationPending = LifeCycleTimestamps._durationPending;
                    realLifeCycle._durationReady = LifeCycleTimestamps._durationReady;
                    realLifeCycle._durationAllocated = LifeCycleTimestamps._durationAllocated;
                    realLifeCycle._durationSpecialized = LifeCycleTimestamps._durationSpecialized;
                    realLifeCycle._durationUserWorkload = LifeCycleTimestamps._durationUserWorkload;
                    double _timePointDeleted = LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.UserWorkload);
                    Debug.Assert(_clock.Now >= _timePointDeleted);
                    realLifeCycle._durationDeleted = _clock.Now - _timePointDeleted;
                    break;
                case PodState.BeingRecycled:
                    realLifeCycle._durationCreation = LifeCycleTimestamps._durationCreation;
                    realLifeCycle._durationPending = LifeCycleTimestamps._durationPending;
                    realLifeCycle._durationReady = LifeCycleTimestamps._durationReady;
                    realLifeCycle._durationAllocated = LifeCycleTimestamps._durationAllocated;
                    realLifeCycle._durationSpecialized = LifeCycleTimestamps._durationSpecialized;
                    realLifeCycle._durationUserWorkload = LifeCycleTimestamps._durationUserWorkload;
                    realLifeCycle._durationDeleted = LifeCycleTimestamps._durationDeleted;
                    double _timePointRecycledStart = LifeCycleTimestamps.GetTransitionEndTimePoint(PodState.Deleted);
                    Debug.Assert(_clock.Now >= _timePointRecycledStart);
                    realLifeCycle._durationRecyclingVm = _clock.Now - _timePointRecycledStart;
                    break;
                case PodState.Recycled:
                    realLifeCycle = LifeCycleTimestamps;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return realLifeCycle;
        }

    }
}