using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace ServerlessPoolOptimizer
{
    public enum HostRoleState { Pending, Ready, BeingDeleted, Deleted }

    public class HostRoleStateTimeTracker(int pId)
    {
        internal int _id = pId;
        internal double LastAllocationTimePoint;
        internal double HostRoleTotalTime; 
        internal double HostRoleServiceManagement;
        internal double HostRoleBootstrapping;
        internal double HostRoleUnallocatedCores;
        internal double PodCreation;
        internal double PodPending;
        internal double PodReady; 
        internal double PodAllocated;
        internal double PodSpecialization;
        internal double PodUserWorkload;
        internal double PodDeletion;
        internal double PodRecycling;

        public void Add(HostRoleStateTimeTracker instanceToAdd)
        {
            HostRoleTotalTime += instanceToAdd.HostRoleTotalTime;
            HostRoleServiceManagement += instanceToAdd.HostRoleServiceManagement;
            HostRoleBootstrapping += instanceToAdd.HostRoleBootstrapping;
            HostRoleUnallocatedCores += instanceToAdd.HostRoleUnallocatedCores;
            PodCreation += instanceToAdd.PodCreation;
            PodPending += instanceToAdd.PodPending;
            PodReady += instanceToAdd.PodReady;
            PodAllocated += instanceToAdd.PodAllocated;
            PodSpecialization += instanceToAdd.PodSpecialization;
            PodUserWorkload += instanceToAdd.PodUserWorkload;
            PodDeletion += instanceToAdd.PodDeletion;
            PodRecycling += instanceToAdd.PodRecycling;
        }

        public static string GetPrintHeader(bool isForTerminal = true)
        {
            string str;
            if (isForTerminal)
            {
                str = String.Format("{0,-5} {1,-17} {2,-17} {3,-17} {4,-17} {5,-17} {6,-17} {7,-17} {8,-17} {9,-17} {10,-17} {11,-17} {12,-17}",
                                                "ID", "Total", "Reserved", "H-Boot", "H-Idle", "P-Create", "P-Pending", "P-Ready",
                                                "P-Alloc", "P-Spec", "P-User", "P-Deleted", "P-Recycled");
            }
            else
            {
                str = String.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},",
                                            "ID", "Total", "Reserved", "Reserved-perc", "H-Boot", "H-Boot-perc", "H-Idle", "H-Idle-perc", "P-Create",
                                            "P-Create-perc", "P-Pending", "P-Pending-perc", "P-Ready", "P-Ready-perc", "P-Alloc", "P-Alloc-Perc",
                                            "P-Spec", "P-Spec-Perc", "P-User", "P-User-Perc", "P-Deleted", "P-Deleted-Perc", "P-Recycled", "P-Recycled-Perc");
            }
            return str;
        }

        public string ToString(bool isForTerminal = true)
        {
            string str = String.Format("{0,-5} {1,-17:0.00} {2,-17:0.00} {3,-17:0.00} {4,-17:0.00} {5,-17:0.00} {6,-17:0.00} {7,-17:0.00}",
                        _id, HostRoleTotalTime,
                        Utilities.GetValuePercentageStr(HostRoleServiceManagement, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(HostRoleBootstrapping, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(HostRoleUnallocatedCores, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(PodCreation, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(PodPending, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(PodReady, HostRoleTotalTime));

            str += String.Format(" {0,-17} {1,-17:0.00} {2,-17:0.00} {3,-17:0.00} {4,-17:0.00}",
                        Utilities.GetValuePercentageStr(PodAllocated, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(PodSpecialization, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(PodUserWorkload, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(PodDeletion, HostRoleTotalTime),
                        Utilities.GetValuePercentageStr(PodRecycling, HostRoleTotalTime));
            if (!isForTerminal)
            {
                str = Regex.Replace(str, @"\s+", ",");
                str = str.Replace("(", ",");
                str = str.Replace(")", "");
            }
            return str;
        }
    }

    public class HostRole
    {
        internal int _id;
        internal HostRoleState MyState;
        private readonly double _maxCores;
        private readonly double _reservedCores;
        private readonly int _maxPods;
        private double _idleCores;
        public readonly List<Pod> HostedPods;
        private int _hostedPodsCount;
        private readonly ISimulationTimeReader _clock;
        private double _timePointBootStart;
        private double _timePointBootComplete;
        public double _timePointDeleted;
        public HostRoleStateTimeTracker HostRoleStateTimeTracker;
        private PoolGroupId? _assignedPoolGroup;

        public event EventHandler<(HostRole, HostRoleState)> FireScheduleHostRoleStateTransitionAt;

        internal HostRole(int pId,
                        ISimulationTimeReader pSimulationTimeReader,
                        double pMaxCores,
                        int pMaxPods,
                        double pBootDemand,
                        double pReservedCores,
                        System.EventHandler<(ServerlessPoolOptimizer.HostRole, ServerlessPoolOptimizer.HostRoleState)> StateTransitionHandler)
        {
            _id = pId;
            _clock = pSimulationTimeReader;
            MyState = HostRoleState.Pending;
            _maxCores = pMaxCores;
            _reservedCores = pReservedCores;
            _maxPods = pMaxPods;
            _idleCores = pMaxCores;
            HostedPods = new List<Pod>();
            _hostedPodsCount = 0;
            _timePointBootStart = _clock.Now;
            _timePointBootComplete = _timePointBootStart + pBootDemand;
            _timePointDeleted = -1;
            HostRoleStateTimeTracker = new HostRoleStateTimeTracker(_id);
            HostRoleStateTimeTracker.LastAllocationTimePoint = _timePointBootComplete;
            _assignedPoolGroup = null;
            FireScheduleHostRoleStateTransitionAt += StateTransitionHandler;
        }

        internal bool IsPending()
        {
            return MyState == HostRoleState.Pending;
        }
        internal bool IsDeleted()
        {
            return MyState == HostRoleState.Deleted;
        }

        internal bool IsReady()
        {
            return MyState == HostRoleState.Ready;
        }

        internal bool IsBeingDeleted()
        {
            return MyState == HostRoleState.BeingDeleted;
        }

        internal double GetTotalCores()
        {
            return _maxCores;
        }
        internal double GetIdleCores()
        {
            if (_hostedPodsCount >= _maxPods)
            {
                return 0.0;
            }
            return _idleCores;
        }

        internal bool HasCapacity(double _pReqestedCore)
        {
            return _idleCores >= _pReqestedCore && _hostedPodsCount < _maxPods;
        }

        internal bool PlacePod(Pod pod)
        {
            if (!IsReady() || _idleCores < pod._podCores || _hostedPodsCount >= _maxPods)
            {
                return false;
            }
            HostRoleStateTimeTracker.HostRoleUnallocatedCores += _idleCores * (_clock.Now - HostRoleStateTimeTracker.LastAllocationTimePoint);
            HostRoleStateTimeTracker.LastAllocationTimePoint = _clock.Now;
            _idleCores -= pod._podCores;
            HostedPods.Add(pod);
            _hostedPodsCount++;
            return true;
        }

        internal void DeallocatePod(Pod pod)
        {
            Debug.Assert(IsReady() || IsBeingDeleted());
            HostRoleStateTimeTracker.HostRoleUnallocatedCores += _idleCores * (_clock.Now - HostRoleStateTimeTracker.LastAllocationTimePoint);
            HostRoleStateTimeTracker.LastAllocationTimePoint = _clock.Now;
            _idleCores += pod._podCores;
            // HostedPods.Remove(pod);
            _hostedPodsCount--;
            Debug.Assert(_idleCores <= _maxCores);
            Debug.Assert(_hostedPodsCount >= 0);
            if (IsBeingDeleted())
            {
                Delete();
            }
        }

        public void SetState(HostRoleState pState)
        {
            switch (pState)
            {
                case HostRoleState.Pending:
                    Debug.Assert(MyState == HostRoleState.Pending);
                    MyState = pState;
                    break;
                case HostRoleState.Ready:
                    Debug.Assert(MyState == HostRoleState.Pending);
                    MyState = pState;
                    break;
                case HostRoleState.BeingDeleted:
                    Debug.Assert(MyState == HostRoleState.Ready || MyState == HostRoleState.Pending);
                    MyState = pState;
                    break;
                case HostRoleState.Deleted:
                    Debug.Assert(MyState == HostRoleState.BeingDeleted);
                    MyState = pState;
                    _timePointDeleted = _clock.Now;
                    if (_timePointDeleted < _timePointBootComplete)
                    {
                        _timePointBootComplete = _timePointDeleted;
                        HostRoleStateTimeTracker.LastAllocationTimePoint = _timePointDeleted;
                    }
                    var idleTime = _idleCores * (_clock.Now - HostRoleStateTimeTracker.LastAllocationTimePoint);
                    if (idleTime > 0)
                    {
                        HostRoleStateTimeTracker.HostRoleUnallocatedCores += idleTime;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Delete()
        {
            if (_hostedPodsCount == 0)
            {
                // hostrole is not hosting any pods
                // can be deleted now  
                FireScheduleHostRoleStateTransitionAt(this, (this, HostRoleState.Deleted));
            }
        }

        public void PopulateHostRoleStateTimeTracker()
        {
            double hostRoleLastTimePoint = _clock.Now;
            if (IsDeleted())
            {
                hostRoleLastTimePoint = _timePointDeleted;
            }
            double totalCores = _maxCores + _reservedCores;
            HostRoleStateTimeTracker.HostRoleTotalTime = hostRoleLastTimePoint - _timePointBootStart;
            HostRoleStateTimeTracker.HostRoleServiceManagement = HostRoleStateTimeTracker.HostRoleTotalTime * _reservedCores;
            HostRoleStateTimeTracker.HostRoleTotalTime = HostRoleStateTimeTracker.HostRoleTotalTime * totalCores;
            if (IsPending())
            {
                HostRoleStateTimeTracker.HostRoleBootstrapping = (_clock.Now - _timePointBootStart) * totalCores;
            }
            else
            {
                HostRoleStateTimeTracker.HostRoleBootstrapping = (_timePointBootComplete - _timePointBootStart) * totalCores;
            }

            if (IsReady() || IsBeingDeleted())
            {
                HostRoleStateTimeTracker.HostRoleUnallocatedCores += (hostRoleLastTimePoint - HostRoleStateTimeTracker.LastAllocationTimePoint) * _idleCores;
                HostRoleStateTimeTracker.LastAllocationTimePoint = hostRoleLastTimePoint;
            }

            HostRoleStateTimeTracker.PodCreation = 0;
            HostRoleStateTimeTracker.PodPending = 0;
            HostRoleStateTimeTracker.PodReady = 0;
            HostRoleStateTimeTracker.PodAllocated = 0;
            HostRoleStateTimeTracker.PodSpecialization = 0;
            HostRoleStateTimeTracker.PodUserWorkload = 0;
            HostRoleStateTimeTracker.PodDeletion = 0;
            HostRoleStateTimeTracker.PodRecycling = 0;

            foreach (var pod in HostedPods)
            {
                PodLifeCycleTimestamps podReadLifeCycle = pod.GetRealLifeCycle();
                HostRoleStateTimeTracker.PodCreation += podReadLifeCycle._durationCreation * pod._podCores;
                HostRoleStateTimeTracker.PodPending += podReadLifeCycle._durationPending * pod._podCores;
                HostRoleStateTimeTracker.PodReady += podReadLifeCycle._durationReady * pod._podCores;
                HostRoleStateTimeTracker.PodAllocated += podReadLifeCycle._durationAllocated * pod._podCores;
                HostRoleStateTimeTracker.PodSpecialization += 0;
                HostRoleStateTimeTracker.PodUserWorkload += podReadLifeCycle._durationUserWorkload * pod._podCores;
                HostRoleStateTimeTracker.PodDeletion += podReadLifeCycle._durationDeleted * pod._podCores;
                HostRoleStateTimeTracker.PodRecycling += podReadLifeCycle._durationRecyclingVm * pod._podCores;
            }
        }

        internal bool IsAssignedToPoolGroup(PoolGroupId allocationLabel)
        {
            if (_assignedPoolGroup == null)
            {
                return false;
            }
            return _assignedPoolGroup.Equals(allocationLabel);
        }

        internal bool AssignToPoolGroup(PoolGroupId poolGroupId)
        {
            if (!IsReady() || _assignedPoolGroup != null)
            {
                return false;
            }
            _assignedPoolGroup = poolGroupId;
            return true;
        }

        internal bool RemovePoolGroupAssignment()
        {
            if (_assignedPoolGroup == null)
            {
                return true;
            }
            if (_hostedPodsCount != 0)
            {
                return false;
            }
            _assignedPoolGroup = null;
            return true;
        }
    }
}