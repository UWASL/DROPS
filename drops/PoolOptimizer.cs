using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic;

namespace ServerlessPoolOptimizer
{
    public class PoolOptimizer
    {
        private readonly ISimulationTimeReader _clock;
        private readonly ServerlessService _serverlessService;
        private readonly Experiment _experiment;
        private readonly Simulator _simulator;
        public event EventHandler<SimEvent> FireOptimizerRunAt;


        public PoolOptimizer(ISimulationTimeReader pClock, ServerlessService pServerlessService, Simulator pSimulator, Experiment pExp)
        {
            _clock = pClock;
            _serverlessService = pServerlessService;
            _simulator = pSimulator;
            _experiment = pExp;
        }

        public void ScheduleOptimizeEvent(object sender)
        {
            SimEvent newEvent = _simulator.CreateEvent(EventType.RunOptimizer, _clock.Now,
                                            _clock.Now + _experiment.OptimizerFrequency,
                                            null, null, null);
            FireOptimizerRunAt(this, newEvent);
        }

        public void HandleRunOptimizerNowNotification(object sender, bool scheduleEvent)
        {
            ReconcileLegion();
            if (scheduleEvent)
                ScheduleOptimizeEvent(this);
        }

        public void ReconcileLegion()
        {
            if (!_experiment.OptimizerIsEnabled)
            {
                return;
            }
            // reconcile container pools
            ReconcilePoolGroups();

            // reconcile VM pool
            ReconcileVmPool();
        }

        private void ReconcileVmPool()
        {
            if (!_experiment.OptimizerScalingHostRolesIsEnabled)
            {
                return;
            }

            if (_experiment.PoolOptimizationMethod == PoolOptimizationMethod.Reactive
                || _experiment.PoolOptimizationMethod == PoolOptimizationMethod.PredictiveReactive)
            {
                var neededHostRolesForReactive = _serverlessService.GetRequiredHostRolesForReactive();
                neededHostRolesForReactive = Math.Min(neededHostRolesForReactive, 4096);
                neededHostRolesForReactive = Math.Max(neededHostRolesForReactive, Parameter.ReactiveMinVmPoolSize);
                neededHostRolesForReactive = neededHostRolesForReactive + Parameter.ReactiveExtraVmPoolSize;
                _serverlessService.PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount = neededHostRolesForReactive;
            }

            var idleHostRolesCount = _serverlessService.GetIdleHostRolesCount();
            var pendingHostRolesCount = _serverlessService.GetPendingHostRolesCount();
            var totalIdleHostRolesCount = idleHostRolesCount + pendingHostRolesCount;
            var hostRolesUtilizedAggressively = _serverlessService.GetHostRolesUtilizedAggressive();
            var hostRolesNeededForQueuedRequests = _serverlessService.GetRequiredHostRolesForQueuedRequests();
            hostRolesNeededForQueuedRequests -= (int)hostRolesUtilizedAggressively;
            var targetHostRolesCount = Math.Max(hostRolesNeededForQueuedRequests, _serverlessService.PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount);

            var assignedCoresCount = _serverlessService.GetTotalAssignedCores();
            assignedCoresCount = assignedCoresCount + hostRolesNeededForQueuedRequests * _experiment.HostRoleCores;

            switch (_experiment.HostRoleOptimizationMethod)
            {
                case HostRoleOptimizationMethod.DROPS:
                case HostRoleOptimizationMethod.PredictiveReactive:
                    if (_experiment.OptimizerEnableHostRoleDeletion
                        && idleHostRolesCount > targetHostRolesCount)
                    {
                        var deleteHostRolesCount = totalIdleHostRolesCount - targetHostRolesCount;
                        _serverlessService.LegionDeleteHostRoles((int)deleteHostRolesCount);
                    }
                    else if (totalIdleHostRolesCount < targetHostRolesCount)
                    {
                        var count = targetHostRolesCount - totalIdleHostRolesCount;
                        if (count > 0)
                        {
                            // Console.WriteLine("Creating {0} vm. current idle VMs: {1}, VM pool size: {2}",
                            // count, totalIdleHostRolesCount,
                            // _serverlessService.PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount);
                        }
                        _serverlessService.CreateHostRoles((int)count);
                    }
                    break;

                case HostRoleOptimizationMethod.Production:
                    var idleCoresCount = _serverlessService.GetIdlePendingHostRoleCores();
                    var minimumIdleCores = assignedCoresCount * _experiment.ShrinkHostRolesUtilizationThreshold;
                    var maximumIdleCores = assignedCoresCount * _experiment.ExpandHostRolesUtilizationThreshold;
                    var targetIdleCores = assignedCoresCount * _experiment.TargetHostRolesUtilizationThreshold;

                    if (idleCoresCount >= maximumIdleCores)
                    {
                        var deleteHostRolesCount = idleCoresCount - targetIdleCores;
                        deleteHostRolesCount = deleteHostRolesCount / _experiment.HostRoleCores;
                        var newIdleHostRolesCount = idleHostRolesCount - deleteHostRolesCount;
                        if (newIdleHostRolesCount < Parameter.MinHostRolesCount)
                        {
                            deleteHostRolesCount = idleHostRolesCount - Parameter.MinHostRolesCount;
                        }
                        _serverlessService.LegionDeleteHostRoles((int)deleteHostRolesCount);
                    }
                    else if (idleCoresCount <= minimumIdleCores)
                    {
                        var extraHostRolesCount = targetIdleCores - idleCoresCount;
                        extraHostRolesCount = extraHostRolesCount / _experiment.HostRoleCores;
                        _serverlessService.CreateHostRoles((int)extraHostRolesCount);
                    }
                    break;
            }
        }

        private void ReconcilePoolGroups()
        {
            double serviceMaxIdleCores = _serverlessService.PoolGroup.PoolGroupParameters.MinAssignedHostRolesCount;
            serviceMaxIdleCores = serviceMaxIdleCores * _experiment.HostRoleCores;

            _serverlessService.ReconcilePoolGroups();

            double currentIdleHostRolesCores = _serverlessService.GetIdleHostRoleCores();

            var totalReadyHostRoles = _serverlessService.GetReadyHostRolesCount();
            var totalReadyCores = totalReadyHostRoles * _experiment.HostRoleCores;
            currentIdleHostRolesCores = _serverlessService.GetIdleHostRoleCores();
            // if(currentIdleHostRolesCores > 10)
            //     Console.WriteLine("Before optimization: total cores: {0}, idle cores: {1}", totalReadyCores, currentIdleHostRolesCores);


            if (_experiment.OptimizerAggressivePodCreation)
            {
                if (currentIdleHostRolesCores > 0)
                {
                    // Console.WriteLine("Running aggressive");
                    CreatePodsAggressively(serviceMaxIdleCores, currentIdleHostRolesCores);
                }
            }

            if (_experiment.OptimizerAggressivePodCreation)
            {
                AggressiveDeleteExtraPods();
            }
            else
            {
                DeleteExtraPods();
            }

            totalReadyHostRoles = _serverlessService.GetReadyHostRolesCount();
            totalReadyCores = totalReadyHostRoles * _experiment.HostRoleCores;
            var currentIdleHostRolesCores2 = _serverlessService.GetIdleHostRoleCores();
            // if(currentIdleHostRolesCores > 10)
            // Console.WriteLine("After optimization: total cores: {0}, idle cores: {1}", totalReadyCores, currentIdleHostRolesCores2);
        }

        public void DeleteExtraPods()
        {
            foreach (var (allocationLabel, runtimePools) in _serverlessService._allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    double currentPoolSize = pool.GetPoolSize();
                    var waitingRequests = pool.RequestQueue.Count();
                    double extraPods = Math.Max(0, currentPoolSize - pool._poolParameters._minPodsCount - waitingRequests);
                    if (extraPods > 0)
                    {
                        _serverlessService.DeletePods(pool, (int)extraPods);
                    }
                }
            }
        }

        private void AggressiveDeleteExtraPods()
        {
            var currentExp = _experiment;
            double neededCoresCount = 0;
            Dictionary<PoolLabel, (double, double)> poolsExtraPodsRatioMap = new Dictionary<PoolLabel, (double, double)>();
            foreach (var (allocationLabel, runtimePools) in _serverlessService._allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    double currentPoolSize = pool.GetPoolSize();
                    double extraPods = Math.Max(0, currentPoolSize - pool._poolParameters._minPodsCount);
                    var extraPodsRatio = extraPods / pool._poolParameters._minPodsCount;
                    poolsExtraPodsRatioMap.Add(pool.PoolLabel, (extraPods, extraPodsRatio));
                    neededCoresCount += Math.Max(0, pool._poolParameters._minPodsCount - pool.GetPoolSize()) * poolCores;
                }
            }

            var poolsExtraPodsRatioSortedMap = poolsExtraPodsRatioMap.OrderByDescending(x => x.Value.Item2);
            foreach (var (poolLabel, (extraPodsCount, extraPodsRatio)) in poolsExtraPodsRatioSortedMap)
            {
                var extraPodRatiosSum = 0.0;
                var pool = _serverlessService._allocationLabelToPoolsMap[poolLabel.AllocationLabel][poolLabel.Cores];
                foreach (var (poolLabel2, (extraPodsCount2, extraPodsRatio2)) in poolsExtraPodsRatioSortedMap)
                {
                    extraPodRatiosSum += extraPodsRatio2;
                }
                if (extraPodRatiosSum == 0 || neededCoresCount <= 0)
                {
                    break;
                }
                var normalizedExtraPodsRatio = extraPodsRatio / extraPodRatiosSum;
                var coresToRecycle = normalizedExtraPodsRatio * neededCoresCount;
                var podsToRecycle = Math.Round(coresToRecycle / poolLabel.Cores);
                podsToRecycle = Math.Min(podsToRecycle, extraPodsCount);
                _serverlessService.DeletePods(pool, (int)podsToRecycle);
                neededCoresCount -= podsToRecycle * poolLabel.Cores;
                poolsExtraPodsRatioMap[poolLabel] = new(0, 0);
            }
        }

        private void CreatePodsAggressively(double serviceMaxExtraCores, double idleHostRolesCores)
        {
            double currentExtraCores = idleHostRolesCores;

            foreach (var (allocationLabel, runtimePools) in _serverlessService._allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    var poolExtraPodsCount = pool.GetExtraPodsCount();
                    currentExtraCores += poolExtraPodsCount * poolCores;
                }
            }

            // Console.WriteLine("During Addressive. current idle cores: {0} total extra cores: {1}", idleHostRolesCores, currentExtraCores);

            foreach (var (allocationLabel, runtimePools) in _serverlessService._allocationLabelToPoolsMap)
            {
                foreach (var (poolCores, pool) in runtimePools)
                {
                    double poolMaxExtraCores = pool._poolParameters.ExtraCoresRatio * currentExtraCores;
                    double poolExtraCoresRatio = pool._poolParameters.ExtraCoresRatio;
                    int maxPodCreationCount = (int)(idleHostRolesCores / poolCores);
                    int coresPerPool = (int)Math.Ceiling(poolExtraCoresRatio * currentExtraCores);
                    int newPodsCount = (int)Math.Ceiling(coresPerPool / poolCores);
                    var poolCurrentExtraPods = pool.GetExtraPodsCount();
                    newPodsCount = newPodsCount - poolCurrentExtraPods;
                    if (newPodsCount + poolCurrentExtraPods > pool._poolParameters.MaxExtraPods)
                    {
                        newPodsCount = pool._poolParameters.MaxExtraPods - poolCurrentExtraPods;
                    }
                    // Console.WriteLine("{0}: pool size: {1}, current size: {2}, new extra pods: {3}, max extra pods: {4}",
                    //                     pool.PoolLabel, pool.GetPoolSize() ,pool._poolParameters._minPodsCount,
                    //                     newPodsCount, pool._poolParameters.MaxExtraPods);
                    if (newPodsCount > 0)
                    {
                        newPodsCount = Math.Min(newPodsCount, maxPodCreationCount);
                        _serverlessService.CreatePods(pool, newPodsCount, false);
                        var consumedCores = newPodsCount * poolCores;
                        idleHostRolesCores = idleHostRolesCores - consumedCores;
                    }
                }
            }

            currentExtraCores = idleHostRolesCores;
            // if (idleHostRolesCores > 0)
            // {
            //     foreach (var (allocationLabel, runtimePools) in _serverlessService._allocationLabelToPoolsMap)
            //     {
            //         foreach (var (poolCores, pool) in runtimePools)
            //         {
            //             if (idleHostRolesCores <= 0)
            //             {
            //                 break;
            //             }
            //             double poolExtraCoresRatio = pool._poolParameters.ExtraCoresRatio;
            //             int coresPerPool = (int)Math.Ceiling(poolExtraCoresRatio * currentExtraCores);
            //             int newPodsCount = (int)Math.Ceiling(coresPerPool / poolCores);
            //             if (newPodsCount > 0)
            //             {
            //                 _serverlessService.CreatePods(pool, newPodsCount, false);
            //                 idleHostRolesCores = idleHostRolesCores - (newPodsCount * poolCores);
            //             }
            //         }
            //     }
            // }
        }
    }
}