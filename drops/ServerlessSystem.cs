using System.Diagnostics;

namespace ServerlessPoolOptimizer
{
    public class ServerlessSystem
    {
        public readonly OpenLoopLoad Loader;
        public readonly ServerlessService ServerlessService;
        public readonly PoolOptimizer PoolOptimizer;
        public readonly Simulator Simulator;
        private readonly SimulationTime _simulationTime;
        public readonly Experiment Experiment;

        public ServerlessSystem(OpenLoopLoad pOpenLoopLoader,
                                ServerlessService pServerlessService,
                                PoolOptimizer pPoolOptimizer,
                                SimulationTime pSimTime,
                                Simulator pSimulator,
                                Experiment pExp)
        {
            AllocationRequest.Init();
            Experiment = pExp;
            Loader = pOpenLoopLoader;
            ServerlessService = pServerlessService;
            PoolOptimizer = pPoolOptimizer;
            _simulationTime = pSimTime;
            Simulator = pSimulator;

            //wire the components
            ServerlessService.FireServiceInitializationCompleteAt += Simulator.HandleServiceInitializationCompleteAtNotification;
            Simulator.FireServiceInitializationNowComplete += ServerlessService.HandleServiceInitializationNowComplete;
            Simulator.FireServiceInitializationNowComplete += HandleServiceInitializationCompleteNowNotification;

            ServerlessService.FireScheduleHostRoleStateTransitionAt += Simulator.HandleScheduleHostRoleTransitionAt;
            Simulator.FireHostRoleStateTransitionNow += ServerlessService.HandleHostRoleStateTransitionNotification;

            ServerlessService.WirePodEvents(Simulator.HandleSchedulePodStateTransitionAt);
            Simulator.FirePodStateTransitionNow += ServerlessService.HandlePodStateTransitionNotification;

            Loader.FireRequestWillArrive += Simulator.HandleRequestWillArriveNotification;
            Simulator.FireRequestNowArrives += Loader.HandleRequestNowArrivesNotification;
            Simulator.FireRequestNowArrives += ServerlessService.HandleRequestNowArrivesNotification;

            ServerlessService.FireRequestWillDepartAt += Simulator.HandleRequestWillDepartAtNotification;

            pPoolOptimizer.FireOptimizerRunAt += Simulator.HandleOptimizerRunAtNotification;
            Simulator.FireRunOptimizerNow += pPoolOptimizer.HandleRunOptimizerNowNotification;
            ServerlessService.FireRunOptimizerNow += pPoolOptimizer.HandleRunOptimizerNowNotification;

            Simulator.FireCollectStatsNow += ServerlessService.HandleCollectStatsNotification;
            ServerlessService.FireCollectStatsAt += Simulator.HandleCollectStatsAtNotification;

            Simulator.FireReactiveScaleDownPoolSizesNow += ServerlessService.HandleReactiveScaleDownPoolSizes;
            ServerlessService.FireReactiveScaleDownPoolSizesAt += Simulator.HandleCollectStatsAtNotification;

            Simulator.FireUpdatePoolSizesNow += ServerlessService.HandleUpdatePoolSizesNotification;
            ServerlessService.FireUpdatePoolSizesAt += Simulator.HandleUpdatePoolSizesAtNotification;

            Loader.FireEndOfTrace += Simulator.HandleEndOfTraceNotification;
        }

        public override string ToString()
        {
            var s = "";
            return s;
        }

        public void RunSimulation(Double pStopTimeUnits, int pMaxRequest, bool pOutputFlag)
        {
            Debug.Assert(pStopTimeUnits > 0);
            Simulator.RunSimulation(this, pStopTimeUnits, pMaxRequest);
        }

        public event EventHandler<String> FireGetMyStatus;

        public void HandleServiceInitializationCompleteNowNotification(object sender, EventArgs e)
        {
            Debug.Assert(sender.GetType() == typeof(Simulator));
            ServerlessService.UpdatePoolSizesPrediction(null);
            Loader.HandleRequestNowArrivesNotification(this, null);
            // PoolOptimizer.ScheduleOptimizeEvent(this);
            ServerlessService.HandleCollectStatsNotification(this, EventArgs.Empty);
        }
        public void HandleGetStatusNotification(object sender, string s)
        {
            FireGetMyStatus(this, String.Format("SYS {0:0000.00}", _simulationTime.Now));
        }

        public void UnSubscribeHandlers()
        {
            ServerlessService.FireServiceInitializationCompleteAt -= Simulator.HandleServiceInitializationCompleteAtNotification;
            Simulator.FireServiceInitializationNowComplete -= ServerlessService.HandleServiceInitializationNowComplete;
            Simulator.FireServiceInitializationNowComplete -= HandleServiceInitializationCompleteNowNotification;

            ServerlessService.FireScheduleHostRoleStateTransitionAt -= Simulator.HandleScheduleHostRoleTransitionAt;
            // HostRole.FireScheduleHostRoleStateTransitionAt -= ServerlessService.HandleHostRoleStateTransitionNotification;
            Simulator.FireHostRoleStateTransitionNow -= ServerlessService.HandleHostRoleStateTransitionNotification;

            ServerlessService.UnwirePodEvents(Simulator.HandleSchedulePodStateTransitionAt);
            Simulator.FirePodStateTransitionNow -= ServerlessService.HandlePodStateTransitionNotification;

            Loader.FireRequestWillArrive -= Simulator.HandleRequestWillArriveNotification;
            Simulator.FireRequestNowArrives -= Loader.HandleRequestNowArrivesNotification;
            Simulator.FireRequestNowArrives -= ServerlessService.HandleRequestNowArrivesNotification;

            ServerlessService.FireRequestWillDepartAt -= Simulator.HandleRequestWillDepartAtNotification;

            Simulator.FireCollectStatsNow -= ServerlessService.HandleCollectStatsNotification;
            ServerlessService.FireCollectStatsAt -= Simulator.HandleCollectStatsAtNotification;

            Loader.FireEndOfTrace -= Simulator.HandleEndOfTraceNotification;
        }
    }
}
