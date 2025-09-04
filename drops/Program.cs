

namespace ServerlessPoolOptimizer
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Usage: drops experiments.json");
                Console.WriteLine("Arguments passed to the program:");
                foreach (var arg in args)
                {
                    Console.WriteLine(arg);
                }
                Environment.Exit(0);
            }

            string experimentsConfig = args[0];

            var experiments = Utilities.ParseExperiments(experimentsConfig);
            Console.WriteLine("Experiments count: {0}", experiments.Count());

            Analyzer.RunExperiments(experiments);

            // write results to a csv file
            Utilities.WriteResults(experiments);
        }
    }
}
