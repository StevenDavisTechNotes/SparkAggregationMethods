// See https://aka.ms/new-console-template for more information
using Microsoft.Data.Analysis;
using Microsoft.Extensions.Logging;
using System.Text;
using System;
using Microsoft.Extensions.DependencyInjection;
using Challenges.Vanilla.VanillaRunnerCSharpDF;
using System.Collections.ObjectModel;

using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
ILogger logger = factory.CreateLogger("Program");
logger.LogInformation("Running...");
var serviceProvider =
    new ServiceCollection()
    .AddLogging(options =>
    {
        options.ClearProviders();
        options.AddConsole();
    })
    .AddSingleton<VanillaRunnerCSharpDF>()
    .BuildServiceProvider();

var commandLineArguments = new ReadOnlyCollection<string>(args);
serviceProvider.GetRequiredService<VanillaRunnerCSharpDF>().Main(commandLineArguments);
// bi_level_runner_py_st.main();  TODO: Implement this
// conditional_runner_py_st.main();  TODO: Implement this
// dedupe_runner_py_st.main();  TODO: Implement this
// section_runner_py_st.main();  TODO: Implement this
// vanilla_runner_py_st.main();  TODO: Implement this

logger.LogInformation("Done");
// Return a success code.
return 0;