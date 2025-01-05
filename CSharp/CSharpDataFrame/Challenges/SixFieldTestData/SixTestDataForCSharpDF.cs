// using System;
// using System.Collections.Generic;
// using System.Data;
// using System.IO;
// using Microsoft.Data.Analysis;

// public class SixDataSetDataCSharpDF
// {
//     public string SourceFilePathParquet { get; private set; }

//     public SixDataSetDataCSharpDF(string sourceFilePathParquet)
//     {
//         SourceFilePathParquet = sourceFilePathParquet;
//     }
// }

// public class SixDataSetCSharpDF
// {
//     public SixTestDataSetDescription DataDescription { get;  private set; }
//     public SixDataSetDataPythonST Data { get;  private set; }

//     public SixDataSetPythonST(SixTestDataSetDescription dataDescription, SixDataSetDataPythonST data)
//     {
//         DataDescription = dataDescription;
//         Data = data;
//     }
// }

// public interface IChallengeMethodPythonST
// {
//     object Call(SixTestExecutionParameters execParams, SixDataSetPythonST dataSet);
// }

// public class ChallengeMethodPythonSingleThreadedRegistration : SixTestDataChallengeMethodRegistrationBase<SolutionInterfacePythonST, IChallengeMethodPythonST>
// {
//     public string StrategyName2018 { get; }
//     public string StrategyName { get; }
//     public SolutionLanguage Language { get; }
//     public CalcEngine Engine { get; }
//     public SolutionInterfacePythonST Interface { get; }

//     public ChallengeMethodPythonSingleThreadedRegistration(string strategyName2018, string strategyName, SolutionLanguage language, CalcEngine engine, SolutionInterfacePythonST @interface)
//     {
//         StrategyName2018 = strategyName2018;
//         StrategyName = strategyName;
//         Language = language;
//         Engine = engine;
//         Interface = @interface;
//     }
// }