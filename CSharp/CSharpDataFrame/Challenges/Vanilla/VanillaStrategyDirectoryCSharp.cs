namespace CSharpDataFrame.Challenges.Vanilla;

using CSharpDataFrame.Challenges.SixFieldTestData.SixTestDataTypes;
using CSharpDataFrame.Challenges.Vanilla.Strategies;

public class VanillaStrategyDirectoryCSharp
{
    public SixTestDataChallengeMethod[] Registry {get;} 
    = [
        VanillaCSharpSingleThreadDataframeNaive.Instance
    ];
}