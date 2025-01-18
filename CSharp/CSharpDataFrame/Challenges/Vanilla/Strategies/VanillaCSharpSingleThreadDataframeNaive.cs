namespace CSharpDataFrame.Challenges.Vanilla.Strategies;

using CSharpDataFrame.Challenges.SixFieldTestData.SixTestDataTypes;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;

public record VanillaCSharpSingleThreadDataframeNaive : SixTestDataChallengeMethod
{
    public static VanillaCSharpSingleThreadDataframeNaive Instance { get; }
        = new VanillaCSharpSingleThreadDataframeNaive();
    private VanillaCSharpSingleThreadDataframeNaive(
    ) : base(
        strategyName: nameof(VanillaCSharpSingleThreadDataframeNaive),
        engine: CalcEngine.DATA_FRAME,
        solutionInterface: CSharpSolutionInterface.MSFT_DATA_FRAME,
        requiresGpu: false,
        numericalTolerance: NumericalToleranceExpectations.NUMPY
    ) {}
}
