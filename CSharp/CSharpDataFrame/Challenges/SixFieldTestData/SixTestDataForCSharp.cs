using System;
using System.Collections.Generic;
using System.IO;
using CSharpDataFrame.SharedBetweenChallenges.CommonTypes;

public class SixTestDataSetDescription
{
    public int NumGrp1 { get; set; }
    public int NumGrp2 { get; set; }
    public int PointsPerIndex { get; set; }
}


public static class Constants
{
    public static readonly string LOCAL_TEST_DATA_FILE_LOCATION = "path_to_local_test_data";
    public static readonly List<Challenge> SIX_TEST_CHALLENGES = new List<Challenge>();
}

public class SixTestSourceDataFilePaths
{
    public string SourceDirectoryPath { get; private set; }
    public string SourceFilePathParquetSmallV1Files { get; private set; }
    public string SourceFilePathParquetSingleFile { get; private set; }
    public string SourceFilePathCsv { get; private set; }
    public SixTestSourceDataFilePaths(
        string sourceDirectoryPath,
        string sourceFilePathParquetSmallV1Files,
        string sourceFilePathParquetSingleFile,
        string sourceFilePathCsv)
    {
        SourceDirectoryPath = sourceDirectoryPath;
        SourceFilePathParquetSmallV1Files = sourceFilePathParquetSmallV1Files;
        SourceFilePathParquetSingleFile = sourceFilePathParquetSingleFile;
        SourceFilePathCsv = sourceFilePathCsv;
    }
    public List<string> FilePaths
    {
        get
        {
            return new List<string>
            {
                SourceFilePathParquetSmallV1Files,
                SourceFilePathParquetSingleFile,
                SourceFilePathCsv
            };
        }
    }
}




public static class SixTestDataTypes
{
    public static Dictionary<Challenge, string> SixDeriveExpectedAnswerDataFilePathsCsv(
        SixTestDataSetDescription dataDescription,
        bool tempFile = false)
    {
        int numGrp1 = dataDescription.NumGrp1;
        int numGrp2 = dataDescription.NumGrp2;
        int repetition = dataDescription.PointsPerIndex;
        string tempPostfix = tempFile ? "_temp" : "";
        var answerFileNames = new Dictionary<Challenge, string>();

        foreach (var challenge in Constants.SIX_TEST_CHALLENGES)
        {
            string filePath = Path.Combine(
                Constants.LOCAL_TEST_DATA_FILE_LOCATION,
                "SixField_Test_Data",
                $"{challenge}_answer_data_{numGrp1}_{numGrp2}_{repetition}{tempPostfix}.csv");
            answerFileNames[challenge] = filePath;
        }

        return answerFileNames;
    }
}