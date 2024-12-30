// See https://aka.ms/new-console-template for more information
using Microsoft.Data.Analysis;

Console.WriteLine("Hello, World!");

PrimitiveDataFrameColumn<DateTime> dateTimes = new PrimitiveDataFrameColumn<DateTime>("DateTimes"); // Default length is 0.
PrimitiveDataFrameColumn<int> ints = new PrimitiveDataFrameColumn<int>("Ints", 3); // Makes a column of length 3. Filled with nulls initially
StringDataFrameColumn strings = new StringDataFrameColumn("Strings", 3); // Makes a column of length 3. Filled with nulls initially
// Append 3 values to dateTimes
dateTimes.Append(DateTime.Parse("2019/01/01"));
dateTimes.Append(DateTime.Parse("2019/01/01"));
dateTimes.Append(DateTime.Parse("2019/01/02"));
DataFrame df = new DataFrame(dateTimes, ints, strings); // This will throw if the columns are of different lengths
Console.WriteLine(df);
