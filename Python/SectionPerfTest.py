#!python
# set PYSPARK_DRIVER_PYTHON=python
# set PYSPARK_DRIVER_PYTHON_OPTS=
# spark-submit --master local[7] --deploy-mode client SectionPerfTest.py
import gc
import scipy.stats, numpy
import time
import random
from LinearRegression import linear_regression
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = None
sc = None
log = None

def createSparkContext():
    global spark
    spark = SparkSession \
        .builder \
        .appName("SectionPerfTest") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.ui.enabled", "false") \
        .config("spark.rdd.compress", "false") \
        .config("spark.worker.cleanup.enabled", "true") \
        .config("spark.default.parallelism", 7) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.executor.memoryOverhead", "1g") \
        .config("spark.python.worker.reuse", "true") \
        .config("spark.port.maxRetries","1") \
        .config("spark.rpc.retry.wait","10s") \
        .config("spark.reducer.maxReqsInFlight","1") \
        .config("spark.network.timeout","30s") \
        .config("spark.shuffle.io.maxRetries","10") \
        .config("spark.shuffle.io.retryWait","60s") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
def setupSparkContext(in_spark):
    global spark, sc, log
    spark = in_spark
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.info("script initialized")
    sc.setCheckpointDir("SectionAggCheckpoint")
    return sc, log

# http://codeliberates.blogspot.com/2008/05/detecting-cpuscores-in-python.html
def detectCPUs():
    """
    Detects the number of CPUs on a system. Cribbed from pp.
    """
    # Linux, Unix and MacOS:
    if hasattr(os, "sysconf"):
        if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
            # Linux & Unix:
            ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
            if isinstance(ncpus, int) and ncpus > 0:
                return ncpus
        else: # OSX:
            return int(os.popen2("sysctl -n hw.ncpu")[1].read())
    # Windows:
    if os.environ.has_key("NUMBER_OF_PROCESSORS"):
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
            if ncpus > 0:
                return ncpus
    return 1 # Default

NumExecutors = 7
MaximumProcessableSegment = pow(10, 5)

import datetime as dt
import random
import re
import collections
import math
import os
import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql.window import Window
from pyspark.sql import Row

#region GenData
StudentHeader = collections.namedtuple("StudentHeader", 
    ["StudentId", "StudentName"])
TrimesterHeader = collections.namedtuple("TrimesterHeader", 
    ["Date", "WasAbroad"])
ClassLine = collections.namedtuple("ClassLine", 
    ["Dept", "Credits", "Grade"])
TrimesterFooter = collections.namedtuple("TrimesterFooter", 
    ["Major", "GPA", "Credits"])
StudentSummary = collections.namedtuple("StudentSummary", 
    ["StudentId", "StudentName", "SourceLines", "GPA", "Major", "MajorGPA"])
StudentSummaryStruct = DataTypes.StructType([
    DataTypes.StructField("StudentId", DataTypes.IntegerType(), True),
    DataTypes.StructField("StudentName", DataTypes.StringType(), True),
    DataTypes.StructField("SourceLines", DataTypes.IntegerType(), True),
    DataTypes.StructField("GPA", DataTypes.DoubleType(), True),
    DataTypes.StructField("Major", DataTypes.StringType(), True),
    DataTypes.StructField("MajorGPA", DataTypes.DoubleType(), True),
])
SparseLineSchema = DataTypes.StructType([
    DataTypes.StructField("Type", DataTypes.StringType(), True),
    DataTypes.StructField("StudentId", DataTypes.IntegerType(), True),
    DataTypes.StructField("StudentName", DataTypes.StringType(), True),
    DataTypes.StructField("Date", DataTypes.StringType(), True),
    DataTypes.StructField("WasAbroad", DataTypes.BooleanType(), True),
    DataTypes.StructField("Dept", DataTypes.IntegerType(), True),
    DataTypes.StructField("ClassCredits", DataTypes.IntegerType(), True),
    DataTypes.StructField("ClassGrade", DataTypes.IntegerType(), True),
    DataTypes.StructField("Major", DataTypes.IntegerType(), True),
    DataTypes.StructField("TriGPA", DataTypes.DoubleType(), True),
    DataTypes.StructField("TriCredits", DataTypes.IntegerType(), True),
])
LabeledTypedRow = collections.namedtuple("LabeledTypedRow", 
    ["Index", "Value"])
NumDepts = 4
#endregion

TestMethod = collections.namedtuple("TestMethod", 
    ["name", "interface", "scale", "delegate"])
test_method_list = []
def count_iter(iterator):
    count = 0
    for obj in iterator:
        count += 1
    return count

#region parsers
def parseLineToTypes(line):
    fields = line.split(',')
    if fields[0] == 'S':
        return StudentHeader(StudentId=int(fields[1]), StudentName=fields[2] )
    if fields[0] == 'TH':
        return TrimesterHeader(Date=fields[1], WasAbroad=(fields[2] == 'True') )
    if fields[0] == 'C':
        return ClassLine(Dept=int(fields[1]), Credits=int(fields[2]), Grade=int(fields[3]) )
    if fields[0] == 'TF':
        return TrimesterFooter(Major=int(fields[1]), GPA=float(fields[2]), Credits=int(fields[3]) )
    raise Exception("Malformed data "+line)
def parseLineToRow(line):
    fields = line.split(',')
    if fields[0] == 'S':
        return Row(Type=fields[0], 
            StudentId=int(fields[1]), StudentName=fields[2], 
            Date=None, WasAbroad=None,
            Dept=None, ClassCredits=None, ClassGrade=None,
            Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'TH':
        return Row(Type=fields[0], 
            StudentId=None, StudentName=None, 
            Date=fields[1], WasAbroad=(fields[2] == 'True'),
            Dept=None, ClassCredits=None, ClassGrade=None,
            Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'C':
        return Row(Type=fields[0], 
            StudentId=None, StudentName=None, 
            Date=None, WasAbroad=None, 
            Dept=int(fields[1]), ClassCredits=int(fields[2]), ClassGrade=int(fields[3]),
            Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'TF':
        return Row(Type=fields[0], 
            StudentId=None, StudentName=None, 
            Date=None, WasAbroad=None,
            Dept=None, ClassCredits=None, ClassGrade=None, 
            Major=int(fields[1]), TriGPA=float(fields[2]), TriCredits=int(fields[3]) )
    raise Exception("Malformed data "+line)
def dfSparseRowsFactory(filename, numPartitions=None):
    rdd = sc.textFile(filename, minPartitions=(numPartitions or 1))
    rdd = rdd \
        .map(parseLineToRow)
    df = spark.createDataFrame(rdd, SparseLineSchema)
    return df
def rddTypedWithIndexFactory(filename, numPartitions=None):
    rdd = sc.textFile(filename, minPartitions=(numPartitions or 1))
    rddTypedWithIndex = rdd \
        .map(parseLineToTypes) \
        .zipWithIndex() \
        .map(lambda pair: 
             LabeledTypedRow(\
                            Index = pair[1],
                            Value = pair[0]))
    return rddTypedWithIndex

#endregion
#region Mutable
class MutableTrimester:
    def __init__(self, date, wasAbroad):
        self.SourceLines = 1
        self.Credits = [0 for x in range(0, NumDepts)]
        self.WeightedGradeTotal = [0 for x in range(0, NumDepts)]
        self.Major = None
    def addClass(self, dept, credits, grade):
        self.SourceLines += 1
        self.Credits[dept] += credits
        self.WeightedGradeTotal[dept] += credits*grade
    def addFooter(self, major, gpa, credits):
        self.SourceLines += 1
        self.Major = major
    def _asdict(self):
        return {"Credits":list(self.Credits), "WGrade":list(self.WeightedGradeTotal), "Major":self.Major}
class MutableStudent:
    def __init__(self, studentId, studentName):
        self.SourceLines = 1
        self.StudentId = studentId
        self.StudentName = studentName
        self.LastMajor = None
        self.Credits = [0 for x in range(0, NumDepts)]
        self.WeightedGradeTotal = [0 for x in range(0, NumDepts)]
    def addTrimester(self, trimester):
        self.SourceLines += trimester.SourceLines
        self.LastMajor = trimester.Major
        for dept in range(0, NumDepts):
            self.Credits[dept] += trimester.Credits[dept]
            self.WeightedGradeTotal[dept] += trimester.WeightedGradeTotal[dept]
    def gradeSummary(self):
        return StudentSummary(
            StudentId=self.StudentId,
            StudentName=self.StudentName,
            SourceLines=self.SourceLines,
            Major=self.LastMajor,
            GPA=sum(self.WeightedGradeTotal)/max(1,sum(self.Credits)),
            MajorGPA=self.WeightedGradeTotal[self.LastMajor]/max(1,self.Credits[self.LastMajor])
                if self.LastMajor is not None else None
        )
    def gradeSummaryRow(self):
        return Row(**self.gradeSummary())
    def _asdict(self):
        return {"StudentId":self.StudentId, "LastMajor":self.LastMajor, "SourceLines":self.SourceLines, "Credits":list(self.Credits), "WGrade":list(self.WeightedGradeTotal)}
#endregion
#region aggregators
def aggregateTypedRowsToGrades(iterator):
    student = None
    trimester = None
    for lineno, rec in enumerate(iterator):
        if rec.__class__.__name__ == 'StudentHeader':
            if student is not None:
                yield student.gradeSummary()
            student = MutableStudent(rec.StudentId, rec.StudentName)
        elif rec.__class__.__name__ == 'TrimesterHeader':
            trimester = MutableTrimester(rec.Date, rec.WasAbroad)
        elif rec.__class__.__name__ == 'ClassLine':
            trimester.addClass(rec.Dept, rec.Credits, rec.Grade)
        elif rec.__class__.__name__ == 'TrimesterFooter':
            trimester.addFooter(rec.Major, rec.GPA, rec.Credits)
            student.addTrimester(trimester)
            trimester = None
        else:
            raise Exception(f"Unknown parsed row type {rec.__class__.__name__} on line {lineno}")
    if student is not None:
        yield student.gradeSummary()
#
def rowToStudentSummary(x):
    return  StudentSummary(
                StudentId=x.StudentId,
                StudentName=x.StudentName,
                SourceLines=x.SourceLines,
                Major=x.Major,
                GPA=x.GPA,
                MajorGPA=x.MajorGPA)
#
#endregion
#region Snippets
StudentSnippet = collections.namedtuple("StudentSnippet", 
    ["StudentId", "StudentName", 
     "FirstTrimester", "LastTrimester", "LastMajor", "Credits", "WeightedGradeTotal", 
     "FirstLineIndex", "LastLineIndex"])
CompletedStudent = collections.namedtuple("CompletedStudent", 
    ["StudentId", "StudentName", "SourceLines", "LastMajor", "Credits", "WeightedGradeTotal", 
     "FirstLineIndex", "LastLineIndex"])

class StudentSnippetBuilder:
    @staticmethod 
    def studentSnippetFromTypedRow(lineIndex, rec):
        credits = [0 for x in range(0, NumDepts)]
        weightedGradeTotal = [0 for x in range(0, NumDepts)]

        if rec.__class__.__name__ == 'StudentHeader':
            return StudentSnippet(
                StudentId=rec.StudentId, StudentName=rec.StudentName, 
                FirstTrimester=None, LastTrimester=None, 
                LastMajor=None, 
                Credits=credits, WeightedGradeTotal=weightedGradeTotal,
                FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
        elif rec.__class__.__name__ == 'TrimesterHeader':
            return StudentSnippet(
                StudentId=None, StudentName=None, 
                FirstTrimester=rec.Date, LastTrimester=rec.Date, 
                LastMajor=None, 
                Credits=credits, WeightedGradeTotal=weightedGradeTotal, 
                FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
        elif rec.__class__.__name__ == 'ClassLine':
            credits[rec.Dept] += rec.Credits
            weightedGradeTotal[rec.Dept] += rec.Credits * rec.Grade
            return StudentSnippet(
                StudentId=None, StudentName=None, 
                FirstTrimester=None, LastTrimester=None, 
                LastMajor=None, 
                Credits=credits, WeightedGradeTotal=weightedGradeTotal, 
                FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
        elif rec.__class__.__name__ == 'TrimesterFooter':
            return StudentSnippet(
                StudentId=None, StudentName=None, 
                FirstTrimester=None, LastTrimester=None, 
                LastMajor=rec.Major, 
                Credits=credits, WeightedGradeTotal=weightedGradeTotal, 
                FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
        else:
            raise Exception("Unknown parsed row type")
    @staticmethod
    def completedFromSnippet(lhs):
        assert lhs.StudentId is not None and lhs.LastMajor is not None
        return CompletedStudent(
            StudentId = lhs.StudentId,
            StudentName = lhs.StudentName,
            SourceLines = lhs.LastLineIndex - lhs.FirstLineIndex + 1,
            LastMajor = lhs.LastMajor,
            Credits = lhs.Credits,
            WeightedGradeTotal = lhs.WeightedGradeTotal,
            FirstLineIndex = lhs.FirstLineIndex,
            LastLineIndex = lhs.LastLineIndex)
    @staticmethod 
    def addSnippets(lhgroup, rhgroup):
        assert len(rhgroup) > 0
        if len(lhgroup) == 0:
            return rhgroup
        while len(rhgroup) > 0:
            lhs = lhgroup[-1]
            rhs = rhgroup[0]
            # print("Trying snip ending at %d against %d"%(lhs.LastLineIndex, rhs.FirstLineIndex))
            if rhs.StudentId is not None:
                if lhs.StudentId is not None:
                    lhgroup[-1] = StudentSnippetBuilder.completedFromSnippet(lhs)
                #                     print("Found student %d from lgroup"%(lhs.StudentId))
                lhgroup.append(rhs)
            else:
                assert lhs.LastLineIndex + 1 == rhs.FirstLineIndex
                credits = [0 for x in range(0, NumDepts)]
                weightedGradeTotal = [0 for x in range(0, NumDepts)]
                for dept in range(0, NumDepts):
                    credits[dept] = lhs.Credits[dept] + rhs.Credits[dept]
                    weightedGradeTotal[dept] = lhs.WeightedGradeTotal[dept] + rhs.WeightedGradeTotal[dept]
                lhgroup[-1] = StudentSnippet(
                    StudentId = lhs.StudentId,
                    StudentName = lhs.StudentName,
                    FirstTrimester = lhs.FirstTrimester if lhs.FirstTrimester is not None else rhs.FirstTrimester,
                    LastTrimester = rhs.LastTrimester,
                    LastMajor = rhs.LastMajor,
                    Credits = credits,
                    WeightedGradeTotal = weightedGradeTotal,
                    FirstLineIndex = lhs.FirstLineIndex,
                    LastLineIndex = rhs.LastLineIndex)
            rhgroup.pop(0)
        return lhgroup
    @staticmethod 
    def addSnippetsWOCompleting(lhgroup, rhgroup):
        assert len(rhgroup) > 0
        if len(lhgroup) == 0:
            return rhgroup
        for rhs in rhgroup:
            lhs = lhgroup[-1]
            # print("Trying snip ending at %d against %d"%(lhs.LastLineIndex, rhs.FirstLineIndex))
            if rhs.StudentId is not None:
                lhgroup.append(rhs)
            else:
                if lhs.LastLineIndex + 1 != rhs.FirstLineIndex:
                    print('about to assert ', lhs.LastLineIndex, rhs.FirstLineIndex)
                assert lhs.LastLineIndex + 1 == rhs.FirstLineIndex
                credits = [0 for x in range(0, NumDepts)]
                weightedGradeTotal = [0 for x in range(0, NumDepts)]
                for dept in range(0, NumDepts):
                    credits[dept] = lhs.Credits[dept] + rhs.Credits[dept]
                    weightedGradeTotal[dept] = lhs.WeightedGradeTotal[dept] + rhs.WeightedGradeTotal[dept]
                lhgroup[-1] = StudentSnippet(
                    StudentId = lhs.StudentId,
                    StudentName = lhs.StudentName,
                    FirstTrimester = lhs.FirstTrimester if lhs.FirstTrimester is not None else rhs.FirstTrimester,
                    LastTrimester = rhs.LastTrimester,
                    LastMajor = rhs.LastMajor,
                    Credits = credits,
                    WeightedGradeTotal = weightedGradeTotal,
                    FirstLineIndex = lhs.FirstLineIndex,
                    LastLineIndex = rhs.LastLineIndex)
        return lhgroup
    @staticmethod 
    def gradeSummary(x):
        assert x.LastMajor is not None
        return StudentSummary(
            StudentId=x.StudentId,
            StudentName=x.StudentName,
            SourceLines=x.SourceLines,
            Major=x.LastMajor,
            GPA=sum(x.WeightedGradeTotal)/max(1,sum(x.Credits)),
            MajorGPA=x.WeightedGradeTotal[x.LastMajor]/max(1,x.Credits[x.LastMajor])
        )

#endregion
#region Preprocessor
def identifySectionUsingIntermediateFile(srcFilename):
    destFilename = "e:/temp/sparkperftesting/temp.csv"
    if os.path.exists(destFilename):
        os.unlink(destFilename)
    reExtraType = re.compile("^S,")
    sectionId = -1
    with open(destFilename,"w") as outf:
        with open(srcFilename,"r") as inf:
            for line in inf:
                if reExtraType.match(line):
                    sectionId += 1
                assert sectionId >= 0
                outf.write(f"{sectionId},{line}")
    return destFilename
#endregion

#region nospark
def method_nospark_single_threaded(dataSize, filename, sectionMaximum):
    count = 0
    with open(filename, "r") as fh:
        for student in aggregateTypedRowsToGrades(map(parseLineToTypes, fh)):
            count += 1
    return count, None
test_method_list.append(TestMethod(
    name='method_nospark_single_threaded', 
    interface='python', 
    scale='singleline',
    delegate=method_nospark_single_threaded))
#endregion
#region mapPartitions
def method_mappart_single_threaded(dataSize, filename, sectionMaximum):
    rdd = sc.textFile(filename, minPartitions=1)
    rdd = rdd \
        .map(parseLineToTypes) \
        .mapPartitions(aggregateTypedRowsToGrades)
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_mappart_single_threaded', 
    interface='rdd', 
    scale='wholefile',
    delegate=method_mappart_single_threaded))
#
def method_mappart_odd_even(dataSize, filename, sectionMaximum):
    SegmentOffset = sectionMaximum - 1
    SegmentExtra = 2 * sectionMaximum
    SegmentSize = SegmentOffset + sectionMaximum - 1 + SegmentExtra
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    #
    rddTypedWithIndex = rddTypedWithIndexFactory(filename, TargetNumPartitions)
    rddSegmentsEven = rddTypedWithIndex \
        .keyBy(lambda x: (x.Index // SegmentSize, x.Index)) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .map(lambda x: x[1])
    rddSegmentsOdd = rddTypedWithIndex \
        .keyBy(lambda x: ((x.Index-SegmentOffset) // SegmentSize, x.Index)) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .filter(lambda x: x[0][0]>=0) \
        .map(lambda x: x[1])
    rddSegments = rddSegmentsEven.union(rddSegmentsOdd)
    #
    def aggregate(iterator):
        student = None
        trimester = None
        prevIndex = -1
        for labeled_row in iterator:
            index = labeled_row.Index
            if prevIndex+1 != index:
                if student is not None:
                    yield student.gradeSummary()
                    student = None
            prevIndex = index
            rec = labeled_row.Value
            if rec.__class__.__name__ == 'StudentHeader':
                if student is not None:
                    yield student.gradeSummary()
                    student = None
                student = MutableStudent(rec.StudentId, rec.StudentName)
            elif student is None:
                pass
            elif rec.__class__.__name__ == 'TrimesterHeader':
                trimester = MutableTrimester(rec.Date, rec.WasAbroad)
            elif trimester is None:
                pass
            elif rec.__class__.__name__ == 'ClassLine':
                trimester.addClass(rec.Dept, rec.Credits, rec.Grade)
            elif rec.__class__.__name__ == 'TrimesterFooter':
                trimester.addFooter(rec.Major, rec.GPA, rec.Credits)
                student.addTrimester(trimester)
                trimester = None
            else:
                raise Exception("Unknown parsed row type")
        if student is not None:
                yield student.gradeSummary()
    #                
    def chooseCompleteSection(iterator):
        held = None
        for rec in iterator:
            if held is not None:
                if held.StudentId != rec.StudentId:
                    yield held
                    held = rec
                elif rec.SourceLines > held.SourceLines:
                    held = rec
            else:
                held = rec
        if held is not None:
            yield held
    #                
    rddParallelMapPartitionsInter = rddSegments.mapPartitions(aggregate)
    rddParallelMapPartitions = rddParallelMapPartitionsInter \
        .keyBy(lambda x: (x.StudentId, x.SourceLines)) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .map(lambda x: x[1]) \
        .mapPartitions(chooseCompleteSection) \
        .sortBy(lambda x: x.StudentId)
    rdd = rddParallelMapPartitions
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_mappart_odd_even', 
    interface='rdd', 
    scale='wholesection',
    delegate=method_mappart_odd_even))
#
def method_mappart_partials(dataSize, filename, sectionMaximum):
    def strainCompletedItems(lgroup, passNumber):
        completedList = []
        #     print(f"Pass {passNumber}: Before Compaction series:", [(x.StudentId, x.FirstLineIndex, x.LastLineIndex) for x in lgroup])
        for i in range(len(lgroup)-2, 0, -1):
            rec = lgroup[i]
            if rec.__class__.__name__ == 'CompletedStudent':
                #             print("Found student %d from lgroup"%(rec.StudentId))
                completedList.append(rec)
                del lgroup[i]
            else:
                break
        #     print(f"Pass {passNumber}: After Compaction series:", [(x.StudentId, x.FirstLineIndex, x.LastLineIndex) for x in lgroup])
        return completedList, lgroup
    #
    def consolidateSnippetsInPartition(iter):
        residual = []
        lGroupNumber = None
        lgroup = None
        for rGroupNumber, rIsAtStartOfSegment, passNumber, rMember in iter:
            if lGroupNumber is not None and rGroupNumber != lGroupNumber:
                completedItems, lgroup = strainCompletedItems(lgroup, passNumber)
                for item in completedItems:
                    yield True, item
                residual.extend(lgroup)
                lGroupNumber = None
                lgroup = None
            if lGroupNumber is None:
                lGroupNumber = rGroupNumber
                lgroup = [rMember]
                assert rIsAtStartOfSegment == True
            else:
                StudentSnippetBuilder.addSnippets(lgroup, [rMember])
        if lGroupNumber is not None:
            completedItems, lgroup = strainCompletedItems(lgroup, passNumber)
            for item in completedItems:
                yield True, item
            residual.extend(lgroup)
        for item in residual:
            yield item.__class__.__name__ == 'CompletedStudent', item
    #
    maximumProcessableSegment = MaximumProcessableSegment # max(1000, MaximumProcessableSegment // 10)
    # print("maximumProcessableSegment=", maximumProcessableSegment)
    targetNumPartitions = max(NumExecutors, (dataSize+maximumProcessableSegment-1)//maximumProcessableSegment)
    # print("targetNumPartitions=", targetNumPartitions)
    #
    rdd = rddTypedWithIndexFactory(filename, targetNumPartitions) \
        .map(lambda x: StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value))
    rdd.persist(StorageLevel.DISK_ONLY)
    rddCumulativeCompleted = sc.parallelize([])
    passNumber=0
    while True:
        passNumber += 1
        # rdd type = RDD<StudentSnippet>
        #
        dataSize = rdd.count()
        rdd = rdd.zipWithIndex() \
            .map(lambda pair: LabeledTypedRow(Index = pair[1], Value = pair[0]))
        # rdd type = RDD<LabeledTypedRow(index, snippet)>
        #
        rdd = rdd.keyBy(lambda x: (x.Index // maximumProcessableSegment, \
                                x.Index % maximumProcessableSegment)) 
        # type = RDD<((igroup, iremainder),(index, snippet))>
        #
        targetNumPartitions = max(NumExecutors, (dataSize+maximumProcessableSegment-1)//maximumProcessableSegment)
        rdd = rdd.repartitionAndSortWithinPartitions(
                numPartitions=targetNumPartitions,
                partitionFunc=lambda x: x[0])
        #
        rdd = rdd.map(lambda x: (x[0][0], x[0][1] == 0, passNumber, x[1].Value))
        # type = (igroup, bool, snippet)
        #
        rdd = rdd.mapPartitions(consolidateSnippetsInPartition)
        # type RDD<(bool, snippet)>
        #
        rdd.persist(StorageLevel.DISK_ONLY)
        #         print("Pass %d Found %d combined snippets"%(passNumber, rdd.count()))
        #
        rddCompleted = rdd.filter(lambda x:x[0]).map(lambda x:x[1])
        # type: RDD<completed>
        # print("Pass %d Found %d students on pass %d"%(passNumber, rddCompleted.count(), passNumber))
        #
        rddCumulativeCompleted = rddCumulativeCompleted.union(rddCompleted)
        rddCumulativeCompleted.localCheckpoint()
        rddCumulativeCompleted.count()
        # print("Pass %d Found %d students total"%(passNumber, rddCumulativeCompleted.count()))
        # type: RDD<snippit>
        #
        rdd = rdd.filter(lambda x: not x[0]).map(lambda x:x[1])
        # type: RDD<snippit>
        #
        rdd = rdd.sortBy(lambda x: x.FirstLineIndex)
        # type: RDD<snippit>
        #
        rdd.persist(StorageLevel.DISK_ONLY)
        NumRowsLeftToProcess = rdd.count()
        if passNumber == 20:
            print("Failed to complete")
            print(rdd.collect())
            raise Exception("Failed to complete")
        if NumRowsLeftToProcess == 1:
            rddFinal = rdd.map(StudentSnippetBuilder.completedFromSnippet)
            # type: RDD<completed>
            rddCumulativeCompleted = rddCumulativeCompleted.union(rddFinal)
            # print("Pass %d Final found %d students total"%(passNumber, rddCumulativeCompleted.count()))
            break
    rdd = rddCumulativeCompleted \
        .map(StudentSnippetBuilder.gradeSummary)
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_mappart_partials', 
    interface='rdd', 
    scale='threerows',
    delegate=method_mappart_partials))
#endregion
#region reduce
def method_reduce_partials_broken(dataSize, filename, sectionMaximum):
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    rdd = rddTypedWithIndexFactory(filename, TargetNumPartitions)
    dataSize = rdd.count()
    rdd = rdd \
        .map(lambda x: [StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value)])
    targetDepth = max(1,math.ceil(math.log(dataSize/MaximumProcessableSegment,MaximumProcessableSegment-2)))
    students = rdd.treeAggregate([], StudentSnippetBuilder.addSnippets, StudentSnippetBuilder.addSnippets, depth=targetDepth)
    if len(students)>0:
        students[-1] = StudentSnippetBuilder.completedFromSnippet(students[-1])
    students = [StudentSnippetBuilder.gradeSummary(x) for x in students]
def nonCommutativeTreeAggregate(rdd, zeroValueFactory, seqOp, combOp, depth=2, divisionBase=2):
    def aggregatePartition(ipart, iterator):
        acc = zeroValueFactory()
        lastindex = None
        for index, obj in iterator:
            acc = seqOp(acc, obj)
            lastindex = index
        yield (lastindex, acc)
    #
    def combineInPartition(ipart, iterator):
        acc = zeroValueFactory()
        lastindex = None
        for (ipart, subindex), (index, obj) in iterator:
            acc = seqOp(acc, obj)
            lastindex = index
        if lastindex is not None:
            yield (lastindex, acc)
    #
    persistedRDD = rdd
    rdd.persist(StorageLevel.DISK_ONLY)
    rdd = rdd \
        .zipWithIndex() \
        .persist(StorageLevel.DISK_ONLY)
    numRows = rdd.count()
    persistedRDD.unpersist()
    persistedRDD = rdd
    rdd = rdd \
        .map(lambda x: (x[1], x[0])) \
        .mapPartitionsWithIndex(aggregatePartition)
    for idepth in range(depth-1, -1, -1):
        rdd.localCheckpoint()
        numSegments = pow(divisionBase, idepth)
        if numSegments >= numRows: 
            continue
        segmentSize = (numRows + numSegments-1) // numSegments
        rdd = rdd \
            .keyBy(lambda x: (x[0] // segmentSize, x[0] % segmentSize)) \
            .repartitionAndSortWithinPartitions(
                numPartitions=numSegments,
                partitionFunc=lambda x: x[0]) \
            .mapPartitionsWithIndex(combineInPartition)
    rdd = rdd \
        .flatMap(lambda x: x[1])
    return rdd
def method_asymreduce_partials(dataSize, filename, sectionMaximum):
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    rdd = sc.textFile(filename, minPartitions=TargetNumPartitions) \
        .zipWithIndex() \
        .map(lambda x: LabeledTypedRow(Index=x[1],Value=parseLineToTypes(x[0]))) \
        .map(lambda x: [StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value)])
    divisionBase = 2
    targetDepth = max(1,math.ceil(math.log(dataSize/MaximumProcessableSegment,divisionBase)))
    rdd = nonCommutativeTreeAggregate(rdd,
        lambda:[], 
        StudentSnippetBuilder.addSnippetsWOCompleting, 
        StudentSnippetBuilder.addSnippetsWOCompleting, 
        depth=targetDepth,
        divisionBase=divisionBase)
    rdd = rdd \
        .map(StudentSnippetBuilder.completedFromSnippet) \
        .map(StudentSnippetBuilder.gradeSummary)
    return count_iter(rdd.toLocalIterator()), rdd
    # rdd.count()
test_method_list.append(TestMethod(
    name='method_asymreduce_partials', 
    interface='rdd', 
    scale='finalsummaries',
    delegate=method_asymreduce_partials))

#endregion
#region preprocess
def method_prep_mappart(dataSize, filename, sectionMaximum):
    def parseLineToTypesWithLineNo(args):
        lineNumber = args[1]
        line = args[0]
        fields = line.split(',')
        sectionId = int(fields[0])
        fields = fields[1:]
        rowType = fields[0]
        if rowType == 'S':
            return (sectionId, lineNumber, StudentHeader(StudentId=int(fields[1]), StudentName=fields[2] ))
        if rowType == 'TH':
            return (sectionId, lineNumber, TrimesterHeader(Date=fields[1], WasAbroad=(fields[2] == 'True') ))
        if rowType == 'C':
            return (sectionId, lineNumber, ClassLine(Dept=int(fields[1]), Credits=int(fields[2]), Grade=int(fields[3]) ))
        if rowType == 'TF':
            return (sectionId, lineNumber, TrimesterFooter(Major=int(fields[1]), GPA=float(fields[2]), Credits=int(fields[3]) ))
        raise Exception(f"Unknown parsed row type {rowType} on line {lineNumber} in file {filename}")
    #
    interFileName = identifySectionUsingIntermediateFile(filename)
    # TargetNumPartitions = int(math.ceil(float(dataSize)/MaximumProcessableSegment/NumExecutors))*NumExecutors
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    rdd = \
        rdd = sc.textFile(interFileName, TargetNumPartitions) \
        .zipWithIndex() \
        .map(parseLineToTypesWithLineNo) \
        .map(lambda x: ((x[0],x[1]),x[2])) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .map(lambda x: x[1]) \
        .mapPartitions(aggregateTypedRowsToGrades)
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_prep_mappart', 
    interface='rdd', 
    scale='wholesection',
    delegate=method_prep_mappart))
#
def method_prep_groupby_core(dfSrc, sectionMaximum):
    df = dfSrc
    window = Window \
        .partitionBy(df.SectionId) \
        .orderBy(df.LineNumber) \
        .rowsBetween(-sectionMaximum,sectionMaximum)
    df = df \
        .withColumn('LastMajor', func.last(df.Major).over(window))
    df = df \
        .groupBy(df.SectionId, df.Dept) \
        .agg(
            func.max(df.StudentId).alias('StudentId'),
            func.max(df.StudentName).alias('StudentName'),
            func.count(df.LineNumber).alias('SourceLines'),
            func.first(df.LastMajor).alias('LastMajor'),
            func.sum(df.ClassCredits).alias('DeptCredits'),
            func.sum(df.ClassCredits * df.ClassGrade).alias('DeptWeightedGradeTotal')        
        )
    df = df \
        .groupBy(df.SectionId) \
        .agg(
            func.max(df.StudentId).alias('StudentId'),
            func.max(df.StudentName).alias('StudentName'),
            func.sum(df.SourceLines).alias('SourceLines'),
            func.first(df.LastMajor).alias('Major'),
            func.sum(df.DeptCredits).alias('TotalCredits'),
            func.sum(df.DeptWeightedGradeTotal).alias('WeightedGradeTotal'),
            func.sum(func.when(df.Dept == df.LastMajor, df.DeptCredits)).alias('MajorCredits'),
            func.sum(func.when(df.Dept == df.LastMajor, df.DeptWeightedGradeTotal)).alias('MajorWeightedGradeTotal')
        )
    df = df \
        .fillna({'MajorCredits':0, 'MajorWeightedGradeTotal':0})
    df = df \
        .drop(df.SectionId) \
        .withColumn('GPA', df.WeightedGradeTotal / func.when(df.TotalCredits > 0, df.TotalCredits).otherwise(1)) \
        .drop(df.WeightedGradeTotal) \
        .drop(df.TotalCredits) \
        .withColumn('MajorGPA', df.MajorWeightedGradeTotal / func.when(df.MajorCredits > 0, df.MajorCredits).otherwise(1)) \
        .drop(df.MajorWeightedGradeTotal) \
        .drop(df.MajorCredits)
    return df
#
def method_prep_groupby(dataSize, filename, sectionMaximum):
    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)]+
        SparseLineSchema.fields)
    def parseLineToRowWithLineNo(arg):
        lineNumber = int(arg[1])
        line = arg[0]
        fields = line.split(',')
        sectionId = int(fields[0])
        fields = fields[1:]
        lineType = fields[0]
        if lineType == 'S':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber, 
                Type=fields[0], 
                StudentId=int(fields[1]), StudentName=fields[2], 
                Date=None, WasAbroad=None,
                Dept=None, ClassCredits=None, ClassGrade=None,
                Major=None, TriGPA=None, TriCredits=None)
        if lineType == 'TH':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber, 
                Type=fields[0], 
                StudentId=None, StudentName=None, 
                Date=fields[1], WasAbroad=(fields[2] == 'True'),
                Dept=None, ClassCredits=None, ClassGrade=None,
                Major=None, TriGPA=None, TriCredits=None)
        if lineType == 'C':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber, 
                Type=fields[0], 
                StudentId=None, StudentName=None, 
                Date=None, WasAbroad=None, 
                Dept=int(fields[1]), ClassCredits=int(fields[2]), ClassGrade=int(fields[3]),
                Major=None, TriGPA=None, TriCredits=None)
        if lineType == 'TF':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber, 
                Type=fields[0], 
                StudentId=None, StudentName=None, 
                Date=None, WasAbroad=None,
                Dept=None, ClassCredits=None, ClassGrade=None, 
                Major=int(fields[1]), TriGPA=float(fields[2]), TriCredits=int(fields[3]) )
        raise Exception("Malformed data "+line)
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    interFileName = identifySectionUsingIntermediateFile(filename)
    rdd = sc.textFile(interFileName, TargetNumPartitions) \
        .zipWithIndex() \
        .map(parseLineToRowWithLineNo)
    df = spark.createDataFrame(rdd, SparseLineWithSectionIdLineNoSchema)
    df = method_prep_groupby_core(df, sectionMaximum)
    # students = [StudentSummary(
    #             StudentId=x.StudentId,
    #             StudentName=x.StudentName,
    #             SourceLines=x.SourceLines,
    #             Major=x.Major,
    #             GPA=x.GPA,
    #             MajorGPA=x.MajorGPA
    #         ) for x in df.toLocalIterator()]
    rdd = df.rdd.map(rowToStudentSummary)
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_prep_groupby', 
    interface='df', 
    scale='wholesection',
    delegate=method_prep_groupby))
#
def method_prepcsv_groupby(dataSize, filename, sectionMaximum):
    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)]+
        SparseLineSchema.fields)
    def convertToRowCsv(srcFilename):
        destFilename = "e:/temp/sparkperftesting/temp.csv"
        if os.path.exists(destFilename):
            os.unlink(destFilename)
        studentId = None
        with open(destFilename,"w") as outf:
            lineNumber = 0
            with open(srcFilename,"r") as inf:
                for line in inf:
                    lineNumber += 1
                    row = parseLineToRow(line.strip())
                    if row.StudentId is not None:
                        studentId = str(row.StudentId)
                    outf.write(",".join([
                        studentId, str(lineNumber), row.Type,
                        str(row.StudentId) if row.StudentId is not None else '',
                        row.StudentName if row.StudentName is not None else '',
                        row.Date if row.Date is not None else '',
                        str(row.WasAbroad) if row.WasAbroad is not None else '',
                        str(row.Dept) if row.Dept is not None else '',
                        str(row.ClassCredits) if row.ClassCredits is not None else '',
                        str(row.ClassGrade) if row.ClassGrade is not None else '',
                        str(row.Major) if row.Major is not None else '',
                        str(row.TriGPA) if row.TriGPA is not None else '',
                        str(row.TriCredits) if row.TriCredits is not None else '',
                        "\n"
                    ]))
        return destFilename
    #
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    interFileName = convertToRowCsv(filename)
    df = spark.read.format("csv") \
        .schema(SparseLineWithSectionIdLineNoSchema) \
        .load(interFileName)
    df = method_prep_groupby_core(df, sectionMaximum)
    # students = [StudentSummary(
    #             StudentId=x.StudentId,
    #             StudentName=x.StudentName,
    #             SourceLines=x.SourceLines,
    #             Major=x.Major,
    #             GPA=x.GPA,
    #             MajorGPA=x.MajorGPA
    #         ) for x in df.toLocalIterator()]
    rdd = df.rdd.map(rowToStudentSummary)
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_prepcsv_groupby', 
    interface='df', 
    scale='wholesection',
    delegate=method_prepcsv_groupby))
#endregion
#region join
def method_join_groupby(dataSize, filename, sectionMaximum):
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    rdd = sc.textFile(filename, TargetNumPartitions)
    def withIndexColumn(lineNumber, row):
        return Row(
            LineNumber=lineNumber,
            ClassCredits=row.ClassCredits, 
            ClassGrade=row.ClassGrade, 
            Date=row.Date,
            Dept=row.Dept, 
            Major=row.Major, 
            StudentId=row.StudentId, 
            StudentName=row.StudentName, 
            TriCredits=row.TriCredits, 
            TriGPA=row.TriGPA, 
            Type=row.Type,
            WasAbroad=row.WasAbroad)
    #
    NumRows = rdd.count()
    rdd = rdd \
        .zipWithIndex() \
        .map(lambda x: withIndexColumn(x[1], parseLineToRow(x[0])))
    SparseLineWithLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)
    df = spark.createDataFrame(rdd, SparseLineWithLineNoSchema)
    dfStudentHeaders = df \
        .filter(df.StudentId.isNotNull())
    window = Window \
        .orderBy(df.LineNumber) \
        .rowsBetween(1,1)
    dfStudentHeaders = dfStudentHeaders \
        .withColumn("FirstSHLineNumber", dfStudentHeaders.LineNumber) \
        .withColumn("NextSHLineNumber", func.lead(dfStudentHeaders.LineNumber).over(window)) \
        .select('StudentId', 'StudentName', 'FirstSHLineNumber', 'NextSHLineNumber')
    dfStudentHeaders = dfStudentHeaders \
        .na.fill({"NextSHLineNumber":NumRows})
    df = df \
        .drop('StudentId', 'StudentName') \
        .join(dfStudentHeaders, 
            (dfStudentHeaders.FirstSHLineNumber <= df.LineNumber) &
            (dfStudentHeaders.NextSHLineNumber > df.LineNumber)
            ) \
        .drop('FirstSHLineNumber', 'NextSHLineNumber')
    window = Window \
        .partitionBy(df.StudentId) \
        .orderBy(df.LineNumber) \
        .rowsBetween(-sectionMaximum,sectionMaximum)
    df = df \
        .withColumn('LastMajor', func.last(df.Major).over(window))
    df = df \
        .groupBy(df.StudentId, df.Dept) \
        .agg(
            func.max(df.StudentName).alias('StudentName'),
            func.count(df.LineNumber).alias('SourceLines'),
            func.first(df.LastMajor).alias('LastMajor'),
            func.sum(df.ClassCredits).alias('DeptCredits'),
            func.sum(df.ClassCredits * df.ClassGrade).alias('DeptWeightedGradeTotal')        
        )
    df = df \
        .groupBy(df.StudentId) \
        .agg(
            func.max(df.StudentName).alias('StudentName'),
            func.sum(df.SourceLines).alias('SourceLines'),
            func.first(df.LastMajor).alias('Major'),
            func.sum(df.DeptCredits).alias('TotalCredits'),
            func.sum(df.DeptWeightedGradeTotal).alias('WeightedGradeTotal'),
            func.sum(func.when(df.Dept == df.LastMajor, df.DeptCredits)).alias('MajorCredits'),
            func.sum(func.when(df.Dept == df.LastMajor, df.DeptWeightedGradeTotal)).alias('MajorWeightedGradeTotal')
        )
    df = df \
        .fillna({'MajorCredits':0, 'MajorWeightedGradeTotal':0})
    df = df \
        .withColumn('GPA', df.WeightedGradeTotal / func.when(df.TotalCredits > 0, df.TotalCredits).otherwise(1)) \
        .drop(df.WeightedGradeTotal) \
        .drop(df.TotalCredits) \
        .withColumn('MajorGPA', df.MajorWeightedGradeTotal / func.when(df.MajorCredits > 0, df.MajorCredits).otherwise(1)) \
        .drop(df.MajorWeightedGradeTotal) \
        .drop(df.MajorCredits)
    # students = [StudentSummary(
    #             StudentId=x.StudentId,
    #             StudentName=x.StudentName,
    #             SourceLines=x.SourceLines,
    #             Major=x.Major,
    #             GPA=x.GPA,
    #             MajorGPA=x.MajorGPA
    #         ) for x in df.toLocalIterator()]
    rdd = df.rdd.map(rowToStudentSummary)
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_join_groupby', 
    interface='df', 
    scale='wholesection',
    delegate=method_join_groupby))
#
def method_join_mappart(dataSize, filename, sectionMaximum):
    TargetNumPartitions = max(NumExecutors, (dataSize+MaximumProcessableSegment-1)//MaximumProcessableSegment)
    rdd = rddTypedWithIndexFactory(filename, TargetNumPartitions)
    NumRows = rdd.count()
    rdd = rdd \
        .map(lambda x: (x.Index, x.Value.__class__.__name__, x.Value))
    rddSH0 = rdd \
        .filter(lambda x: x[1] == 'StudentHeader') \
        .zipWithIndex() \
        .map(lambda x: (x[1], (x[0][0], x[0][2].StudentId)))
    rssSH1 = rddSH0 \
        .map(lambda x: (x[0]-1, x[1])) \
        .filter(lambda x: x[0]>=0) \
        .union(sc.parallelize([(rddSH0.count()-1, (NumRows, -1))]))
    def unpackStudentHeaderFromTuples(x):
        shLineNumber, ((firstLineNo, studentId), (nextLineNo, _)) = x
        return [(lineNo, studentId) for lineNo in range(firstLineNo, nextLineNo)]
    rddSH = rddSH0.join(rssSH1) \
        .flatMap(unpackStudentHeaderFromTuples)
    rdd = rdd \
        .map(lambda x: (x[0], x[2])) \
        .join(rddSH)
    def repackageTypedLineWithSH(x):
        lineNo, (typedRow, studentId) = x
        return ((studentId, lineNo), typedRow)
    rdd = rdd \
        .map(repackageTypedLineWithSH) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0])
    def extractStudentSummary(iterator):
        student = None
        trimester = None
        for lineno, x in enumerate(iterator):
            (studentId, lineNo), rec = x
            if rec.__class__.__name__ == 'StudentHeader':
                if student is not None:
                    yield student.gradeSummary()
                student = MutableStudent(rec.StudentId, rec.StudentName)
            elif rec.__class__.__name__ == 'TrimesterHeader':
                trimester = MutableTrimester(rec.Date, rec.WasAbroad)
            elif rec.__class__.__name__ == 'ClassLine':
                trimester.addClass(rec.Dept, rec.Credits, rec.Grade)
            elif rec.__class__.__name__ == 'TrimesterFooter':
                trimester.addFooter(rec.Major, rec.GPA, rec.Credits)
                student.addTrimester(trimester)
                trimester = None
            else:
                raise Exception(f"Unknown parsed row type {rec.__class__.__name__} on line {lineno}")
        if student is not None:
            yield student.gradeSummary()
    #
    rdd = rdd \
        .mapPartitions(extractStudentSummary)
    return count_iter(rdd.toLocalIterator()), rdd
test_method_list.append(TestMethod(
    name='method_join_mappart', 
    interface='rdd', 
    scale='wholesection',
    delegate=method_join_mappart))
#endregion
def populateDatasets():
    NumTrimesters = 8
    NumClassesPerTrimester = 4
    def generateData(filename, NumStudents, NumTrimesters, NumClassesPerTrimester, NumDepts):
        def AddMonths(d,x):
            serial = d.year*12+(d.month-1)
            serial += x
            return dt.date( serial // 12, serial % 12 +1, d.day)
        if os.path.exists(filename):
            return
        with open(filename, "w") as f:
            for studentId in range(1,NumStudents+1):
                f.write(f"S,{studentId},John{studentId}\n")
                for trimester in range(1, NumTrimesters+1):
                    dated = AddMonths(dt.datetime(2017,1,1), trimester)
                    wasAbroad = random.randint(0,10) == 0
                    major = (studentId % NumDepts) if trimester > 1 else NumDepts-1
                    f.write(f"TH,{dated:%Y-%m-%d},{wasAbroad}\n")
                    trimester_credits = 0
                    trimester_weighted_grades = 0
                    for classno in range(1, NumClassesPerTrimester+1):
                        dept = random.randrange(0,NumDepts)
                        grade = random.randint(1, 4)
                        credits = random.randint(1,5)
                        f.write(f"C,{dept},{grade},{credits}\n")
                        trimester_credits += credits
                        trimester_weighted_grades += grade * credits
                    gpa = trimester_weighted_grades / trimester_credits
                    f.write(f"TF,{major},{gpa},{trimester_credits}\n")
    #
    datasets = []
    numStudents = 1
    for iScale in range(0,9):
        filename = "e:/temp/SparkPerfTesting/testdata%d.csv"%numStudents
        sectionMaximum = (1 + NumTrimesters * (1+NumClassesPerTrimester+1))
        dataSize = numStudents * sectionMaximum
        generateData(filename, numStudents, NumTrimesters, NumClassesPerTrimester, NumDepts)
        datasets.append((dataSize, filename, sectionMaximum))
        numStudents *= 10
    return datasets
RunResult = collections.namedtuple("RunResult", ["dataSize", "SectionMaximum", "elapsedTime", "recordCount"])
def runtests():
    NumRunsPer = 1
    datasets = populateDatasets()
    # datasets = [datasets[1]]
    # datasets = [datasets[8]]
    # datasets = datasets[4:5]
    # datasets = datasets[0:5]
    #
    test_run_itinerary = []
    for testMethod in test_method_list:
        # if testMethod.name not in ['method_prepcsv_groupby',]:
        #     continue
        # if testMethod.name in ['method_nospark_single_threaded', ]:
        #     continue
        for datatuple in datasets:
            dataSize, filename, sectionMaximum = datatuple
            dataNumStudents = dataSize // sectionMaximum
            if testMethod.name == 'method_mappart_single_threaded' \
                and dataNumStudents >= pow(10,5): # unrealiable
                continue
            if testMethod.name == 'method_join_groupby' \
                and dataNumStudents >= pow(10,5): # too slow
                continue
            if testMethod.name == 'method_mappart_partials' \
                and dataNumStudents >= pow(10,7): # unrealiable
                continue
            if testMethod.name == 'method_asymreduce_partials' \
                and dataNumStudents >= pow(10,7): # unrealiable
                continue
            if testMethod.name == 'method_mappart_odd_even' \
                and dataNumStudents >= pow(10,7): # unrealiable
                continue
            if testMethod.name == 'method_join_groupby' \
                and dataNumStudents >= pow(10,7): # times out
                continue
            if testMethod.name == 'method_join_mappart' \
                and dataNumStudents >= pow(10,7): # times out
                continue
            if testMethod.name == 'method_prep_mappart' \
                and dataNumStudents >= pow(10,8): # takes too long
                continue
            if testMethod.name == 'method_prepcsv_groupby' \
                and dataNumStudents >= pow(10,8): # times out
                continue                
            test_run_itinerary.extend((testMethod, datatuple) for i in range(0, NumRunsPer))
    random.seed(dt.datetime.now())
    random.shuffle(test_run_itinerary)
    #    
    test_runs = {}
    with open('Results/section_runs_1.csv', 'a') as f:
        for index, (test_method, (dataSize, filename, sectionMaximum)) in enumerate(test_run_itinerary):
            log.info("Working on %d of %d"%(index, len(test_run_itinerary)))
            startedTime = time.time()
            print("Working on %s %s %d %d"%(test_method.name, filename, sectionMaximum, round(math.log10(dataSize//sectionMaximum))))
            f.write("Working,%s,%s,%d,%d\n"%(test_method.name, filename, sectionMaximum, round(math.log10(dataSize//sectionMaximum))))
            f.flush()
            foundNumStudents, rdd = test_method.delegate(dataSize, filename, sectionMaximum)
            elapsedTime = time.time()-startedTime
            actualNumStudents = dataSize // sectionMaximum
            if foundNumStudents != actualNumStudents:
                f.write("failure,%s,%s,%d,%d,%f,%d\n"%(test_method.name, test_method.interface, result.dataSize, result.SectionMaximum, result.elapsedTime, foundNumStudents))
                print("failure,%s,%s,%d,%d,%f,%d\n"%(test_method.name, test_method.interface, result.dataSize, result.SectionMaximum, result.elapsedTime, foundNumStudents))
                continue
            if test_method.name not in test_runs:
                test_runs[test_method.name] = []
            result = RunResult(
                dataSize=dataSize,
                SectionMaximum=sectionMaximum,
                elapsedTime=elapsedTime,
                recordCount=foundNumStudents)
            test_runs[test_method.name].append(result)
            f.write("success,%s,%s,%d,%d,%f,%d\n"%(test_method.name, test_method.interface, result.dataSize, result.SectionMaximum, result.elapsedTime, result.recordCount))
            f.flush()
            print("Took %f secs"%elapsedTime)
            rdd = None
            gc.collect()
            time.sleep(10)
            print("")
    #
def DoAnalysis():
    test_runs = {}
    with open('Results/section_runs_1.csv', 'r') as f, \
        open('Results/temp.csv', 'w') as fout:
        for textline in f:
            if textline.startswith("Working"):
                print("Excluding line: "+textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: "+textline)
                continue
            fields = textline.rstrip().split(',')
            if len(fields) == 6:
                fields.append(None)
            test_status, test_method_name, test_method_interface, \
                result_dataSize, result_SectionMaximum, \
                result_elapsedTime, result_recordCount = tuple(fields)
            if test_status != 'success':
                print("Excluding line: "+textline)
                continue
            result = RunResult(
                dataSize=int(result_dataSize)//int(result_SectionMaximum),
                SectionMaximum=int(result_SectionMaximum),
                elapsedTime=float(result_elapsedTime),
                recordCount=result_recordCount)
            if test_method_name not in test_runs:
                test_runs[test_method_name] = []
            test_runs[test_method_name].append(result)
            fout.write("%s,%s,%d,%d,%f,%s\n"%(
                test_method_name, test_method_interface, 
                result.dataSize, result.SectionMaximum, result.elapsedTime, result.recordCount))
    # print(test_runs)
    if len(test_runs) < 1:
        print("no tests")
        return
    if min([len(x) for x in test_runs.values()]) < 10:
        print("not enough data ", [len(x) for x in test_runs.values()])
        return
    TestRegression = collections.namedtuple("TestRegression", 
        ["name", "interface", "scale", "run_count",
        "b0", "b0_low", "b0_high",
        "b1", "b1_low", "b1_high",
        "s2", "s2_low", "s2_high"])
    summary_status = ''
    regression_status = ''
    if True:
        test_results = []
        confidence = 0.95
        regression_status += ("%s,%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s\n")%(
            'RawMethod', 'interface', 'scale', 'run_count',
            'b0', 'b0 lo', 'b0 hi',
            'b1M', 'b1M lo', 'b1M hi',
            's2', 's2 lo', 's2 hi')
        summary_status += ("%s,%s,%s,%s,"+"%s,%s,%s,"+"%s,%s\n")% (
            'RawMethod', 'interface', 'scale', 'run_count',
            'SectionMaximum', 'mean', 'stdev', 
            'rl', 'rh')
        for name in test_runs:
            print("Looking to analyze %s"%name)
            test_method = [x for x in test_method_list if x.name == name][0]
            times = test_runs[name]
            size_values = set(x.dataSize for x in times)
            for dataSize in size_values:
                ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
                mean = numpy.mean(ar)
                stdev = numpy.std(ar, ddof=1)
                rl, rh = scipy.stats.norm.interval(confidence, loc=mean, scale=stdev/math.sqrt(len(ar)))
                summary_status += ("%s,%s,%s,"+"%d,%d,"+"%f,%f,%f,%f\n")%(
                    name, test_method.interface, test_method.scale, 
                    len(ar), dataSize, 
                    mean, stdev, rl, rh
                )
            x_values = [float(x.dataSize) for x in times]
            y_values = [x.elapsedTime for x in times]
            (b0, (b0_low, b0_high)), (b1, (b1_low,b1_high)), (s2, (s2_low,s2_high)) = \
                linear_regression(x_values, y_values, confidence)
            # a = numpy.array(y_values)
            # mean, sem, cumm_conf = numpy.mean(a), scipy.stats.sem(a, ddof=1), scipy.stats.t.ppf((1+confidence)/2., len(a)-1)
            # rangelow, rangehigh = \
            #     scipy.stats.t.interval(confidence, len(times)-1, loc=mean, scale=sem)
            result = TestRegression(
                name=test_method.name,
                interface=test_method.interface,
                scale=test_method.scale,
                run_count=len(times),
                b0=b0,
                b0_low=b0_low,
                b0_high=b0_high,
                b1=b1,
                b1_low=b1_low,
                b1_high=b1_high,
                s2=s2,
                s2_low=s2_low,
                s2_high=s2_high
                )
            test_results.append(result)
            regression_status += ("%s,%s,%s,%d,"+"%f,%f,%f,"+"%f,%f,%f,"+"%f,%f,%f\n")%(
                test_method.name, test_method.interface, test_method.scale, result.run_count,
                result.b0, result.b0_low, result.b0_high,
                result.b1*1e+6, result.b1_low*1e+6, result.b1_high*1e+6,
                result.s2, result.s2_low, result.s2_high)
    with open('Results/section_results.csv', 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")
#
if __name__ == "__main__":
    spark = createSparkContext()
    sc, log = setupSparkContext(spark)
    # runtests()
    DoAnalysis()
