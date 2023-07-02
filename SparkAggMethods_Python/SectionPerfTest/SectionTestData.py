import datetime as dt
import os
import random
from typing import List

from .SectionTypeDefs import DataSetDescription, NumDepts

TEST_DATA_FILE_LOCATION = 'd:/temp/SparkPerfTesting'

LARGEST_EXPONENT = 8
available_data_sizes: List[str] = [str(10**i) for i in range(0,LARGEST_EXPONENT+1)]

def populateDatasets():
    NumTrimesters = 8
    NumClassesPerTrimester = 4

    def generateData(filename, NumStudents, NumTrimesters, NumClassesPerTrimester, NumDepts):
        def AddMonths(d, x):
            serial = d.year*12+(d.month-1)
            serial += x
            return dt.date(serial // 12, serial % 12 + 1, d.day)
        if os.path.exists(filename):
            return
        with open(filename, "w") as f:
            for studentId in range(1, NumStudents+1):
                f.write(f"S,{studentId},John{studentId}\n")
                for trimester in range(1, NumTrimesters+1):
                    dated = AddMonths(dt.datetime(2017, 1, 1), trimester)
                    wasAbroad = random.randint(0, 10) == 0
                    major = (studentId %
                             NumDepts) if trimester > 1 else NumDepts-1
                    f.write(f"TH,{dated:%Y-%m-%d},{wasAbroad}\n")
                    trimester_credits = 0
                    trimester_weighted_grades = 0
                    for _classno in range(1, NumClassesPerTrimester+1):
                        dept = random.randrange(0, NumDepts)
                        grade = random.randint(1, 4)
                        credits = random.randint(1, 5)
                        f.write(f"C,{dept},{grade},{credits}\n")
                        trimester_credits += credits
                        trimester_weighted_grades += grade * credits
                    gpa = trimester_weighted_grades / trimester_credits
                    f.write(f"TF,{major},{gpa},{trimester_credits}\n")
    #
    datasets: List[DataSetDescription] = []
    numStudents = 1
    for _iScale in range(0, LARGEST_EXPONENT+1):
        filename = f"{TEST_DATA_FILE_LOCATION}/testdata{numStudents}.csv"
        sectionMaximum = (1 + NumTrimesters * (1+NumClassesPerTrimester+1))
        dataSize = numStudents * sectionMaximum
        generateData(filename, numStudents, NumTrimesters,
                     NumClassesPerTrimester, NumDepts)
        datasets.append(DataSetDescription(dataSize, filename, sectionMaximum, numStudents))
        numStudents *= 10
    return datasets

