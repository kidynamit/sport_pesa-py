# Author:  kidynamit
## Date:    03-04-2016
from __future__ import print_function

from sports_parser import SoccerParser, NULL_STR
from sports_pca import SoccerPCA
from pyspark import SparkContext

from pyspark.mllib.classification import LogisticRegressionWithSGD, \
        LogisticRegressionWithLBFGS, SVMWithSGD, NaiveBayes
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD, \
        RidgeRegressionWithSGD, LassoWithSGD, IsotonicRegression
from pyspark.mllib.evaluation import RegressionMetrics

from json import load as json_load
from tempfile import NamedTemporaryFile

import os

__all__ = ["SoccerSpark"]

UPDATE_KEYWORD_ARGS =   "UPDATE_KEYWORD_ARGS"
KEYWORD_ARGS =          "KEYWORD_ARGS"
LABELS =                "LABELS"
LABEL_REDUCTION_EXPR =  "LABEL_REDUCTION_EXPRESSION"
VAR_NAME =              "data_entry"
CONFIG_FILENAME =       "pyspark.conf"

def _gen_labeled_points(data_entry):
    label_expr, data_start = data_entry[0]
    data_entry = data_entry[1:]
    idx = []
    data = []
    size = len(data_entry) - data_start
    assert(size > 0)
    label = eval(label_expr)

    data_entry = data_entry[data_start:]
    _total = 0.0
    for i, d in zip(range(len(data_entry)), data_entry):
        if d == None:
            d = 0.0
        if not d == 0.0:
            idx.append(i)
            data.append(d)
            _total += abs(d)
    ## TODO apply a different p for L^p
    data = [ abs(d)/float(_total) for d in data] ## normalizing with L^1
    _feature = SparseVector(size, idx, data)
    return LabeledPoint(label, _feature)

class SoccerSpark:
    sc = None
    sp = None
    collated_file = None
    CONFIG_DATA = None

    __parsed = False
    __checked_labels = False
    __nulls_removed = False
    _trainer = None
    _metrics_results = None
    _traineridx = None

    def _load_config(self):
        with open(CONFIG_FILENAME, "r") as config_file:
            self.CONFIG_DATA = json_load(config_file)

    def _gen_label_expression(self, title_dict, var_name="data_entry"):
        self._labelidx = self._labelidx % len(self.CONFIG_DATA[LABEL_REDUCTION_EXPR])
        expr = self.CONFIG_DATA[LABEL_REDUCTION_EXPR][self._labelidx]
        expr_list = expr.split()
        new_expr = []
        for token in expr_list:
            val = token
            try:
                val = eval(token)
            except NameError:
                try:
                    val = "{0}[{1}]".format(var_name, title_dict[token])
                except:
                    print("error encountered. '{0}' is not a valid heading.".format(token))
                    os.sys.exit(1)
            except SyntaxError:
                pass
            finally:
                new_expr.append(val)
        return " ".join(new_expr)

    def __init__(self, collated_filename=None, start_year=1993, end_year=2015):
        try:
            if not collated_filename:
                _temp = NamedTemporaryFile()
                _temp.close()

                tail = "{0:04d}".format(start_year)[-2:]  + \
                        "{0:04d}".format(end_year)[-2:] + "_{0}.csv"\
                        .format(os.path.split(_temp.name)[-1])

                del _temp

                collated_filename = os.path.join("../out", tail)
            self.collated_file = open(collated_filename, "w")
            self.collated_file.close()
        except:
            print("error encountered with creating file '{0}'. exiting ..."\
                    .format(collated_filename))
            os.sys.exit(1)
        self.sp = SoccerParser(collated_filename, start_year, end_year)
        self._load_config()
        FEATURES_START = len(self.CONFIG_DATA[LABELS])
        self._metrics_results = []

    def __init_spark_context__(self):
        if not self.sc:
            self.sc = SparkContext("local", "SoccerSpark")

    def parse_data(self):
        if not self.__parsed:
            self.sp.add_season_files()
            self.sp.clean_season_files()
            self.sp.post_clean_analysis()
            self.__parsed = True
            print("SoccerSpark: Data parsed successfully")

    def check_labels(self):
        print("checking labels ... ", end="")
        if not self.__parsed:
            return None
        if self.__checked_labels:
            return None
        self.collated_file = open(self.collated_file.name, "r")
        column_titles = self.collated_file.readline().rstrip("\r\n").split(",")


        try:
            assert(len(self.CONFIG_DATA[LABELS]) <= len(column_titles))
            assert(self.CONFIG_DATA[LABELS] == column_titles[:len(self.CONFIG_DATA[LABELS])])
        except AssertionError:
            print("reorder the column_titles:\n\t{0} \nto include the labels:\n\t{1} \nas a prefix"\
                    .format(column_titles, self.CONFIG_DATA[LABELS]))
            print("the length of column_titles({0}) should be  >= length of labels({1})"\
                    .format(len(column_titles), len(self.CONFIG_DATA[LABELS])))
            raise
        finally:
            if not self.collated_file.closed:
                self.collated_file.close()

        self.__checked_labels = True
        print("done.")

    def remove_null_featured(self):
        print("removing null features ... ", end="")
        if not self.__parsed and not self.__checked_labels:
            return None
        if self.__nulls_removed:
            return None

        self.collated_file = open(self.collated_file.name, "r")
        nul_file = open(self.collated_file.name + ".nul", "w")

        first_line = self.collated_file.readline()
        heading = first_line.rstrip("\r\n").split(",")
        nul_file.write(first_line)

        data_start = len(self.CONFIG_DATA[LABELS])

        for line in self.collated_file:
            str_data = line.rstrip("\r\n").split(",")

            _continue = False
            for entry in str_data[:data_start]:
                if not eval(entry):
                    _continue = True
                    break
            if _continue:
                continue

            _continue = True
            for entry in str_data[data_start:]:
                if eval(entry):
                    _continue = False
                    break
            if _continue:
                continue

            nul_file.write(line)

        if not nul_file.closed:
            nul_file.close()

        if not self.collated_file.closed:
            self.collated_file.close()

        os.rename(nul_file.name, self.collated_file.name)
        self.__nulls_removed = True

        print("done.")

    def gen_labeled_pointsRDD(self):
        if not self.__nulls_removed or not self.__checked_labels or not self.__parsed:
            return None

        # Perform PCA
        pca = SoccerPCA(self.collated_file.name, len(self.CONFIG_DATA[LABELS]))
        pca.perform_pca(9)

        self.__init_spark_context__()

        if not self.collated_file.closed:
            self.collated_file.close()

        text_fileRDD = self.sc.textFile(self.collated_file.name)\
                .map(lambda line : line.rstrip("\r\n").split(","))
        heading = text_fileRDD.first()
        title_dict = dict(zip(heading, range(len(heading))))

        label_expr = self._gen_label_expression(title_dict, var_name=VAR_NAME)
        data_start = len(self.CONFIG_DATA[LABELS])

        all_dataRDD = text_fileRDD \
                .zipWithIndex() \
                .filter(lambda (_, y): not y == 0) \
                .map(lambda (x, _): [ eval(entry) for entry in x ]) \
                .map(lambda data: _gen_labeled_points([(label_expr, data_start)] + data)) \
                .cache()
        return all_dataRDD

    def train_data(self, *args):
        if not self._trainer:
            return None

        dataRDD = self.gen_labeled_pointsRDD()
        if not dataRDD:
            return None

        kwargs = self.CONFIG_DATA[KEYWORD_ARGS][self._trainer.__name__]
        return self._trainer.train(dataRDD, *args, **kwargs)

    def get_regressor_metrics(self, *args):
        if not self._trainer:
            return None

        dataRDD = self.gen_labeled_pointsRDD()
        if not dataRDD:
            return None
        _count = dataRDD.count()
        k = 0.7
        dataRDD, kfaultRDD = dataRDD.randomSplit([k, 1-k])
        _kcount = kfaultRDD.count()
        _kratio = _kcount / float(_count)

        kwargs = self.CONFIG_DATA[KEYWORD_ARGS][self._trainer.__name__]
        model = self._trainer.train(dataRDD, *args, **kwargs)

        predict_obsRDD = kfaultRDD\
                .map(lambda lp : (float(model.predict(lp.features)), lp.label))\
                .cache()

        dataRDD.unpersist()
        metrics = RegressionMetrics(predict_obsRDD)
        predict_obsRDD.unpersist()
        self._metrics_results.append("""

        THE METRICS FOR YOUR '{0}' MODEL IS AS FOLLOWS:

            keyword_args:       {6}
            reduction_expr:     {7}
            K-faultRatio:       {8:2.2f}%

            explainedVariance:  {1}
            meanAbsoluteError:  {2}
            meanSquaredError:   {3}
            r2:                 {4}
            rootMeanSquaredE:   {5}

            """.format(self._trainer.__name__, \
                metrics.explainedVariance, \
                metrics.meanAbsoluteError, \
                metrics.meanSquaredError, \
                metrics.r2, \
                metrics.rootMeanSquaredError, \
                self.CONFIG_DATA[KEYWORD_ARGS][self._trainer.__name__], \
                self.CONFIG_DATA[LABEL_REDUCTION_EXPR][self._labelidx], \
                _kratio*100.0))


    def __del__(self):
        if self.sc:
            self.sc.cancelAllJobs()
            self.sc.stop()
        if self.sp:
            del self.sp
        if self.collated_file:
            if not self.collated_file.closed:
                self.collated_file.close()
        if len(self._metrics_results):
            for results in self._metrics_results:
                print(results)

    def update_labelidx(self):
        if not self._trainer:
            return False

        self._labelidx += 1
        _size = len(self.CONFIG_DATA[LABEL_REDUCTION_EXPR])
        if self._labelidx >= _size:
            return False
        return True


    def update_traineridx(self):
        if not self._trainer:
            return False

        self._traineridx += 1
        _size = len(self.\
                CONFIG_DATA[UPDATE_KEYWORD_ARGS]\
                [self._trainer.__name__])
        if self._traineridx >= _size:
            return False
        _name = self._trainer.__name__
        _idx = self._traineridx
        self.CONFIG_DATA[KEYWORD_ARGS][_name]\
                .update(self.CONFIG_DATA[UPDATE_KEYWORD_ARGS][_name][_idx])
        return True

    def set_trainer(self, new_trainer):
        if hasattr(new_trainer, "train"):
            self._trainer = new_trainer
            self._traineridx = -1
            self._labelidx = -1

    def reset_traineridx (self):
        self._traineridx = -1

def main ():
    ss = SoccerSpark(start_year=2000)
    ss.parse_data()
    ss.check_labels()
    ss.remove_null_featured()
    trainers = [ \
                LogisticRegressionWithSGD,\
                NaiveBayes,
                SVMWithSGD]
    for trainer in trainers:
        ss.set_trainer(trainer)
        while (ss.update_labelidx()):
            ss.reset_traineridx()
            while (ss.update_traineridx()):
                ss.get_regressor_metrics()

if __name__ == "__main__":
    main ()
