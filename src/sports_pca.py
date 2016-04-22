
## Author:  kidynamit
## Date:    15-04-16

from __future__ import print_function

from sys import exit
from os import rename

from numpy import array, cov, corrcoef, hstack
from numpy.linalg import eig

__all__ = [ "SoccerPCA"]

class SoccerPCA:
    __file = None
    __fidx = 0
    __ncomponents = 0
    __features = None
    __labels = None

    def __init__(self, filename, features_idx):
        try:
            self.__file = open(filename, "r")
            self.__file.close()
        except:
            print("ERROR encountered reading the file {0}".format(filename))
            exit(1)
        self.__fidx = features_idx

    def __load_data(self):
        if not self.__file:
            return None

        self.__file = open(self.__file.name, "r")
        self.__features = list()
        self.__labels = list()

        features_size = None

        self.__file.readline()
        for line in self.__file:
            entry = line.rstrip("\r\n").split(",")
            entry = [ eval(x) for x in entry ]
            try:
                self.__features.append(entry[self.__fidx:])
                self.__labels.append(entry[:self.__fidx])
                if features_size:
                    x = len(entry) - self.__fidx
                    if not features_size == x:
                        print("ERROR encountered the feature sizes don't match")
                        exit(1)
                else:
                    features_size = len(entry) - self.__fidx
            except:
                print("ERROR encountered. self.__fidx[{0}] is invalid.".format(self.__fidx))
                exit(1)
        self.__file.close()

        return array( self.__features ), features_size

    def __save_to_file(self, ncomponents):
        self.__file = open(self.__file.name, "r")
        save_file = open(self.__file.name + ".sav" , "w")

        features_heading = ["Comp{0}".format(i + 1) for i in range(ncomponents) ]
        labels_heading = (self.__file.readline().rstrip("\r\n").split(","))[:self.__fidx]
        heading = ",".join(labels_heading + features_heading)
        save_file.write(heading + "\n")

        for label, feature in zip(self.__labels, self.__features):
            label = [ str(l) for l in label ]
            feature = [ str(f) for f in feature ]
            save_file.write((",".join(label + feature)) + "\n")

        save_file.close()
        self.__file.close()

        rename(save_file.name, self.__file.name)

    def __normalize(self, X, p):
        if X == None or p == None:
            return None

        xcopy = array(X)
        p = float(p)
        assert(len(xcopy.shape) == 2)

        for i in range(len(xcopy)):
            _norm = 0.0
            if p == float("inf"):
                _norm = max(xcopy[i])
            else:
                for r in xcopy[i]:
                    if r == None:
                        r = 0.0
                    _norm += pow(abs(r), p)
                _norm = pow(_norm, 1.0/p)

            for j in range(len(xcopy[i])):
                if xcopy[i][j] == None:
                    xcopy[i][j] = 0.0
                xcopy[i][j] = xcopy[i][j]/float(_norm)
        return xcopy

    def __optimal_components(self, values, confidence):
        values_norm = self.__normalize(array([values]), 1)
        values_norm = values_norm[0]

        _cumsum = 0.0
        for val, k in zip(values, range(len(values))):
            _cumsum += val
            if _cumsum > confidence:
                return k, _cumsum
        return len(values) - 1, _cumsum

    def perform_pca(self, ncomponents, p=2, confidence=0.999, override_components=True):
        if not ncomponents or ncomponents == 1:
            return None
        if self.__fidx < 0:
            return None
        if not confidence:
            return None
        confidence = float(confidence)
        if confidence < 0.0 or confidence > 1.0:
            return None

        X, features_size = self.__load_data()
        X_norm = self.__normalize(X, p)
        # cov_mat = cov(X_norm.T)
        corr_mat = corrcoef(X_norm.T)
        eig_values, eig_vectors = eig(corr_mat)
        eig_pairs = [(abs(eig_values[i]), eig_vectors[:,i]) for i in range(len(eig_values))]

        eig_pairs.sort()
        eig_pairs.reverse()

        ocomponents, actual_confidence = self.__optimal_components(eig_values, confidence)

        if ncomponents < ocomponents:
            print("WARNING the principal components [{0}] are less than the optimum components [{1}] \
                    which gives a confidence of [{2:2f}%]".format(ncomponents, ocomponents, confidences * 100.0))

            if override_components:
                ncomponents = ocomponents
            print ("WARNING using the principal components set as [{0}]".format(ncomponents))


        proj_matrix = hstack(tuple( eig_pairs[i][1].reshape(features_size, 1) for i in range(ncomponents)))
        self.__features = X_norm.dot(proj_matrix)
        self.__save_to_file(ncomponents)
        print("PCA completed successfully")

    def __del__(self):
        if not self.__file.closed:
            self.__file.close()

def main():
    pass

if __name__ == "__main__":
    main()
