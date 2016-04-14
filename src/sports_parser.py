## Author: kidynamit
## Date: 31 March 2016

from __future__ import print_function
from json import load as json_load
from tempfile import NamedTemporaryFile

import datetime
import os

__all__ = ["SoccerParser", "NULL_STR"]

class __Season:
    """Defines the soccer season
    """
    _start = 0
    _end = 0
    MINYEAR = datetime.MINYEAR
    MAXYEAR = datetime.MAXYEAR
    def __init__ (self, start_year, min_year=1993, max_year=2015):
        if min_year:
            self.MINYEAR = min_year
        if max_year:
            self.MAXYEAR = max_year
        assert (self.MINYEAR < self.MAXYEAR)
        if type (start_year) == int:
            assert(start_year >= self.MINYEAR and start_year <= self.MAXYEAR)
            self._start = start_year
            self._end = start_year + 1
            assert (self._start < self._end)
        else:
            raise TypeError("'start_year' and 'end_year' are not integers")

    def _year_str(self, year):
        assert(year >= self.MINYEAR and year <= self.MAXYEAR)
        return "{0:04d}".format (year)


    def __str__ (self):
        return self._year_str(self._start)[-2:] + self._year_str(self._end)[-2:]

def _generate_seasons_list(start_year, end_year):
    """generates a list of _season from [start_year, end_year)
    """
    if type (start_year) == int and type(end_year) == int:
        assert(start_year < end_year)
        return list([__Season(yr) for yr in range(start_year, end_year)])
    else:
        return None

## Config data keys
FOOTBALL_DATA_HOME =    "FOOTBALL_DATA_HOME"
COLUMN_HEADERS =        "COLUMN_HEADERS"
EVALUATED_VALUES =      "EVALUATED_VALUES"
EXCLUDES =              "EXCLUDES"
BETBRAINS_AVG_MAX =     "BETBRAINS_AVERAGES_MAXIMUMS"
OVER_2POINT5_GOALS =    "OVER_2POINT5_GOALS"
ASIAN_HANDICAP =        "ASIAN_HANDICAP"

## Null
NULL =                  0.0
NULL_STR =              "None"

class _SoccerParser__SoccerCollator:
    collated_file = None
    COMMENT = "####"
    def __init__(self, collated_filename=None):
        if collated_filename:
            self.collated_file = open(collated_filename, "w")
        else:
            temporary_file = NamedTemporaryFile()
            temporary_file.close ()
            self.collated_file = open(temporary_file.name, "w")

    def __del__(self):
        self.collated_file.close ()

    def add_file(self, filename):
        new_file = open(filename, "r")
        firstLine = True
        for line in  new_file:
            if firstLine:
                line = self.COMMENT + line
                firstLine = False
            self.collated_file.write(line)
        new_file.close()

    def get_name(self):
        return self.collated_file.name

    def is_closed(self):
        return self.collated_file.closed

class SoccerParser:
    """This a csv parser class for data similar to or from
    www.football-data.co.uk
    """
    CONFIG_FILENAME = "parser.conf"
    CONFIG_DATA = {}
    _start_year = datetime.MINYEAR
    _end_year = datetime.MAXYEAR
    _seasons = []
    _soccer_collator = None
    # ordered list of column headers for post-cleaning analysis
    ALL_COLUMN_HEADERS = []
    CLEAN_COMMENT = "::::"

    __SoccerCollator = _SoccerParser__SoccerCollator

    def _load_config(self):
        with open(self.CONFIG_FILENAME, "r") as headers_file:
            self.CONFIG_DATA = json_load(headers_file)

    def _eval_data_entry(self, data_entry):
        if not data_entry or not type(data_entry) == list:
            print("error encountered parsing data entry [{0}]".format(data_entry))
            return None
        eval_entry = []
        for entry in data_entry:
            val = None
            try:
                val = eval(entry)
            except NameError:
                try:
                    val = self.CONFIG_DATA[EVALUATED_VALUES][entry]
                except KeyError:
                    pass
            except:
                pass
            finally:
                eval_entry.append(val)
        return eval_entry



    def __init__ (self, collated_filename=None, start_year=1993, end_year=2015):
        self._load_config()
        self._start_year = start_year
        self._end_year = end_year
        try:
            self._seasons = _generate_seasons_list(self._start_year, self._end_year)
        except AssertionError:
            self._seasons = None

        if collated_filename:
            self._soccer_collator = __SoccerCollator(collated_filename)
        else:
            _temp_file = NamedTemporaryFile()
            _temp_file.close()
            self._soccer_collator = __SoccerCollator(_temp_file.name)

    def __del__(self):
        del self._soccer_collator
        del self._seasons
        del self.CONFIG_DATA

    def close_collator(self):
        self._soccer_collator.collated_file.close ()

    def _add_season_directory(self, season_path):
        if os.path.isdir(season_path):
            contents = os.listdir(season_path)
            for content in contents:
                if os.path.splitext(content)[-1] == ".csv":
                    self._soccer_collator.add_file(os.path.join(season_path, content))
        else:
            print("'{0}' is not a valid directory"\
                    .format(season_path))

    def add_season_files(self):
        if self._seasons:
            for season in self._seasons:
                self._add_season_directory(\
                        os.path.join(self.CONFIG_DATA[FOOTBALL_DATA_HOME], str(season)))
            print("Season files added to '{0}'".format(self._soccer_collator.get_name()))
        else:
            print("'{0}'={1} is not set.".format("self._seasons", self._seasons))

    def parse_data_entry(self, entry):
        entry = entry.rstrip("\r\n")
        quote = entry.find ("\"")
        if quote == -1:
            return entry.split (",")
        sep = entry.find(",", quote)
        entry = entry[:sep] + entry[sep+1:]
        return entry.split(",")

    def clean_season_files(self, exc_handicap=True, exc_averages=True, exc_2_5_goals=True):
        if not self._soccer_collator.is_closed():
            self.close_collator()

        excludes = self.CONFIG_DATA[EXCLUDES]
        if exc_handicap:
            excludes.extend(self.CONFIG_DATA[ASIAN_HANDICAP])
        if exc_averages:
            excludes.extend(self.CONFIG_DATA[BETBRAINS_AVG_MAX])
        if exc_2_5_goals:
            excludes.extend(self.CONFIG_DATA[OVER_2POINT5_GOALS])

        collated_file = open(self._soccer_collator.get_name(), "r")
        _temp = NamedTemporaryFile()
        _temp.close()

        cleaning_name = collated_file.name + "-" + os.path.split(_temp.name)[1] + ".clean"
        clean_collated_file = open(cleaning_name, "w")

        self.ALL_COLUMN_HEADERS = []

        heading = {}
        column_headings =[]
        for line in collated_file:
            if line.find(self._soccer_collator.COMMENT) == 0:
                column_heading = line.lstrip(self._soccer_collator.COMMENT).rstrip(",\r\n").split(",")
                heading_vals = list([True for _ in range(len(column_heading))])
                heading = dict(zip(column_heading, heading_vals))
                for title in column_heading:
                    if title == "":
                        print("encountered a null title")
                for exc in excludes:
                    try:
                        heading[exc] = False
                    except KeyError:
                        print ("'{0}' is not a valid column header.".format(exc))
                clean_heading = []
                for title in line.lstrip(self._soccer_collator.COMMENT).rstrip(",\r\n").split(","):
                    if heading[title]:
                        clean_heading.append(title)

                self.ALL_COLUMN_HEADERS.append (clean_heading)
                line = self.CLEAN_COMMENT + (",".join(clean_heading)) + "\n"
            else:
                data_entry = self.parse_data_entry(line)
                clean_entry = self._eval_data_entry(data_entry)
                if len(clean_entry) > len(column_heading):
                    clean_entry = clean_entry[:len(column_heading)]
                elif len(clean_entry) < len(column_heading):
                    diff = len(column_heading) - len(clean_entry)
                    clean_entry.extend([None for i in range(diff)])

                data_str = []
                for title, entry in zip(column_heading, clean_entry):
                    try:
                        if heading[title]:
                            data_str.append(str(entry))
                    except:
                        print("column_heading[{2}]: {0}\n\t data_entry[{3}]: {1}"\
                                .format(column_heading, data_entry, len(column_heading), len(data_entry)))
                        os.sys.exit(1)
                line = (",".join(data_str)) + "\n"
                # for i in range(len(data_entry)):
                    # if data_entry[i] == "":
                        # data_entry[i] = NULL_STR
                # if len(data_entry) > len(column_heading):
                    # data_entry = data_entry[:len(column_heading)]
                # for i in range(len(data_entry)):
                    # try:
                        # if heading[column_heading[i]]:
                            # clean_entry.append(data_entry[i])
                    # except IndexError:
                        # print("column_heading[{2}]: {0}\n\t data_entry[{3}]: {1}"
                                # .format(column_heading, data_entry, len(column_heading), len(data_entry)))
                        # os.sys.exit(1)
            clean_collated_file.write (line)

        clean_collated_file.close()
        collated_file.close()

        os.rename(clean_collated_file.name, collated_file.name)

        print("Season files cleaned and saved to '{0}'".format(collated_file.name))

    def post_clean_analysis (self):
        # get the most descriptive column heading
        desc_heading = []
        _title_dict = {}
        for heading in self.ALL_COLUMN_HEADERS:
            if len(heading) > len(desc_heading):
                desc_heading = heading
            for title in heading:
                try:
                    _title_dict[title] += 1
                except KeyError:
                    _title_dict[title] = 1

        for title in desc_heading:
            _title_dict[title] = None

        for heading in self.ALL_COLUMN_HEADERS:
            for title in heading:
                if _title_dict[title]:
                    desc_heading.append(title)
                    _title_dict[title] = None
        del _title_dict

        heading = dict(zip(desc_heading, range(len(desc_heading))))

        _tmp = NamedTemporaryFile()
        _tmp.close ()

        if not self._soccer_collator.collated_file.closed:
            self.close_collator()
        clean_file = open(self._soccer_collator.get_name(), "r")
        post_file = open(clean_file.name + "-" + os.path.split(_tmp.name)[-1] + ".post", "w")

        post_file.write((",".join(desc_heading)) + "\n")
        current_heading = []
        null_entry = list([str(None) for i in range(len(desc_heading))])

        invalid_headings = {}
        for line in clean_file:
            if line.find(self.CLEAN_COMMENT) == 0:
                current_heading = line.rstrip(",\n").lstrip(self.CLEAN_COMMENT).split(",")
            else:
                clean_entry=line.rstrip("\r\n").split(",")
                try:
                    assert(len(clean_entry) == len(current_heading))
                except AssertionError:
                    print("current_heading[{0}]: {1}\n\t clean_entry[{2}]: {3}"\
                            .format(len(current_heading), current_heading, len(clean_entry), clean_entry))
                    os.sys.exit(1)
                post_entry = null_entry
                for title, field in zip(current_heading, clean_entry):
                    try:
                        post_entry[heading[title]] = field
                    except KeyError:
                        try:
                            invalid_headings[title] += 1
                        except KeyError:
                            invalid_headings[title] = 1
                        # print("'{0}' is not a valid column heading".format(title))
                        # os.sys.exit(1)
                post_file.write((",".join(post_entry)) + "\n")
        if len(invalid_headings.keys()):

            print ("Invalid headings and occurrences:\n\t{0}".format(invalid_headings))
            for keys in invalid_headings.keys():
                try:
                    self.CONFIG_DATA[COLUMN_HEADERS][keys]
                except:
                    print ("'{0}' is not a valid column heading".format(keys))

        clean_file.close()
        post_file.close()

        os.rename(post_file.name, clean_file.name)

        print("Post-cleaning analysis performed successfully and saved to '{0}'".format(clean_file.name))


def main():
    """
    This program parses data from www.football-data.co.uk
    """
    sp = SoccerParser(collated_filename="../out/0015.csv", start_year=2000)
    sp.add_season_files()
    sp.clean_season_files()
    sp.post_clean_analysis()

if __name__ == "__main__":
    main ()
