from mrjob.job import MRJob
from mrjob.step import MRStep
import re


REGEX_ONLY_WORDS = "[\w']+"


class MRDataMining(MRJob):


    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words, reducer = self.reducer_count_words)
        ]


    def mapper_get_words(self, _, line):
        words = re.findall(REGEX_ONLY_WORDS, line)
        for word in words:
            yield word.lower(), 1


    def reducer_count_words(self, word, values):
        yield word, sum(values)


if __name__ == '__main__':
    MRDataMining.run()
