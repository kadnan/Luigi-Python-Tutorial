# luigid --port 80
# https://bionics.it/posts/luigi-tutorial
# https://vsupalov.com/new-to-luigi-good-to-know/
# https://medium.com/@prasanth_lade/luigi-all-you-need-to-know-f1bc157b20ed (MUST READ)
# https://towardsdatascience.com/data-pipelines-luigi-airflow-everything-you-need-to-know-18dc741449b7 (To Run about Task and Targets)
# https://luigi.readthedocs.io/en/stable/tasks.html For Diagram

import time

import luigi


# Task A
class HelloWorld(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('helloworld.txt')

    def run(self):
        time.sleep(15)
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')
        time.sleep(15)


# Task B
class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return HelloWorld()

    def output(self):
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)

    def run(self):
        time.sleep(15)
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
        time.sleep(15)


if __name__ == '__main__':
    luigi.run()
