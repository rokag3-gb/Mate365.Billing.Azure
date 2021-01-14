"""
원본 : https://github.com/pedroburon/dotenv
"""
class Dotenv(dict):
    def __init__(self, file_path):
        self.file_path = file_path
        super(Dotenv, self).__init__(**self.__create_dict())

    def __create_dict(self):
        with open(self.file_path, 'r') as dotenv:
            variables = {}
            for line in dotenv.readlines():
                variables.update(self.__parse_line(line))
            return variables

    def __parse_line(self, line):
        if line.lstrip().startswith('#'):
            # 주석 discard and return nothing
            return {}
        if line.lstrip():
            # find the second occurence of a quote mark:
            quote_delimit = max(line.find('\'', line.find('\'') + 1),
                                line.find('"', line.rfind('"')) + 1)
            # find first comment mark after second quote mark
            comment_delimit = line.find('#', quote_delimit)
            line = line[:comment_delimit]
            key, value = map(lambda x: x.strip().strip('\'').strip('"'),
                             line.split('=', 1))
            return {key: value}
        else:
            return {}

    def __persist(self):
        with open(self.file_path, 'w') as dotenv:
            for key, value in self.items():
                dotenv.write("%s=%s\n" % (key, value))

    def __setitem__(self, key, value):
        super(Dotenv, self).__setitem__(key, value)
        self.__persist()

    def __delitem__(self, key):
        super(Dotenv, self).__delitem__(key)
        self.__persist()


def get_variables_from_dotenv(file_path):
    return Dotenv(file_path)