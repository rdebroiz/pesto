from pprint import pformat


class Scope():
    name = None
    expression = None
    values = None

    def __init__(self, name, expression, values):
        self.name = name
        self.expression = expression
        self.values = values

    def __str__(self):
        return ("name: {0}\nreg-exp: {1}\nvalues:\n{2}"
                "".format(self.name, self.expression, pformat(self.values)))

    def __repr__(self):
        return "\n[--\n{0}\n--]".format(self.__str__())
