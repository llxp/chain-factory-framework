class ArgumentExcluder:
    def __init__(self, arguments):
        self.arguments = arguments
        self.arguments_copy = dict(arguments)

    def argument_excluded(self, argument, excluded_argument):
        return argument != "exclude" and argument == excluded_argument

    def should_exclude_any_arguments(self):
        return self.arguments and "exclude" in self.arguments

    def excluded_arguments(self):
        return self.arguments["exclude"]

    def del_argument(self, argument):
        del self.arguments[argument]

    def check_exclude_argument(self, argument, excluded_argument):
        if self.argument_excluded(argument, excluded_argument):
            self.del_argument(argument)

    def exclude(self):
        if self.should_exclude_any_arguments():
            self._exclude_arguments()

    def _exclude_arguments(self):
        for argument in self.arguments_copy:
            for excluded_argument in self.excluded_arguments():
                self.check_exclude_argument(argument, excluded_argument)
