class ExecutorError(Exception):
    pass


class CheckError(ExecutorError):
    pass


class TypeCheckError(CheckError):
    pass


class RangeCheckError(CheckError):
    pass
