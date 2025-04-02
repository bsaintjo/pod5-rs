class FrameReader:
    """
    A class representing a reader over a POD5 file.

    :param path: File path to POD5 file
    """
    def __init__(self, path: str) -> None: ...

    # @classmethod
    # def from_unique_name(cls, name: str) -> 'FrameReader':
    #     """
    #     Creates a Car based on unique name

    #     :param name: model name of a car to be created
    #     :return: a Car instance with default data
    #     """

    def reads(self):
        """
        Testing PYI files

        :return: the name of the color our great algorithm thinks is the best for this car
        """
