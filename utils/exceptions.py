class FileFormatError(Exception):
    def __init__(self, message="Formato de archivo incorrecto"):
        self.message = message
        super().__init__(self.message)
