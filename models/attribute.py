class Attribute:
    def __init__(
        self, column_name, display_name=None, value=None, visible=True
    ) -> None:
        self.column_name = column_name
        self.display_name = display_name or column_name
        self.value = value
        self.visible = visible
