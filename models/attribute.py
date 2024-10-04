from typing import Any, Callable, List


class Attribute:
    def __init__(
        self,
        column_name,
        display_name=None,
        value=None,
        visible=4,
        editable=1,
        pre_processors: List[Callable] = [],
    ) -> None:
        self.column_name = column_name
        self.display_name = display_name or column_name
        self.value = value
        self.visible = visible
        self.editable = editable
        self.pre_processors = pre_processors

    def run_preprocessor(self, pre_processor: Callable) -> None:
        if self.value != None:
            self.value = pre_processor(self.value)

    def run_preprocessors(self) -> None:
        if self.pre_processors:
            for pre_processor in self.pre_processors:
                self.run_preprocessor(pre_processor)

    def set_value(self, value: Any) -> None:
        self.value = value
        self.run_preprocessors()
