class DataType:
    name = None
    py_type = None
    struct_format = None

    def validate(self, value):
        if not isinstance(value, self.py_type):  # type: ignore
            raise TypeError(f"Valor {value!r} não é do tipo {self.name}")


class _INT(DataType):
    name = "INT"
    py_type = int
    struct_format = "<i"


class _TEXT(DataType):
    name = "TEXT"
    py_type = str


class _BOOL(DataType):
    name = "BOOL"
    py_type = bool
    struct_format = "?"


INT = _INT()
TEXT = _TEXT()
BOOL = _BOOL()


class Column:
    def __init__(self, name, dtype, nullable=False):
        self.name = name
        self.dtype = dtype
        self.nullable = nullable

    def validate(self, value):
        if value is None:
            if self.nullable:
                return
            raise ValueError(f"Coluna '{self.name}' não aceita NULL")

        self.dtype.validate(value)


class Schema:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

        self._validate_definition()

    def _validate_definition(self):
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("Colunas duplicadas no schema")

    def validate(self, record: dict):
        record_keys = set(record.keys())
        schema_keys = {c.name for c in self.columns}

        missing = schema_keys - record_keys
        extra = record_keys - schema_keys

        if missing:
            raise ValueError(f"Colunas faltando: {missing}")

        if extra:
            raise ValueError(f"Colunas desconhecidas: {extra}")

        for column in self.columns:
            value = record[column.name]
            column.validate(value)

    def to_dict(self):
        return {
            "name": self.name,
            "columns": [
                {"name": c.name, "dtype": c.dtype.name, "nullable": c.nullable}
                for c in self.columns
            ],
        }

    @staticmethod
    def from_dict(data):
        type_map = {
            "INT": INT,
            "TEXT": TEXT,
            "BOOL": BOOL,
        }

        cols = [
            Column(c["name"], type_map[c["dtype"]], c["nullable"])
            for c in data["columns"]
        ]

        return Schema(data["name"], cols)
