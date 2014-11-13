def encode_unicode(value):
    if isinstance(value, unicode):
        return value.encode('utf-8')
    return value


class BaseRowProcessor:
    def __init__(self, table, func_type_map):
        self.func_type_map = func_type_map
        self.table = table
        self.col_type_map = [(x.name, x.type) for x in self.table.columns]
        self.result_name_type_map = self._init_result_name_type_map()

    def _find_needed_type(self, type_):
        for needed_type in self.func_type_map.keys():
            if isinstance(type_, needed_type):
                return needed_type
        return None

    def _init_result_name_type_map(self):
        result_name_type_map = {}
        for rp in self.col_type_map:
            result_name_type_map[rp[0]] = self._find_needed_type(rp[1])
        return result_name_type_map

    def _apply_func_to_needed_columns(self, row):
        res = []
        for field in row:
            if self.result_name_type_map[field[0]]:
                res.append((field[0], self.func_type_map[self.result_name_type_map[field[0]]](field[1])))
            else:
                res.append((field[0], field[1]))
        return res


class PreInsertDataProcessor(BaseRowProcessor):
    def __init__(self, target_table, func_type_map, row):
        BaseRowProcessor.__init__(self, target_table, func_type_map)
        self.row = row
        self.func_type_map = func_type_map
        self.col_type_map = [(x.name, x.type) for x in target_table.columns]

    def process_row(self):
        result_tuples = [self._apply_func_to_needed_columns(x) for x in [self.row]]
        result_list = [x[1] for x in result_tuples[0]]
        return result_list


class PostSelectDataProcessor(BaseRowProcessor):
    def __init__(self, source_table, func_type_map, result_proxy, batch_size):
        BaseRowProcessor.__init__(self, source_table, func_type_map)
        self.result_proxy = result_proxy
        self.func_type_map = func_type_map
        self.col_type_map = [(x.name, x.type) for x in source_table.columns]
        self.col_names = [x[0] for x in self.col_type_map]
        self.batch_size = batch_size

    def process_rows(self):
        result = []
        if not self.result_proxy.closed:
            next_rows = self.result_proxy.fetchmany(self.batch_size)
        else:
            return None
        col_set = [zip(self.col_names, (encode_unicode(y) for y in x)) for x in next_rows]
        result = [self._apply_func_to_needed_columns(x) for x in [x for x in col_set]]
        return result

    def __iter__(self):
        while True:
            rows = self.process_rows()
            if rows is None:
                raise StopIteration
            else:
                yield rows