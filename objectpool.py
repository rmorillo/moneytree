class ObjectPool:
    def __init__(self, capacity, item_init_lambda=lambda: None):
        self._capacity = capacity
        self._pool = [item_init_lambda() for _ in range(capacity)]
        self._current_position = 0
        self._sequence = 0
        self._last_position = -1
        self._has_rolled_over = False
        self._offset = 1
        self._length = 0

# Public properties

    @property
    def current(self):
        if self._length > 0:
            return self._pool[self._last_position]
        else:
            raise Exception("There is no current value.  You can use 'has_current' property to check if current value is accessible.")

    @property
    def previous(self):
        if self.has_previous:
            return self._pool[self._get_absolute_index(1)]
        else:
            raise Exception("There is no previous value.  You can use 'has_previous' property to check if previous value is accessible.")

    @property
    def length(self):
        return self._length

    @property
    def sequence(self):
        return self._sequence

    @property
    def has_current(self):
        return self._length > 0

    @property
    def has_previous(self):
        return self._length > 1

    @property
    def capacity(self):
        return self._capacity

# Public methods

    def is_valid_index(self, index):
        return self._length > index

    def assign(self, value):
        self._pool[self._current_position] = value
        self._move_forward()

    def update(self, update_lambda):
        update_lambda(self._pool[self._current_position])
        self._move_forward()

    def read_back(self, count=None, item_lambda=None):
        if count is None:
            count = self.length
        if self._length >= count:
            for i in range(count):
                if item_lambda is None:
                    yield self[i]
                else:
                    yield item_lambda(self[i])
        else:
            raise Exception("Pool index is out of range.  You can only read back maximum of {} items. \
                You can use 'is_valid_index()' method or 'length' property to validate your 'count' parameter value.".format(self._length))
				
    def list_back(self, count=None, item_lambda=None):
        return list(self.read_back(count, item_lambda))

# Internal methods

    def _get_absolute_index(self, relative_index):
        target_index = self._last_position - relative_index
        if target_index < 0:
            absolute_index = target_index + self._capacity
            if self._has_rolled_over and absolute_index > self._last_position:
                target_index = absolute_index
            else:
                raise Exception("Pool index is out of range.  You can only use index values from 0 to {}"
                                .format(self._length - 1))

        return target_index

    def _move_forward(self):
        self._last_position = self._current_position
        self._sequence += 1
        if not self._has_rolled_over:
            self._length += 1
        if self._current_position < (self._capacity - 1):
            self._current_position += self._offset
        else:
            self._current_position = 0
            self._has_rolled_over = True

# Overridden methods

    def __getitem__(self, index):
        return self._pool[self._get_absolute_index(index)]
