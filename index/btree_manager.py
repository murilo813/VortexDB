import struct
from index.btree import BTreeNode, INDEX_HEADER_FMT, INDEX_HEADER_SIZE
from index.offsets import OFFSET_KEY_COUNT, OFFSET_PARENT_ID, OFFSET_NEXT_PAGE


class BTreeManager:
    def __init__(self, buffer_manager, root_page_id=None, on_root_change=None):
        self.buffer_manager = buffer_manager
        self._root_page_id = root_page_id
        self.on_root_change = on_root_change

    @property
    def root_page_id(self):
        return self._root_page_id

    @root_page_id.setter
    def root_page_id(self, new_id):
        if self._root_page_id != new_id:
            self._root_page_id = new_id
            if self.on_root_change:
                self.on_root_change(new_id)

    def _load_node(self, page_id):
        buffer = self.buffer_manager.get_page(page_id)
        return BTreeNode(page_id, buffer, is_leaf=None)

    def search(self, key):
        if self.root_page_id is None:
            return None

        current_node_id = self.root_page_id
        while True:
            node = self._load_node(current_node_id)
            key_count = struct.unpack_from("H", node.buffer, OFFSET_KEY_COUNT)[0]
            idx = node.find_slot(key)

            if node.is_leaf:
                if idx < key_count and node.get_key(idx) == key:
                    offset = INDEX_HEADER_SIZE + (idx * node.entry_size)
                    data = struct.unpack_from(node.entry_fmt, node.buffer, offset)
                    self.buffer_manager.unpin(current_node_id)
                    return (data[1], data[2])
                self.buffer_manager.unpin(current_node_id)
                return None
            else:
                if idx == 0 and key < node.get_key(0):
                    next_node_id = struct.unpack_from(
                        "Q", node.buffer, OFFSET_NEXT_PAGE
                    )[0]
                else:
                    use_idx = idx if idx < key_count else key_count - 1
                    offset = INDEX_HEADER_SIZE + (use_idx * node.entry_size)
                    next_node_id = struct.unpack_from(
                        node.entry_fmt, node.buffer, offset
                    )[1]

                self.buffer_manager.unpin(current_node_id)
                current_node_id = next_node_id

    def insert(self, key, heap_page_id, slot_id):
        if self.root_page_id is None:
            page_id, buffer = self.buffer_manager.create_page("index")
            self.root_page_id = page_id

            node = BTreeNode(page_id, buffer, is_leaf=True)
            node.write_header(is_leaf=True, key_count=1)
            node.insert_entry(key, (heap_page_id, slot_id))

            self.buffer_manager.mark_dirty(page_id)
            self.buffer_manager.unpin(page_id)
            return

        self._insert_recursive(self.root_page_id, key, heap_page_id, slot_id)

    def _insert_recursive(self, current_node_id, key, heap_page_id, slot_id):
        node = self._load_node(current_node_id)

        if node.is_leaf:
            try:
                node.insert_entry(key, (heap_page_id, slot_id))
                self.buffer_manager.mark_dirty(current_node_id)
                self.buffer_manager.unpin(current_node_id)
            except ValueError:
                # pagina cheia, split!
                self.buffer_manager.unpin(current_node_id)
                self._split_leaf(current_node_id, key, heap_page_id, slot_id)

        else:
            idx = node.find_slot(key)
            key_count = struct.unpack_from("H", node.buffer, OFFSET_KEY_COUNT)[0]

            if idx == 0 and key < node.get_key(0):
                child_id = struct.unpack_from("Q", node.buffer, OFFSET_NEXT_PAGE)[0]
            else:
                search_idx = idx if idx < key_count else key_count - 1
                offset = INDEX_HEADER_SIZE + (search_idx * node.entry_size)
                data = struct.unpack_from(node.entry_fmt, node.buffer, offset)
                child_id = data[1]

            self.buffer_manager.unpin(current_node_id)
            self._insert_recursive(child_id, key, heap_page_id, slot_id)

    def _split_leaf(self, node_id, key, heap_page_id, slot_id):
        old_node = self._load_node(node_id)
        old_key_count = struct.unpack_from("H", old_node.buffer, OFFSET_KEY_COUNT)[0]
        parent_id = struct.unpack_from("Q", old_node.buffer, OFFSET_PARENT_ID)[0]
        old_next = struct.unpack_from("Q", old_node.buffer, OFFSET_NEXT_PAGE)[0]

        new_node_id, new_buffer = self.buffer_manager.create_page("index")
        new_node = BTreeNode(new_node_id, new_buffer, is_leaf=True)

        mid = old_key_count // 2
        num_to_move = old_key_count - mid

        for i in range(num_to_move):
            offset = INDEX_HEADER_SIZE + ((mid + i) * old_node.entry_size)
            data = struct.unpack_from(old_node.entry_fmt, old_node.buffer, offset)

            # chave=data[0], RID=(data[1], data[2])
            new_node.insert_entry(data[0], (data[1], data[2]))

        tail_offset = INDEX_HEADER_SIZE + (mid * old_node.entry_size)
        size_to_clean = len(old_node.buffer) - tail_offset
        old_node.buffer[tail_offset : tail_offset + size_to_clean] = bytearray(
            size_to_clean
        )

        struct.pack_into("H", old_node.buffer, OFFSET_KEY_COUNT, mid)

        struct.pack_into("Q", old_node.buffer, OFFSET_NEXT_PAGE, new_node_id)
        new_node.write_header(
            is_leaf=True, key_count=num_to_move, parent=parent_id, next_pg=old_next
        )

        if key < new_node.get_key(0):
            old_node.insert_entry(key, (heap_page_id, slot_id))
        else:
            new_node.insert_entry(key, (heap_page_id, slot_id))

        self.buffer_manager.mark_dirty(node_id)
        self.buffer_manager.mark_dirty(new_node_id)

        new_key_for_parent = new_node.get_key(0)

        if node_id == self.root_page_id:
            self._create_new_root(node_id, new_key_for_parent, new_node_id)
        else:
            self._insert_into_parent(parent_id, new_key_for_parent, new_node_id)

        self.buffer_manager.unpin(node_id)
        self.buffer_manager.unpin(new_node_id)

    def _create_new_root(self, left_child_id, key, right_child_id):
        new_root_id, buffer = self.buffer_manager.create_page("index")

        node = BTreeNode(new_root_id, buffer, is_leaf=False)
        node.write_header(is_leaf=False, key_count=0)

        struct.pack_into("Q", buffer, OFFSET_NEXT_PAGE, left_child_id)
        node.insert_entry(key, (right_child_id,))

        self.root_page_id = new_root_id

        for child_id in [left_child_id, right_child_id]:
            child_buf = self.buffer_manager.get_page(child_id)
            struct.pack_into("Q", child_buf, OFFSET_PARENT_ID, new_root_id)
            self.buffer_manager.mark_dirty(child_id)
            self.buffer_manager.unpin(child_id)

        self.buffer_manager.mark_dirty(new_root_id)
        self.buffer_manager.unpin(new_root_id)

    def _insert_into_parent(self, parent_id, key, new_child_id):
        parent_node = self._load_node(parent_id)
        try:
            parent_node.insert_entry(key, (new_child_id,))
            self.buffer_manager.mark_dirty(parent_id)
            self.buffer_manager.unpin(parent_id)
        except ValueError:
            # se o pai também estiver cheio, split!
            self.buffer_manager.unpin(parent_id)
            self._split_internal(parent_id, key, new_child_id)

    def _split_internal(self, node_id, key, child_id):
        old_node = self._load_node(node_id)
        old_key_count = struct.unpack_from("H", old_node.buffer, OFFSET_KEY_COUNT)[0]
        parent_id = struct.unpack_from("Q", old_node.buffer, OFFSET_PARENT_ID)[0]

        new_node_id, new_buffer = self.buffer_manager.create_page("index")
        new_node = BTreeNode(new_node_id, new_buffer, is_leaf=False)

        mid = old_key_count // 2
        up_key = old_node.get_key(mid)

        offset_up_right_ptr = INDEX_HEADER_SIZE + (mid * old_node.entry_size) + 4
        ptr_for_new_pointer0 = struct.unpack_from(
            "I", old_node.buffer, offset_up_right_ptr
        )[0]

        struct.pack_into("Q", new_node.buffer, OFFSET_NEXT_PAGE, ptr_for_new_pointer0)

        num_to_move = old_key_count - (mid + 1)
        for i in range(num_to_move):
            src_idx = mid + 1 + i
            off = INDEX_HEADER_SIZE + (src_idx * old_node.entry_size)
            data = struct.unpack_from(old_node.entry_fmt, old_node.buffer, off)
            new_node.insert_entry(data[0], (data[1],))

        tail_offset = INDEX_HEADER_SIZE + (mid * old_node.entry_size)
        size_to_clean = len(old_node.buffer) - tail_offset
        old_node.buffer[tail_offset : tail_offset + size_to_clean] = bytearray(
            size_to_clean
        )

        struct.pack_into("H", old_node.buffer, OFFSET_KEY_COUNT, mid)

        new_node.write_header(is_leaf=False, key_count=num_to_move, parent=parent_id)

        self._update_children_parent(new_node_id, new_node)

        if key < up_key:
            old_node.insert_entry(key, (child_id,))
        else:
            new_node.insert_entry(key, (child_id,))

        self.buffer_manager.mark_dirty(node_id)
        self.buffer_manager.mark_dirty(new_node_id)

        if node_id == self.root_page_id:
            self._create_new_root(node_id, up_key, new_node_id)
        else:
            self._insert_into_parent(parent_id, up_key, new_node_id)

        self.buffer_manager.unpin(node_id)
        self.buffer_manager.unpin(new_node_id)

    def _update_children_parent(self, new_parent_id, parent_node):
        left_ptr = struct.unpack_from("Q", parent_node.buffer, OFFSET_NEXT_PAGE)[0]
        self._set_parent(left_ptr, new_parent_id)

        key_count = struct.unpack_from("H", parent_node.buffer, OFFSET_KEY_COUNT)[0]
        for i in range(key_count):
            offset = INDEX_HEADER_SIZE + (i * parent_node.entry_size)
            child_id = struct.unpack_from(
                parent_node.entry_fmt, parent_node.buffer, offset
            )[1]
            self._set_parent(child_id, new_parent_id)

    def _set_parent(self, child_page_id, new_parent_id):
        child_node = self._load_node(child_page_id)
        struct.pack_into("Q", child_node.buffer, OFFSET_PARENT_ID, new_parent_id)
        self.buffer_manager.mark_dirty(child_page_id)
        self.buffer_manager.unpin(child_page_id)
