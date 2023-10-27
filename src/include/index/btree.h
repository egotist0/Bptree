#pragma once

#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>

#include "buffer/buffer_manager.h"
#include "common/defer.h"
#include "common/macros.h"
#include "storage/segment.h"

#define UNUSED(p) ((void)(p))

namespace buzzdb {

template <typename KeyT, typename ValueT, typename ComparatorT,
          uint64_t PageSize>
struct BTree : public Segment {
  struct Node {
    /// The level in the tree.
    uint16_t level;

    /// The number of children.
    uint16_t count;

    // The id of the parent node.
    std::optional<uint64_t> parent;

    // Constructor
    Node(uint16_t level, uint16_t count) : level(level), count(count) {}

    /// Is the node a leaf node?
    bool is_leaf() const { return level == 0; }
  };

  struct InnerNode : public Node {
    /// The capacity of a node.
    static constexpr uint32_t kCapacity =
        (PageSize / (sizeof(KeyT) + sizeof(ValueT))) - 2;

    /// The keys.
    KeyT keys[kCapacity];

    /// The children.
    uint64_t children[kCapacity + 1];

    /// Constructor.
    InnerNode() : Node(0, 0) {}

    /// Get the index of the first key that is not less than than a provided
    /// key.
    /// @param[in] key          The key that should be searched.
    std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
      auto low = 0, high = this->count - 1;
      std::pair<uint32_t, bool> res;
      std::optional<uint32_t> index;

      if (high == 1 && !ComparatorT()(this->keys[0], key)) {
        res = {0, true};
        return res;
      }

      while (low <= high) {
        auto mid = low + (high - low) / 2;
        if (key == this->keys[mid]) {
          res = {static_cast<uint32_t>(mid), true};
          return res;
        } else if (key > this->keys[mid])
          low = mid + 1;
        else {
          index = mid;
          high = mid - 1;
        }
      }
      if (index)
        res = {*index, true};
      else
        res = {0, false};

      return res;
    }

    /// Insert a key.
    /// @param[in] key          The separator that should be inserted.
    /// @param[in] split_page   The id of the split page that should be
    /// inserted.
    void insert(const KeyT &key, uint64_t split_page,
                const std::optional<uint64_t> &leftNode) {
      std::optional<int> index;
      std::vector<KeyT> tmp_keys;
      std::vector<uint64_t> tmp_children;

      for (int i = 0; i < this->count - 1; i++) {
        tmp_keys.emplace_back(this->keys[i]);
        tmp_children.emplace_back(this->children[i]);
        if (!ComparatorT()(this->keys[i], key) && !index) index = i;
      }
      /// if the key already exists, overwrites its value
      tmp_children.emplace_back(this->children[this->count - 1]);
      this->children[this->count - 1] = 0;
      if (!index) {
        tmp_keys.insert(tmp_keys.begin() + (this->count - 1), key);
        tmp_children.insert(tmp_children.begin() + this->count, split_page);
      } else {
        tmp_keys.insert(tmp_keys.begin() + *index, key);
        if (leftNode) index = *index + 1;
        tmp_children.insert(tmp_children.begin() + *index, split_page);
      }
      this->count++;
      for (int i = 0; i < static_cast<int>(tmp_keys.size()); i++)
        this->keys[i] = tmp_keys[i];
      for (int i = 0; i < static_cast<int>(tmp_children.size()); i++)
        this->children[i] = tmp_children[i];
    }

    /// Split the node.
    /// @param[in] buffer       The buffer for the new page.
    /// @return                 The separator key.
    KeyT split(std::byte *buffer) {
      auto mid_index = 1 + ((this->kCapacity - 1) / 2);
      auto new_inner_node = new (buffer) InnerNode();
      int start = 0;
      KeyT separator_key = 0;

      for (int i = mid_index; i < static_cast<int>(this->kCapacity) + 1; i++) {
        if (!start)
          separator_key =
              static_cast<KeyT>((this->keys[i] + this->keys[i - 1]) / 2);

        new_inner_node->keys[start] = this->keys[i];
        new_inner_node->children[start] = this->children[i];
        this->keys[i - !start] = 0;
        this->children[i] = 0;
        this->count--;
        start++;

        if (!start) {
          i++;
          new_inner_node->children[start] = this->children[i];
          this->children[i] = 0;
          this->count--;
          start++;
        }
      }

      new_inner_node->count = start;
      return separator_key;
    }

    /// Returns the keys.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<KeyT> get_key_vector() {
      std::vector<KeyT> key_vector = std::vector<KeyT>(this->count);
      for (uint64_t i = 0; i < this->count; i++)
        key_vector.emplace_back(this->keys[i]);
      return key_vector;
    }

    /// Returns the child page ids.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<uint64_t> get_child_vector() {
      std::vector<uint64_t> child_vector = std::vector<KeyT>(this->count + 1);
      for (uint64_t i = 0; i <= this->count; i++)
        child_vector.emplace_back(this->children[i]);
      return child_vector;
    }
  };

  struct LeafNode : public Node {
    /// The capacity of a node.
    static constexpr uint32_t kCapacity =
        (PageSize / (sizeof(KeyT) + sizeof(ValueT))) - 2;

    /// The keys.
    KeyT keys[kCapacity];

    /// The values.
    ValueT values[kCapacity];

    /// Constructor.
    LeafNode() : Node(0, 0) {}

    /// Insert a key.
    /// @param[in] key          The key that should be inserted.
    /// @param[in] value        The value that should be inserted.
    void insert(const KeyT &key, const ValueT &value) {
      if (!this->count) {
        this->keys[this->count] = key;
        this->values[this->count] = value;
        this->count++;
        return;
      }

      std::vector<KeyT> tmp_keys;
      std::vector<KeyT> tmp_values;
      std::optional<int> index;
      for (int i = 0; i < this->count; i++) {
        tmp_keys.emplace_back(this->keys[i]);
        tmp_values.emplace_back(this->values[i]);
        if (!ComparatorT()(this->keys[i], key) && !index) index = i;
      }
      if (!index) {
        index = this->count;
        /// Key already exists, overwrite value
        if (tmp_keys[*index - 1] == key)
          tmp_values[*index - 1] = value;
        else {
          tmp_keys.insert(tmp_keys.begin() + *index, key);
          tmp_values.insert(tmp_values.begin() + *index, value);
          this->count++;
        }
      } else {
        if (tmp_keys[*index] == key)
          tmp_values[*index] = value;
        else {
          tmp_keys.insert(tmp_keys.begin() + *index, key);
          tmp_values.insert(tmp_values.begin() + *index, value);
          this->count++;
        }
      }

      for (int i = 0; i < static_cast<int>(tmp_keys.size()); i++) {
        this->keys[i] = tmp_keys[i];
        this->values[i] = tmp_values[i];
      }
    }

    /// Split the node.
    /// @param[in] buffer       The buffer for the new page.
    /// @return                 The separator key.
    KeyT split(std::byte *buffer) {
      auto new_leaf_node = new (buffer) LeafNode();
      int index = 0;
      for (int i = 1 + ((this->kCapacity - 1) / 2);
           i < static_cast<int>(this->kCapacity); i++) {
        new_leaf_node->keys[index] = this->keys[i];
        new_leaf_node->values[index] = this->values[i];
        this->count--;
        index++;
      }
      new_leaf_node->count = index;
      return this->keys[index - 1];
    }

    /// Returns the keys.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<KeyT> get_key_vector() {
      std::vector<KeyT> key_vector = std::vector<KeyT>(this->count);
      for (uint64_t i = 0; i < this->count; i++)
        key_vector.emplace_back(this->keys[i]);
      return key_vector;
    }

    /// Returns the child page ids.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<uint64_t> get_child_vector() {
      std::vector<uint64_t> child_vector = std::vector<KeyT>(this->count + 1);
      for (uint64_t i = 0; i <= this->count; i++)
        child_vector.emplace_back(this->children[i]);
      return child_vector;
    }
  };

  /// The root.
  std::optional<uint64_t> root;

  /// Is the tree empty?
  bool isEmpty = true;

  /// The deleted keys.
  std::set<KeyT> deleted;

  /// The level of the root.
  uint16_t rootLevel = 0;

  /// Next page id.
  /// You don't need to worry about about the page allocation.
  /// Just increment the next_page_id whenever you need a new page.
  uint64_t next_page_id;

  /// Constructor.
  BTree(uint16_t segment_id, BufferManager &buffer_manager)
      : Segment(segment_id, buffer_manager) {}

  /// Lookup an entry in the tree.
  /// @param[in] key      The key that should be searched.
  std::optional<ValueT> lookup(const KeyT &key) {
    std::optional<ValueT> res;
    if (this->deleted.find(key) != this->deleted.end() || !this->root)
      return res;

    auto current_page_id = *this->root;
    int next = 0;
    uint64_t previous_page_id = current_page_id;

    while (true) {
      auto &current_page =
          this->buffer_manager.fix_page(current_page_id, false);
      auto current_node = reinterpret_cast<Node *>(current_page.get_data());

      if (current_node->is_leaf()) {
        const auto &leaf_node = reinterpret_cast<LeafNode *>(current_node);
        for (int i = 0; i < leaf_node->count; i++)
          if (leaf_node->keys[i] == key) return leaf_node->values[i];

        current_page_id = previous_page_id;
        next++;
      } else {
        const auto &inner_node = reinterpret_cast<InnerNode *>(current_node);
        if (current_node->parent) previous_page_id = *current_node->parent;

        auto [index, isFound] = inner_node->lower_bound(key);
        if (isFound) {
          if (inner_node->count < next)
            return res;
          else {
            current_page_id = inner_node->children[index + next];
            next = 0;
          }
        } else
          current_page_id = inner_node->children[inner_node->count - 1];
      }

      this->buffer_manager.unfix_page(current_page, false);
    }
    return res;
  }

  /// Erase an entry in the tree.
  /// @param[in] key      The key that should be searched.
  void erase(const KeyT &key) {
    /// erase() and lookup() share the same function logic to find the key.
    bool isFound = false;
    if (!this->root) return;

    auto current_page_id = *this->root;
    int next = 0;
    uint64_t previous_page_id = current_page_id;

    while (!isFound) {
      auto &current_page =
          this->buffer_manager.fix_page(current_page_id, false);
      auto current_node = reinterpret_cast<Node *>(current_page.get_data());

      if (current_node->is_leaf()) {
        const auto &leaf_node = reinterpret_cast<LeafNode *>(current_node);
        for (int i = 0; i < leaf_node->count; i++) {
          if (leaf_node->keys[i] == key) {
            this->deleted.insert(key);
            isFound = true;
            for (int j = i; j < leaf_node->count; j++) {
              leaf_node->keys[j] = leaf_node->keys[j + 1];
              leaf_node->values[j] = leaf_node->values[j + 1];
            }
            leaf_node->count--;
            break;
          }
        }

        current_page_id = previous_page_id;
        next++;
      } else {
        const auto &inner_node = reinterpret_cast<InnerNode *>(current_node);
        if (current_node->parent) previous_page_id = *current_node->parent;

        auto [index, isFoundKey] = inner_node->lower_bound(key);
        if (isFoundKey) {
          if (inner_node->count < next)
            isFound = true;
          else {
            current_page_id = inner_node->children[index + next];
            next = 0;
          }
        } else
          current_page_id = inner_node->children[inner_node->count - 1];
      }

      this->buffer_manager.unfix_page(current_page, false);
    }
  }

  /// Inserts a new entry into the tree.
  /// @param[in] key      The key that should be inserted.
  /// @param[in] value    The value that should be inserted.
  void insert(const KeyT &key, const ValueT &value) {
    if (this->isEmpty) {
      /// intialize root node
      this->root = 0;
      this->next_page_id = 1;
      this->isEmpty = false;
    }

    auto current_node_page_id = *this->root;

    while (true) {
      auto &current_page =
          this->buffer_manager.fix_page(current_node_page_id, false);
      auto current_node = reinterpret_cast<Node *>(current_page.get_data());

      if (current_node->is_leaf()) {
        const auto &leaf_node = reinterpret_cast<LeafNode *>(current_node);

        /// Check for empty slots in the leaf node.
        if (leaf_node->count < leaf_node->kCapacity) {
          leaf_node->insert(key, value);
          this->buffer_manager.unfix_page(current_page, false);
          return;
        }

        /// Create a new leaf node since there are no empty slots.
        auto new_leaf_page_id = this->next_page_id++;
        auto &new_leaf_page =
            this->buffer_manager.fix_page(new_leaf_page_id, false);
        KeyT separator_key = leaf_node->split(
            reinterpret_cast<std::byte *>(new_leaf_page.get_data()));
        auto new_node = reinterpret_cast<Node *>(new_leaf_page.get_data());
        auto new_leaf_node = reinterpret_cast<LeafNode *>(new_node);

        /// Insert the new key into the appropriate node.
        if (!ComparatorT()(separator_key, key))
          leaf_node->insert(key, value);
        else
          new_leaf_node->insert(key, value);

        if (!leaf_node->parent) {
          /// Update the root with the new page id.
          this->root = this->next_page_id++;
          auto &new_root_node_page =
              this->buffer_manager.fix_page(*this->root, false);
          auto new_inner_root_node =
              reinterpret_cast<Node *>(new_root_node_page.get_data());
          auto new_root_node =
              reinterpret_cast<InnerNode *>(new_inner_root_node);

          new_root_node->level = ++rootLevel;

          /// / Set up the new root with the separator key and child pointers.
          new_root_node->keys[0] = separator_key;
          new_root_node->children[0] = current_node_page_id;
          new_root_node->children[1] = new_leaf_page_id;
          new_root_node->count += 2;

          /// Set parent page id for both leaf nodes.
          leaf_node->parent = *this->root;
          new_leaf_node->parent = *this->root;

          this->buffer_manager.unfix_page(new_leaf_page, false);
          this->buffer_manager.unfix_page(new_root_node_page, false);
        } else {
          /// Insert the separator key into the existing parent node.
          auto &parent_node_page =
              this->buffer_manager.fix_page(*leaf_node->parent, false);
          const auto &parent_node =
              reinterpret_cast<Node *>(parent_node_page.get_data());
          auto parent_inner_node = reinterpret_cast<InnerNode *>(parent_node);

          std::optional<uint64_t> leftNode;
          if (leaf_node->values[leaf_node->count - 1] <
              new_leaf_node->values[0])
            leftNode = current_node_page_id;

          parent_inner_node->insert(separator_key, new_leaf_page_id, leftNode);
          new_leaf_node->parent = *leaf_node->parent;

          this->buffer_manager.unfix_page(parent_node_page, false);
        }

        this->buffer_manager.unfix_page(current_page, false);
        return;
      }

      /// inner node
      auto inner_node = reinterpret_cast<InnerNode *>(current_node);

      if (inner_node->count == (inner_node->kCapacity + 1)) {
        /// Create a new inner node when the current one is full.
        auto new_inner_node_page_id = this->next_page_id++;
        auto &new_inner_node_page =
            this->buffer_manager.fix_page(new_inner_node_page_id, false);
        KeyT separator_key = inner_node->split(
            reinterpret_cast<std::byte *>(new_inner_node_page.get_data()));
        const auto &new_node =
            reinterpret_cast<Node *>(new_inner_node_page.get_data());
        const auto &new_inner_node = reinterpret_cast<InnerNode *>(new_node);
        new_inner_node->level = inner_node->level;

        /// Set the new parent id of the children of the new inner node.
        for (int i = 0; i < new_inner_node->count; i++) {
          auto &child =
              this->buffer_manager.fix_page(new_inner_node->children[i], false);
          auto child_node = reinterpret_cast<Node *>(child.get_data());
          auto child_inner_node = reinterpret_cast<InnerNode *>(child_node);
          child_inner_node->parent = new_inner_node_page_id;
        }

        if (!inner_node->parent) {
          /// Update the root with the new page id.
          this->root = this->next_page_id++;
          auto &new_root_node_page =
              this->buffer_manager.fix_page(*this->root, false);
          auto new_root_node =
              reinterpret_cast<Node *>(new_root_node_page.get_data());
          auto new_root_inner_node =
              reinterpret_cast<InnerNode *>(new_root_node);

          rootLevel += 1;
          new_root_inner_node->level = rootLevel;
          new_root_inner_node->keys[0] = separator_key;
          new_root_inner_node->children[0] = current_node_page_id;
          new_root_inner_node->children[1] = new_inner_node_page_id;
          new_root_inner_node->count += 2;

          /// Set parent page id for both inner nodes.
          inner_node->parent = *this->root;
          new_inner_node->parent = *this->root;

          this->buffer_manager.unfix_page(new_root_node_page, false);

          /// Move to the next node.
          auto [index, isFound] = new_root_inner_node->lower_bound(key);
          if (isFound)
            current_node_page_id = new_root_inner_node->children[index];
          else
            current_node_page_id =
                new_root_inner_node->children[new_root_inner_node->count - 1];

        } else {
          /// Get the parent node of the current node.
          auto &parent_node_page =
              this->buffer_manager.fix_page(*inner_node->parent, false);
          const auto &parent_node =
              reinterpret_cast<Node *>(parent_node_page.get_data());
          const auto &parent_inner_node =
              reinterpret_cast<InnerNode *>(parent_node);
          std::optional<uint64_t> leftNode;
          parent_inner_node->insert(separator_key, new_inner_node_page_id,
                                    leftNode);
          new_inner_node->parent = *inner_node->parent;

          /// Move to the next node.
          auto [index, isFound] = parent_inner_node->lower_bound(key);
          if (isFound)
            current_node_page_id = parent_inner_node->children[index];
          else
            current_node_page_id =
                parent_inner_node->children[parent_inner_node->count - 1];
          this->buffer_manager.unfix_page(parent_node_page, false);
        }

        this->buffer_manager.unfix_page(new_inner_node_page, false);
      } else {
        /// Move to the next node based on the key.
        auto [index, isFound] = inner_node->lower_bound(key);
        if (isFound)
          current_node_page_id = inner_node->children[index];
        else
          current_node_page_id = inner_node->children[inner_node->count - 1];
      }

      this->buffer_manager.unfix_page(current_page, false);
    }
  }
};

}  // namespace buzzdb