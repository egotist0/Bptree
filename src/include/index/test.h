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
struct BTree : public Segment {
  struct Node {
    /// The level in the tree.
    uint16_t level;

   public:
    /// The number of children.
    uint16_t count;

    uint64_t parentId;

    // std::mutex lock;

    uint64_t id;
    // Constructor
    Node(uint16_t level, uint16_t count) : level(level), count(count) {}

    /// Is the node a leaf node?
    bool is_leaf() const { return level == 0; }
  };

  struct InnerNode : public Node {
    /// The capacity of a node.
    static constexpr uint32_t kCapacity =
        (PageSize - sizeof(uint32_t) - sizeof(uint64_t) - sizeof(Node)) /
        (sizeof(KeyT) + sizeof(uint64_t));
    /// The keys.
    KeyT keys[kCapacity];

    /// The children.
    uint64_t children[kCapacity];

    /// Constructor.
    InnerNode() : Node(0, 0) {}

    /// Get the index of the first key that is not less than than a provided
    /// key.
    /// @param[in] key          The key that should be searched.
    std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
      uint16_t l = 0;
      uint16_t h = this->count - 1;  // Not n - 1
      while (l < h) {
        int mid = (l + h) / 2;
        if (key <= keys[mid]) {
          h = mid;
        } else {
          l = mid + 1;
        }
      }
      // return std::pair(l, true);
      return l == this->count ? std::pair(this->count, false)
                              : std::pair(l, true);
    }

    /// Insert a key.
    /// @param[in] key          The separator that should be inserted.
    /// @param[in] split_page   The id of the split page that should be
    /// inserted.
    bool insert(const KeyT &key, uint64_t split_page) {
      if (this->count == this->kCapacity) {
        return false;
      }

      auto index = lower_bound(key);
      if (index.second == true) {
        for (size_t i = this->count; i > index.first; i--) {
          this->keys[i] = this->keys[i - 1];
          this->children[i + 1] = this->children[i];
        }

        this->keys[index.first] = key;

        this->children[index.first + 1] = split_page;
      } else {
        this->keys[this->count - 1] = key;
        this->children[this->count] = split_page;
      }
      this->count++;
      return true;
    }

    void remove(const KeyT &key) {
      auto index = lower_bound(key);
      if (index.second) {
        for (size_t i = index.first; i < this->count - 1; i++) {
          this->keys[i] = this->keys[i + 1];
          this->children[i + 1] = this->children[i + 2];
        }
      } else {
        this->keys[this->count - 2] = 0;
        this->children[this->count - 1] = 0;
      }
      this->count--;
    }

    /// Split the node.
    /// @param[in] buffer       The buffer for the new page.
    /// @return                 The separator key.
    KeyT split(char *buffer) {
      auto *newNode = new (buffer) InnerNode();
      uint32_t splitPoint = this->count / 2;

      newNode->children[0] = this->children[splitPoint];
      uint32_t splitKey = this->keys[splitPoint - 1];
      this->children[splitPoint] = 0;
      this->keys[splitPoint - 1] = 0;

      newNode->count++;

      for (size_t i = splitPoint + 1; i < this->count; i++) {
        newNode->insert(this->keys[i - 1], this->children[i]);

        this->keys[i - 1] = 0;
        this->children[i] = 0;
      }

      this->count = splitPoint;
      newNode->count = splitPoint;
      if (kCapacity % 2 != 0) {
        newNode->count++;
      }

      return splitKey;
    }

    /// Returns the keys.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<KeyT> get_key_vector() {
      std::vector<KeyT> keyVector = std::vector<KeyT>(this->count);
      for (size_t i = 0; i < this->count; i++) {
        keyVector.push_back(this->keys[i]);
      }

      return keyVector;
    }

    /// Returns the child page ids.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<uint64_t> get_child_vector() {
      std::vector<uint64_t> childVector = std::vector<KeyT>(this->count + 1);
      for (size_t i = 0; i <= this->count; i++) {
        childVector.push_back(this->children[i]);
      }

      return childVector;
    }
  };

  struct LeafNode : public Node {
    /// The capacity of a node.
    /// TODO think about the capacity that the nodes have.
    static constexpr uint32_t kCapacity =
        (PageSize - sizeof(uint32_t) - sizeof(uint64_t) - sizeof(Node)) /
        (sizeof(KeyT) + sizeof(ValueT));

    /// The keys.
    KeyT keys[kCapacity];

    /// The values.
    ValueT values[kCapacity];

    uint64_t next;

    /// Constructor.
    LeafNode() : Node(0, 0) {}

    /// Insert a key.
    /// @param[in] key          The key that should be inserted.
    /// @param[in] value        The value that should be inserted.
    bool insert(const KeyT &key, const ValueT &value) {
      if (this->count < this->kCapacity) {
        auto index = binarySearch(key);

        if (index.first) {
          keys[index.second] = key;
          values[index.second] = value;
          if (key == 0) {
            this->count++;
          }
        } else {
          for (size_t i = this->count; i > index.second; i--) {
            this->keys[i] = this->keys[i - 1];
            this->values[i] = this->values[i - 1];
          }

          this->keys[index.second] = key;
          this->values[index.second] = value;
          this->count++;
        }

        return true;

      } else {
        return false;
      }
    }

    /// Erase a key.
    void erase(const KeyT &key) {
      auto index = binarySearch(key);
      for (size_t i = index.second; i < this->count - 1; i++) {
        this->keys[i] = this->keys[i + 1];
        this->values[i] = this->values[i + 1];
      }

      this->count--;
    }

    /// Split the node.
    /// @param[in] buffer       The buffer for the new page.
    /// @return                 The separator key.
    KeyT split(char *buffer) {
      auto *newNode = new (buffer) LeafNode();
      uint32_t splitPoint = this->count / 2;
      newNode->next = this->next;
      this->next = newNode->id;

      for (size_t i = splitPoint; i < this->count; i++) {
        newNode->insert(this->keys[i], this->values[i]);
        this->keys[i] = 0;
        this->values[i] = 0;
      }

      this->count = splitPoint;

      return this->keys[splitPoint - 1];
    }

    // returns the index of the given key or the highest lower bound
    std::pair<bool, uint32_t> binarySearch(const KeyT &key) {
      if (this->count == 0) {
        return std::pair(false, 0);
      }
      uint16_t l = 0;
      uint16_t h = this->count - 1;  // Not n - 1
      while (l <= h && h < this->count) {
        int mid = (l + h) / 2;
        if (key == keys[mid]) {
          return std::pair(true, mid);
        }
        if (key <= keys[mid]) {
          h = mid - 1;
        } else {
          l = mid + 1;
        }
      }

      return std::pair(false, l);
    }

    /// Returns the keys.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<KeyT> get_key_vector() {
      std::vector<KeyT> keyVector = std::vector<KeyT>(this->count);
      for (size_t i = 0; i < this->count; i++) {
        keyVector.push_back(this->keys[i]);
      }

      return keyVector;
    }

    /// Returns the values.
    /// Can be implemented inefficiently as it's only used in the tests.
    std::vector<ValueT> get_value_vector() {
      std::vector<ValueT> valueVector = std::vector<ValueT>(this->count);
      for (size_t i = 0; i < this->count; i++) {
        valueVector.push_back(this->values[i]);
      }

      return valueVector;
    }
  };

  /// The root.
  std::optional<uint64_t> root;

  /// Next page id.
  /// You don't need to worry about about the page allocation.
  /// (Neither fragmentation, nor persisting free-space bitmaps)
  /// Just increment the next_page_id whenever you need a new page.
  uint64_t next_page_id;

  /// Constructor.
  BTree(uint16_t segment_id, BufferManager &buffer_manager)
      : Segment(segment_id, buffer_manager) {
    next_page_id = 1;
  }

  std::optional<uint64_t> lookupLeaf(const KeyT &key) {
    if (root.has_value()) {
      BufferFrame *frame = &buffer_manager.fix_page(root.value(), false);
      auto *node = reinterpret_cast<Node *>(frame->get_data());
      while (node->level != 0) {
        // no leaf node
        auto *inner = reinterpret_cast<InnerNode *>(frame->get_data());
        ;
        auto result = inner->lower_bound(key);
        uint32_t nodeid;
        if (result.second) {
          nodeid = inner->children[result.first];
        } else {
          nodeid = inner->children[inner->count - 1];
        }
        buffer_manager.unfix_page(*frame, false);
        frame = &buffer_manager.fix_page(nodeid, false);
        node = reinterpret_cast<Node *>(frame->get_data());
      }

      uint64_t id = node->id;
      buffer_manager.unfix_page(*frame, false);
      return id;
    }
  }

  /// Lookup an entry in the tree.
  /// @param[in] key      The key that should be searched.
  std::optional<ValueT> lookup(const KeyT &key) {
    std::optional<uint64_t> id = lookupLeaf(key);
    if (id.has_value()) {
      BufferFrame frame = buffer_manager.fix_page(id.value(), false);
      auto *node = reinterpret_cast<LeafNode *>(frame.get_data());
      auto index = node->binarySearch(key);
      if (index.first) {
        ValueT result = node->values[index.second];

        return std::optional<ValueT>(result);
      }
    }
  }

  /// Erase an entry in the tree.
  /// @param[in] key      The key that should be searched.
  void erase(const KeyT &key) {
    std::optional<uint64_t> leafId = lookupLeaf(key);
    if (!leafId.has_value()) {
      return;
    }
    BufferFrame &currentFrame = buffer_manager.fix_page(leafId.value(), true);

    auto *leaf = reinterpret_cast<LeafNode *>(currentFrame.get_data());
    leaf->erase(key);

    double full =
        static_cast<double>(leaf->count) / static_cast<double>(leaf->kCapacity);
    if (full > 0.5) {
      buffer_manager.unfix_page(currentFrame, true);
      return;
    }

    BufferFrame &nextFrame = buffer_manager.fix_page(leaf->next, true);
    auto *next = reinterpret_cast<LeafNode *>(currentFrame.get_data());

    double nextFull =
        static_cast<double>(next->count) / static_cast<double>(next->kCapacity);

    if (nextFull > 0.5) {
      // balance pages
      BufferFrame &parentFrame = buffer_manager.fix_page(leaf->parentId, true);
      auto *parent = reinterpret_cast<InnerNode *>(currentFrame.get_data());

      KeyT balanceKey = next->keys[0];
      ValueT balanceValue = next->values[0];
      next->erase(balanceKey);
      leaf->insert(balanceKey, balanceValue);

      KeyT separator = next->keys[0];

      std::pair<uint64_t, bool> result = parent->lower_bound(separator);

      parent->keys[result.first] = separator;
      buffer_manager.unfix_page(parentFrame, true);
      buffer_manager.unfix_page(nextFrame, true);
      buffer_manager.unfix_page(currentFrame, true);
      return;
    }

    if (leaf->count != 0) {
      buffer_manager.unfix_page(currentFrame, true);
      return;
    }

    // here the pages have to be merge, but we allow under full pages
    /* }else{
         InnerNode* inner =
     reinterpret_cast<InnerNode*>(currentFrame.get_data()); inner->remove()
     }*/
  }

  /// Inserts a new entry into the tree.
  /// @param[in] key      The key that should be inserted.
  /// @param[in] value    The value that should be inserted.
  void insert(const KeyT &key, const ValueT &value) {
    if (!root.has_value()) {
      BufferFrame &frame =
          buffer_manager.fix_page((segment_id << 48) + next_page_id, true);
      auto *root = new (frame.get_data()) LeafNode();
      root->insert(key, value);
      root->id = (segment_id << 48) + next_page_id;

      this->root = root->id;
      buffer_manager.unfix_page(frame, true);
      next_page_id++;
      return;
    }

    // 1.Lookup leave node
    std::optional leafId = lookupLeaf(key);
    BufferFrame *currentFrame = &buffer_manager.fix_page(leafId.value(), true);
    auto *node = reinterpret_cast<Node *>(currentFrame->get_data());

    KeyT keyForParent = 0;
    uint64_t pageForParent;
    while (true) {
      if (node->is_leaf()) {
        auto *leaf = reinterpret_cast<LeafNode *>(node);
        // is theres a place, done
        if (leaf->insert(key, value)) {
          buffer_manager.unfix_page(*currentFrame, true);
          return;
        }

        // leave couldn't be inserted in node, node is full so we have to split
        uint64_t newPageid = (segment_id << 48) + next_page_id;
        BufferFrame *splitLeafFrame = &buffer_manager.fix_page(newPageid, true);

        KeyT splitKey = leaf->split(splitLeafFrame->get_data());
        auto *rightLeave =
            reinterpret_cast<LeafNode *>(splitLeafFrame->get_data());
        rightLeave->id = newPageid;
        rightLeave->next = leaf->id;
        next_page_id++;

        if (leaf->parentId == 0) {
          rightLeave->parentId = (segment_id << 48) + next_page_id;
        } else {
          rightLeave->parentId = leaf->parentId;
        }
        if (key <= splitKey) {
          leaf->insert(key, value);
        } else {
          rightLeave->insert(key, value);
        }

        pageForParent = rightLeave->id;
        // after split we free the new created node to be able to deliver it in
        // next step.
        buffer_manager.unfix_page(*splitLeafFrame, true);

        keyForParent = splitKey;

      } else {
        auto *inner = static_cast<BTree::InnerNode *>(node);
        BufferFrame *parentFrame;

        //  insert in parent
        if (inner->insert(keyForParent, pageForParent)) {
          buffer_manager.unfix_page(*currentFrame, true);
          return;
        } else {
          // split parent
          uint64_t innerPageId = (segment_id << 48) + next_page_id;
          BufferFrame *splitInnerFrame =
              &buffer_manager.fix_page(innerPageId, true);
          next_page_id++;

          KeyT splitKey = inner->split(splitInnerFrame->get_data());
          auto *rightNode =
              reinterpret_cast<InnerNode *>(splitInnerFrame->get_data());
          rightNode->id = innerPageId;
          rightNode->level = inner->level;

          // if right split node does not have a parent node, we will have to
          // create a new one in next step, by using the next available page id
          if (inner->parentId == 0) {
            rightNode->parentId = (segment_id << 48) + next_page_id;
          } else {
            rightNode->parentId = inner->parentId;
          }

          // not very beautiful, but we have to change the parent id of every
          // moved page
          for (size_t i = 0; i < rightNode->count; i++) {
            BufferFrame &switchFrame =
                buffer_manager.fix_page(rightNode->children[i], true);
            auto &switchNode =
                *reinterpret_cast<BTree::Node *>(switchFrame.get_data());
            switchNode.parentId = rightNode->id;
            buffer_manager.unfix_page(switchFrame, true);
          }

          uint64_t nodeId;
          if (keyForParent <= splitKey) {
            inner->insert(keyForParent, pageForParent);
            nodeId = inner->id;
          } else {
            rightNode->insert(keyForParent, pageForParent);
            nodeId = rightNode->id;
          }

          BufferFrame &switchFrame =
              buffer_manager.fix_page(pageForParent, true);
          auto &switchNode =
              *reinterpret_cast<BTree::Node *>(switchFrame.get_data());
          switchNode.parentId = nodeId;
          buffer_manager.unfix_page(switchFrame, true);

          keyForParent = splitKey;
          pageForParent = rightNode->id;
          buffer_manager.unfix_page(*splitInnerFrame, true);
        }
      }

      // get parent node from id
      if (node->parentId == 0) {
        uint16_t newLevel = node->level + 1;
        uint64_t leftPage = node->id;
        node->parentId = (segment_id << 48) + next_page_id;

        buffer_manager.unfix_page(*currentFrame, true);

        currentFrame =
            &buffer_manager.fix_page((segment_id << 48) + next_page_id, true);
        auto *inner = new (currentFrame->get_data()) InnerNode();

        inner->id = (segment_id << 48) + next_page_id;
        inner->level = newLevel;
        root = (segment_id << 48) + next_page_id;
        next_page_id++;

        inner->children[0] = leftPage;
        inner->count++;
        node = reinterpret_cast<BTree::Node *>(currentFrame->get_data());

      } else {
        buffer_manager.unfix_page(*currentFrame, true);
        currentFrame = &buffer_manager.fix_page(node->parentId, true);
        node = reinterpret_cast<InnerNode *>(currentFrame->get_data());
      }
    }
  }
};

}  // namespace buzzdb
