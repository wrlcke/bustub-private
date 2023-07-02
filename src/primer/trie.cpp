#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

#define MAKE_NODE std::make_unique<TrieNode>
#define MAKE_VALUE_NODE std::make_unique<TrieNodeWithValue<T>>
#define MAKE_VALUE(v) std::make_shared<T>(std::move(v))

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (!root_) {
    return nullptr;
  }
  const TrieNode *curr_node = root_.get();
  for (char c : key) {
    auto it = curr_node->children_.find(c);
    if (it == curr_node->children_.end()) {
      return nullptr;
    }
    curr_node = it->second.get();
  }
  if (!curr_node->is_value_node_) {
    return nullptr;
  }
  const auto *target_node = dynamic_cast<const TrieNodeWithValue<T> *>(curr_node);
  return target_node ? target_node->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<TrieNode> new_root = root_ ? root_->Clone() : MAKE_NODE();
  if (key.empty()) {
    return Trie(MAKE_VALUE_NODE(new_root->children_, MAKE_VALUE(value)));
  }

  TrieNode *curr_node = new_root.get();
  std::shared_ptr<TrieNode> new_node;
  for (auto str_it = key.begin(); str_it != key.end(); ++str_it) {
    auto it = curr_node->children_.find(*str_it);
    if (it == curr_node->children_.end()) {
      new_node = MAKE_NODE();
    } else {
      new_node = it->second->Clone();
    }
    if (str_it != key.end() - 1) {
      curr_node->children_[*str_it] = new_node;
      curr_node = new_node.get();
    }
  }
  curr_node->children_[key.back()] = MAKE_VALUE_NODE(new_node->children_, MAKE_VALUE(value));
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (!root_) {
    return *this;
  }
  const TrieNode *target_node = root_.get();
  std::vector<std::shared_ptr<TrieNode>> nodes_on_key_path;
  nodes_on_key_path.reserve(key.size() + 1);

  for (char c : key) {
    nodes_on_key_path.emplace_back(target_node->Clone());
    auto it = target_node->children_.find(c);
    if (it == target_node->children_.end()) {
      return *this;
    }
    target_node = it->second.get();
  }
  nodes_on_key_path.emplace_back(MAKE_NODE(target_node->children_));

  std::shared_ptr<TrieNode> new_root = nodes_on_key_path.front();
  std::shared_ptr<TrieNode> child_node = nodes_on_key_path.back();
  std::shared_ptr<TrieNode> parent_node;
  for (auto it = nodes_on_key_path.rbegin() + 1; it != nodes_on_key_path.rend(); ++it) {
    char c = key[nodes_on_key_path.rend() - it - 1];
    parent_node = *it;
    if (child_node->Empty()) {
      parent_node->children_.erase(c);
    } else {
      parent_node->children_[c] = child_node;
    }
    child_node = parent_node;
  }
  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
