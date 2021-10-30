## Exponential Tree

- Internal nodes should store keys and child pointers separately; leaf nodes
  should store values next to keys.
    - Leaf nodes only store 1-2 keys, so we don't need to do anything fancy
    - Internal nodes don't store values, and only search and iteration time matters
        - On search, we only need to look at keys. Once we find the correct key,
          we can jump to the correct node
        - On iteration, we only need to look at the nodes
- Size allocations based on height?
    - Would this reduce the amount of unusable space in the tree? Is this
      even something that needs to be worried about?
