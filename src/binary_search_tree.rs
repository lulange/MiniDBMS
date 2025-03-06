use std::{
    cmp::Ordering,
    fs,
    path::Path,
    error::Error
};

type Child = Option<Box<Node>>;

#[derive(Debug)]
struct Node {
    key: i32,
    data: String,
    left: Child,
    right: Child,
}

impl Node {
    fn new(key: i32, data: String) -> Node {
        Node {
            key,
            data,
            left: None,
            right: None,
        }
    }
}

#[derive(Debug)]
pub struct BST {
    root: Child,
}

// TODO complete and test bst logic
// TODO write BST parsing and writing to file stuff
impl BST {
    pub fn new() -> Self {
        BST { root: None }
    }

    pub fn read_from_file(path: &Path) -> Result<Self, String> {
        Ok(BST { root: None })
    }

    pub fn write_to_file(&self) -> Result<(), Box<dyn Error>> {
        // get in order traversal iterator
        // write the 

        Ok(())
    }

    pub fn insert(&mut self, key: i32, data: String) -> Result<(), ()> {
        let mut curr_node = &mut self.root;

        while let Some(node) = curr_node {
            match key.cmp(&node.key) {
                Ordering::Equal => return Err(()),
                Ordering::Less => curr_node = &mut node.left,
                Ordering::Greater => curr_node = &mut node.right,
            }
        }

        *curr_node = Some(Box::new(Node::new(key, data)));
        Ok(())
    }

    fn find_node(&self, key: &i32) -> Option<&Node> {
        let mut curr_node = &self.root;

        while let Some(node) = curr_node {
            match key.cmp(&node.key) {
                Ordering::Equal => return Some(&node),
                Ordering::Less => curr_node = &node.left,
                Ordering::Greater => curr_node = &node.right,
            }
        }

        None
    }

    pub fn find(&self, key: &i32) -> Option<&String> {
        let node_opt = self.find_node(key);

        match node_opt {
            Some(node) => Some(&node.data),
            None => None,
        }
    }

    fn remove_with_children(mut node: &mut Child) -> Option<String> {
        let mut curr_node = &mut node.as_mut().unwrap().left; // will fail if passed a node without a left child

        while let Some(n) = curr_node {
            if n.right.is_none() {
                // no more right nodes to go to... so don't go past the node you want
                break;
            }
            curr_node = &mut curr_node.as_mut().unwrap().right;
        }

        let return_data = curr_node.as_mut().unwrap().data.clone();
        *node = curr_node.take();
        return Some(return_data);
    }

    pub fn remove(&mut self, key: i32) -> Option<String> {
        // if is root
        if let Some(ref mut node) = self.root {
            if node.key == key {
                let return_data = node.data.clone();
                self.root = None;
                return Some(return_data);
            }
        } else {
            // no root
            return None;
        }

        let mut curr_node = &mut self.root;

        while let Some(ref mut node) = curr_node {
            match key.cmp(&node.key) {
                Ordering::Less => curr_node = &mut curr_node.as_mut().unwrap().left, // cannot use node.left because borrow checker doesn't understand
                Ordering::Greater => curr_node = &mut curr_node.as_mut().unwrap().right,
                Ordering::Equal => match (&mut node.left, &mut node.right) {
                    (Some(_), Some(_)) => {
                        BST::remove_with_children(curr_node);
                    }
                    (None, Some(_)) => {
                        let return_data = Some(node.data.clone());
                        *curr_node = node.right.take();
                        return return_data;
                    }
                    (Some(_), None) => {
                        let return_data = Some(node.data.clone());
                        *curr_node = node.left.take();
                        return return_data;
                    }
                    (None, None) => {
                        return Some(curr_node.take().unwrap().data.clone());
                    }
                },
            }
        }

        None
    }
}
