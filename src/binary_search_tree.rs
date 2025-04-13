use crate::base::Data;
use std::{
    cmp::Ordering,
    error::Error,
    fmt::Display,
    fs,
    io::{self, Read, Write},
};

/// An error to represent issues with attempting to insert two equal keys into a BST
#[derive(Debug)]
pub struct BSTInsertErr;

impl Display for BSTInsertErr {
    /// Provides a message for the BSTInsertErr
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to insert into binary search tree. Key value already exists."
        )
    }
}

// semantic extension of Display.
impl Error for BSTInsertErr {}

/// A shorthand for Option<Box<Node>> - which is necessary for recursive structure
type Child = Option<Box<Node>>;

/// Nodes in the BST. Do not guarantee that both Children have the same Data variant
/// so user must guarantee that to avoid panics from Data::cmp()
#[derive(Debug)]
struct Node {
    key: Data,
    data: usize, // these will be used for record numbers in the Tables
    left: Child,
    right: Child,
}

impl Node {
    /// Create a Node with no children and the given key/data
    fn new(key: Data, data: usize) -> Node {
        Node {
            key,
            data,
            left: None,
            right: None,
        }
    }
}

/// Binary Search Tree that uses a mix of recursive and iterative implementations.
#[derive(Debug)]
pub struct BST {
    root: Child,
}

impl BST {
    /// Returns a new BST with no root
    pub fn new() -> Self {
        BST { root: None }
    }

    /// Attempts to read a BSST back from a file. This function
    /// Assumes BST Nodes are written in order by key.
    ///
    /// # Errors
    ///
    /// Fails when a Node cannot be read from the file.
    pub fn read_from_file(path: &str) -> Result<Self, Box<dyn Error>> {
        // Readonly file
        let mut file = fs::File::open(path)?;

        // read whole file into buf
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;

        // read each node back and push it to nodes
        let mut nodes: Vec<Box<Node>> = Vec::new();
        let mut index = 0;
        while let Some(key_len) = buf.get(index) {
            index += 1; // for the key_len just read
            let key_buf_slice = &buf[index..index + (*key_len as usize)]; // read bytes for key
                                                                          // parse the key from bytes
            let key_vec = Vec::from(key_buf_slice);
            let key = Data::from_bytes(&key_vec)?;
            index += *key_len as usize; // for the key just read
            let data_buf_slice: &[u8; 8] = &buf[index..index + 8].try_into().unwrap(); // read bytes for the data
            let data = usize::from_be_bytes(*data_buf_slice); // parse data
            nodes.push(Box::new(Node::new(key, data)));
            index += 8; // for usize bytes for 64 bit system
        }

        // creates a bst structure out of the in order nodes by recursively splitting the nodes vec
        let root = BST::root_from_in_order_node_vec(nodes);

        Ok(BST { root })
    }

    /// Returns a root to the balanced BST created from nodes by recursively splitting
    fn root_from_in_order_node_vec(mut nodes: Vec<Box<Node>>) -> Child {
        // split half off
        let mut second_half = nodes.split_off(nodes.len() / 2);

        // contains the larger half of the nodes
        if second_half.len() == 0 {
            return None;
        }

        // make the root the middle node
        let mut root = second_half.remove(0);
        root.left = BST::root_from_in_order_node_vec(nodes); // left from root in nodes first half
        root.right = BST::root_from_in_order_node_vec(second_half); // right from root in nodes second half
        return Some(root);
    }

    /// Attempts to write BST to the file located at path. Writes Nodes in key order.
    ///
    /// # Errors
    ///
    /// Fails when path is invalid or files cannot be accessed.
    pub fn write_to_file(&self, path: &str) -> Result<(), io::Error> {
        let file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        BST::write_in_order_traversal(&self.root, &file)
    }

    /// Attempts to write Nodes in key order to the given file.
    ///
    /// # Errors
    ///
    /// Fails when file cannot be written to.
    fn write_in_order_traversal(curr_node: &Child, mut file: &fs::File) -> Result<(), io::Error> {
        let curr_node = match curr_node {
            None => return Ok(()),
            Some(node) => node,
        };
        BST::write_in_order_traversal(&curr_node.left, file)?;
        let key_bytes = curr_node.key.as_bytes();
        file.write_all(&[key_bytes.len() as u8])?; // no keys longer than u8 len allowed
        file.write_all(&key_bytes)?;
        file.write_all(&curr_node.data.to_be_bytes())?;
        BST::write_in_order_traversal(&curr_node.right, file)?;
        Ok(())
    }

    /// Returns a Result that indicates whether or not the Node was inserted.
    ///
    /// # Errors
    ///
    /// Fails when the key is found in the BST already or
    /// when the key requires more bytes to store than can be
    /// expressed in a u8. Note that this should never happen with
    /// Data's current implementation since it maxes out at 100 bytes for Text
    pub fn insert(&mut self, key: Data, data: usize) -> Result<(), BSTInsertErr> {
        if key.as_bytes().len() > std::u8::MAX as usize {
            return Err(BSTInsertErr); // bytes stored must be < u8 so that u8 can be used to store length of key in file
        }

        // iterative insert
        let mut curr_node = &mut self.root;

        while let Some(node) = curr_node {
            match key.cmp(&node.key) {
                Ordering::Equal => return Err(BSTInsertErr),
                Ordering::Less => curr_node = &mut node.left,
                Ordering::Greater => curr_node = &mut node.right,
            }
        }

        *curr_node = Some(Box::new(Node::new(key, data)));
        Ok(())
    }

    /// Returns an Option that contains the found Node if it is Option::Some.
    fn find_node(&self, key: &Data) -> Option<&Node> {
        // iterative search
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

    /// Returns an Option that contains a reference to the data (usize) stored with
    /// the given key if it is Option::Some.
    pub fn find(&self, key: &Data) -> Option<&usize> {
        let node_opt = self.find_node(key); // get the node reference privately

        match node_opt {
            Some(node) => Some(&node.data), // extract data
            None => None,
        }
    }

    /// Handles a removal of a node that has two children. Returns the data contained in the
    /// removed Node.
    ///
    /// # Panics
    ///
    /// Panics if passed a node that is Option::None or a node which does not have at least
    /// a left child.
    fn remove_with_children(node: &mut Child) -> Option<usize> {
        // will panic here if node is Option::None
        let return_data = node.as_ref().unwrap().data;
        let mut curr_node = &mut node.as_mut().unwrap().left;

        // will panic here if passed a node without a left child
        // get right most Child in left sub-tree
        while curr_node.as_mut().unwrap().right.is_some() {
            curr_node = &mut curr_node.as_mut().unwrap().right;
        }

        // clone Node key and data
        let key: Data = curr_node.as_mut().unwrap().key.clone();
        let data: usize = curr_node.as_mut().unwrap().data;

        // remove the right most Child in left sub-tree
        *curr_node = curr_node.as_mut().unwrap().left.take();

        // inject cloned key and data into the node we want to remove
        node.as_mut().unwrap().key = key;
        node.as_mut().unwrap().data = data;
        return Some(return_data);
    }

    /// Returns an Option that contains the data from the removed Node when Node is found/removed
    pub fn remove(&mut self, key: &Data) -> Option<usize> {
        let mut curr_node = &mut self.root;

        while let Some(ref mut node) = curr_node {
            match key.cmp(&node.key) {
                Ordering::Less => curr_node = &mut curr_node.as_mut().unwrap().left, // cannot use node.left because borrow checker doesn't understand
                Ordering::Greater => curr_node = &mut curr_node.as_mut().unwrap().right,
                // when key is equal remove based on children
                Ordering::Equal => match (&mut node.left, &mut node.right) {
                    (Some(_), Some(_)) => {
                        return BST::remove_with_children(curr_node);
                    }
                    (None, Some(_)) => {
                        let return_data = Some(node.data);
                        *curr_node = node.right.take();
                        return return_data;
                    }
                    (Some(_), None) => {
                        let return_data = Some(node.data);
                        *curr_node = node.left.take();
                        return return_data;
                    }
                    (None, None) => {
                        return Some(curr_node.take().unwrap().data);
                    }
                },
            }
        }

        None
    }

    /// Returns a Vec that contains the data values of all nodes in order of the keys stored with each.
    pub fn get_data(&self) -> Vec<usize> {
        let mut data = Vec::new();
        BST::fill_with_data(&self.root, &mut data);
        data
    }

    /// Fills a Vec with the data values of all nodes in order of the keys stored with each.
    fn fill_with_data(node: &Child, data: &mut Vec<usize>) {
        if node.is_none() {
            return;
        }

        let node = node.as_ref().unwrap();
        // in-order traversal
        BST::fill_with_data(&node.left, data);
        data.push(node.data);
        BST::fill_with_data(&node.right, data);
    }
}
