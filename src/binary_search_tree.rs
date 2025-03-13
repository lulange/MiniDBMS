use std::{
    cmp::Ordering, error::Error, fmt::Display, fs, io::{Read, Write}
};

// TODO either streamline this to be the only error or add another error type
#[derive(Debug)]
pub enum BSTError {
    InsertError,
}

impl Display for BSTError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            BSTError::InsertError => write!(f, "Failed to insert into binary search tree. Key value already exists."),
        }
    }
}

impl Error for BSTError {}

type Child = Option<Box<Node>>;

// TODO make a way that the BST can create itself from a table file if issues with syncing happen or in the case of certain commands

#[derive(Debug)]
struct Node {
    key: String,
    data: u32, // these will be block offset values in the files
    left: Child,
    right: Child,
}

impl Node {
    fn new(key: String, data: u32) -> Node {
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

impl BST {
    pub fn new() -> Self {
        BST {
            root: None,
        }
    }

    pub fn read_from_file(path: &str) -> Result<Self, Box<dyn Error>> {
        let mut file = fs::File::open(path)?;
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut nodes: Vec<Box<Node>> = Vec::new();
        let mut index = 0;
        while let Some(key_len) = buf.get(index) {
            index += 1;
            let key_buf_slice = &buf[index..index + (*key_len as usize)];
            let key_vec = Vec::from(key_buf_slice);
            let key = String::from_utf8(key_vec)?;
            index += *key_len as usize;
            let data_buf_slice: &[u8; 4] = &buf[index..index + 4].try_into().unwrap();
            let data = u32::from_be_bytes(*data_buf_slice);
            nodes.push(Box::new(Node::new(key, data)));
            index += 4;
        }
        
        let root = BST::root_from_in_order_node_vec(nodes);
        
        Ok(BST { root })
    }

    fn root_from_in_order_node_vec(mut nodes: Vec<Box<Node>>) -> Child {
        let mut second_half = nodes.split_off(nodes.len()/2);
        if second_half.len() == 0 {
            return None
        }
        let mut root = second_half.remove(0);
        root.left = BST::root_from_in_order_node_vec(nodes);
        root.right = BST::root_from_in_order_node_vec(second_half);
        return Some(root);
    }

    pub fn write_to_file(&self, path: &str) -> Result<(), Box<dyn Error>> {
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        let file = fs::OpenOptions::new()
            .append(true)
            .open(path)?;
        BST::write_in_order_traversal(&self.root, &file)?;
        Ok(())
    }

    fn write_in_order_traversal(curr_node: &Child, mut file: &fs::File) -> Result<(), Box<dyn Error>> {
        let curr_node = match curr_node {
            None => return Ok(()), // maybe just use byte arrays or something
            Some(node) => node,
        };
        BST::write_in_order_traversal(&curr_node.left, file)?;
        let key_bytes = curr_node.key.as_bytes();
        file.write_all(&(key_bytes.len() as u8).to_be_bytes())?; // no keys longer than u8 len allowed
        file.write_all(curr_node.key.as_bytes())?;
        file.write_all(&curr_node.data.to_be_bytes())?;
        BST::write_in_order_traversal(&curr_node.right, file)?;
        Ok(())
    }

    pub fn insert(&mut self, key: String, data: u32) -> Result<(), BSTError> {
        if key.len() > std::u8::MAX as usize {
            return Err(BSTError::InsertError)
        }

        let mut curr_node = &mut self.root;

        while let Some(node) = curr_node {
            match key.cmp(&node.key) {
                Ordering::Equal => return Err(BSTError::InsertError),
                Ordering::Less => curr_node = &mut node.left,
                Ordering::Greater => curr_node = &mut node.right,
            }
        }

        *curr_node = Some(Box::new(Node::new(key, data)));
        Ok(())
    }

    fn find_node(&self, key: &str) -> Option<&Node> {
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

    pub fn find(&self, key: &str) -> Option<&u32> {
        let node_opt = self.find_node(key);

        match node_opt {
            Some(node) => Some(&node.data),
            None => None,
        }
    }

    fn remove_with_children(node: &mut Child) -> Option<u32> {
        let return_data = node.as_ref().unwrap().data;
        let mut curr_node = &mut node.as_mut().unwrap().left;
        while curr_node.as_mut().unwrap().right.is_some() { // will fail if passed a node without a left child
            curr_node = &mut curr_node.as_mut().unwrap().right;
        }

        let key: String = curr_node.as_mut().unwrap().key.clone();
        let data: u32 = curr_node.as_mut().unwrap().data;

        *curr_node = curr_node.as_mut().unwrap().left.take();

        node.as_mut().unwrap().key = key;
        node.as_mut().unwrap().data = data;
        return Some(return_data);
    }

    pub fn remove(&mut self, key: &str) -> Option<u32> {
        let mut curr_node = &mut self.root;

        while let Some(ref mut node) = curr_node {
            match key.cmp(&node.key) {
                Ordering::Less => curr_node = &mut curr_node.as_mut().unwrap().left, // cannot use node.left because borrow checker doesn't understand
                Ordering::Greater => curr_node = &mut curr_node.as_mut().unwrap().right,
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
}


#[cfg(test)]
mod tests {
    use super::*;

    // also can use #[should_panic] after #[test]
    // #[should_panic(expected = "less than or equal to 100")]
    // with panic!("less than or equal to 100");

    #[test]
    fn test_read_write() -> Result<(), Box<dyn Error>> {
        let mut bst = BST::new();
        
        bst.insert(String::from("1"), 1)?;
        bst.insert(String::from("2"), 3)?;
        bst.insert(String::from("3"), 4)?;
        bst.insert(String::from("4"), 7)?;
        bst.insert(String::from("5"), 8)?;
        bst.insert(String::from("6"), 9)?;
        bst.insert(String::from("7"), 400)?;
        bst.insert(String::from("8"), 400)?;
        let path = "./test.index";
        bst.write_to_file(path)?;
        let mut bst = BST::read_from_file(path)?;
        bst.remove("5").unwrap();
        bst.insert(String::from("q"), 273)?;
        //bst.remove("meh").unwrap();
        bst.write_to_file(path)?;
        Ok(())
    }
}