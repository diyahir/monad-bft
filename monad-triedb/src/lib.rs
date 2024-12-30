use std::{
    cmp::Ordering,
    collections::BTreeMap,
    ffi::CString,
    path::Path,
    ptr::{null, null_mut},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use alloy_rlp::Decodable;
use futures::channel::oneshot::Sender;
use tracing::{debug, error, warn};

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/triedb.rs"));
}

#[derive(Clone, Debug)]
pub struct TriedbHandle {
    db_ptr: *mut bindings::triedb,
}

pub struct SenderContext {
    sender: Sender<Option<Vec<u8>>>,
    completed_counter: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct TraverseData {
    // a sorted list of (txn_index, rlp_encoded_data)
    data: std::sync::Mutex<BTreeMap<u64, Vec<u8>>>,
}

/// # Safety
/// This should be used only as a callback for async TrieDB calls
///
/// This function is called by TrieDB once it proceses a single read async call
pub unsafe extern "C" fn read_async_callback(
    value_ptr: *const u8,
    value_len: i32,
    sender_context: *mut std::ffi::c_void,
) {
    // Unwrap the sender context struct
    let sender_context = unsafe { Box::from_raw(sender_context as *mut SenderContext) };
    // Increment the completed counter
    sender_context.completed_counter.fetch_add(1, SeqCst);

    let result = match value_len.cmp(&0) {
        Ordering::Less => None,
        Ordering::Equal => Some(Vec::new()),
        Ordering::Greater => {
            let value =
                unsafe { std::slice::from_raw_parts(value_ptr, value_len as usize).to_vec() };
            unsafe { bindings::triedb_finalize(value_ptr) };
            Some(value)
        }
    };

    // Send the retrieved result through the channel
    let _ = sender_context.sender.send(result);
}

/// # Safety
/// This is used as a callback when traversing the transaction or receipt trie
pub unsafe extern "C" fn traverse_callback(
    context: *mut std::ffi::c_void,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) {
    let traverse_data = unsafe { Box::from_raw(context as *mut TraverseData) };

    let key = unsafe {
        let key = std::slice::from_raw_parts(key_ptr, key_len).to_vec();
        key
    };

    let Ok(tx_index) = <u64>::decode(&mut key.as_slice()) else {
        debug!("Txn index decode failed");
        return;
    };

    let value = unsafe {
        let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
        value
    };

    if let Ok(mut data) = traverse_data.data.lock() {
        data.insert(tx_index, value);
    } else {
        warn!("Failed to acquire lock");
    };

    // prevent Box<TraverseData> from dropping
    let _ = Box::into_raw(traverse_data);
}

impl TriedbHandle {
    pub fn try_new(dbdir_path: &Path) -> Option<Self> {
        let path = CString::new(dbdir_path.to_str().expect("invalid path"))
            .expect("failed to create CString");

        let mut db_ptr = null_mut();

        let result = unsafe { bindings::triedb_open(path.as_c_str().as_ptr(), &mut db_ptr) };

        if result != 0 {
            debug!("triedb try_new error result: {}", result);
            return None;
        }

        Some(Self { db_ptr })
    }

    pub fn read(&self, key: &[u8], key_len_nibbles: u8, block_id: u64) -> Option<Vec<u8>> {
        let mut value_ptr = null();
        // make sure doesn't overflow
        if key_len_nibbles >= u8::MAX - 1 {
            error!("Key length nibbles exceeds maximum allowed value");
            return None;
        }
        if (key_len_nibbles as usize + 1) / 2 > key.len() {
            error!("Key length is insufficient for the given nibbles");
            return None;
        }

        let result = unsafe {
            bindings::triedb_read(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                &mut value_ptr,
                block_id,
            )
        };
        if result == -1 {
            return None;
        }

        if result == 0 {
            return Some(Vec::new());
        }

        // check that there's no unexpected error
        if result <= 0 {
            error!("Unexpected result from triedb_read: {}", result);
            return None;
        }

        let value_len = result.try_into().unwrap();
        let value = unsafe {
            let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
            bindings::triedb_finalize(value_ptr);
            value
        };

        Some(value)
    }

    /// This is used to make an async read call to TrieDB.
    /// It creates a oneshot channel and Boxes its sender and the completed_counter
    /// into a context struct and passes it to TrieDB. When TrieDB completes processing
    /// the call, it will call the `read_async_callback` which will unwrap the context
    /// struct, increment the completed_counter, and send the retrieved TrieDB value
    /// through the channel.
    /// The user needs to poll TrieDB using the `triedb_poll` function to pump the async
    /// reads and wait on the returned receiver for the value.
    /// NOTE: the returned receiver must be resolved before key is dropped
    pub fn read_async(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
        completed_counter: Arc<AtomicUsize>,
        sender: Sender<Option<Vec<u8>>>,
    ) {
        // make sure doesn't overflow
        if key_len_nibbles >= u8::MAX - 1 {
            error!("Key length nibbles exceeds maximum allowed value");
            return;
        }
        if (key_len_nibbles as usize + 1) / 2 > key.len() {
            error!("Key length is insufficient for the given nibbles");
            return;
        }

        // Wrap the sender and completed_counter in a context struct
        let sender_context = Box::new(SenderContext {
            sender,
            completed_counter,
        });

        unsafe {
            // Convert the struct into a raw pointer which will be sent to the callback function
            let sender_context_ptr = Box::into_raw(sender_context);

            bindings::triedb_async_read(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                block_id,
                Some(read_async_callback), // TrieDB read async callback
                sender_context_ptr as *mut std::ffi::c_void,
            );
        }
    }

    /// Used to pump async reads in TrieDB
    /// if blocking is true, the thread will sleep at least until 1 completion is available to process
    /// if blocking is false, poll will return if no completion is available to process
    /// max_completions is used as a bound for maximum completions to process in this poll
    ///
    /// Returns the number of completions processed
    /// NOTE: could call poll internally: number of calls to this functions != number of completions processed
    pub fn triedb_poll(&self, blocking: bool, max_completions: usize) -> usize {
        unsafe { bindings::triedb_poll(self.db_ptr, blocking, max_completions) }
    }

    pub fn traverse_triedb(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
    ) -> Option<Vec<Vec<u8>>> {
        // make sure doesn't overflow
        if key_len_nibbles >= u8::MAX - 1 {
            error!("Key length nibbles exceeds maximum allowed value");
            return None;
        }
        if (key_len_nibbles as usize + 1) / 2 > key.len() {
            error!("Key length is insufficient for the given nibbles");
            return None;
        }

        let traverse_data = Box::new(TraverseData {
            data: std::sync::Mutex::new(BTreeMap::new()),
        });

        let result = unsafe {
            let context = Box::into_raw(traverse_data) as *mut std::ffi::c_void;
            bindings::triedb_traverse(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                block_id,
                context,
                Some(traverse_callback),
            );

            Box::from_raw(context as *mut TraverseData)
        };

        // array containing the data in sorted order
        let mut rlp_data_vec = Vec::new();
        match result.data.lock().ok() {
            Some(data) => {
                for (_, rlp_data) in data.iter() {
                    rlp_data_vec.push(rlp_data.to_vec());
                }
            }
            None => return None,
        }

        Some(rlp_data_vec)
    }

    pub fn latest_finalized_block(&self) -> Option<u64> {
        let maybe_latest_finalized_block =
            unsafe { bindings::triedb_latest_finalized_block(self.db_ptr) };
        if maybe_latest_finalized_block == u64::MAX {
            None
        } else {
            Some(maybe_latest_finalized_block)
        }
    }

    pub fn latest_verified_block(&self) -> Option<u64> {
        let maybe_latest_verified_block =
            unsafe { bindings::triedb_latest_verified_block(self.db_ptr) };
        if maybe_latest_verified_block == u64::MAX {
            None
        } else {
            Some(maybe_latest_verified_block)
        }
    }

    pub fn earliest_finalized_block(&self) -> Option<u64> {
        let maybe_earliest_finalized_block =
            unsafe { bindings::triedb_earliest_finalized_block(self.db_ptr) };
        if maybe_earliest_finalized_block == u64::MAX {
            None
        } else {
            Some(maybe_earliest_finalized_block)
        }
    }
}

impl Drop for TriedbHandle {
    fn drop(&mut self) {
        let result = unsafe { bindings::triedb_close(self.db_ptr) };
        if result != 0 {
            error!("Unexpected result from triedb close: {}", result);
        }
    }
}
